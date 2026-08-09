"""
Advanced compaction strategies for Iceberg tables.
"""

import logging
from typing import Any, Dict, List, Optional

import polars as pl
from pyiceberg.expressions import AlwaysTrue, And, EqualTo, IsNull
from pyiceberg.table import Table

from iceframe.cache import invalidate_query_cache
from iceframe.exceptions import CompactionError, UnsupportedOperationError

logger = logging.getLogger(__name__)


def _eq_or_isnull(col: str, val: Any):
    """``EqualTo(col, None)`` is invalid in Iceberg — nulls need ``IsNull``."""
    return IsNull(col) if val is None else EqualTo(col, val)


def _conjunction(preds) -> Any:
    """AND a sequence of Iceberg predicates together (empty -> AlwaysTrue)."""
    combined = None
    for pred in preds:
        combined = pred if combined is None else And(combined, pred)
    return combined if combined is not None else AlwaysTrue()


class CompactionManager:
    """
    Manage table compaction (rewrite data files).
    """

    def __init__(self, table: Table):
        self.table = table

    def _scope_filter(
        self,
        filter_expr: Optional[Any],
        partition_filter: Optional[Dict[str, Any]],
    ) -> Any:
        """
        Build the Iceberg predicate describing *exactly* the rows this
        compaction is allowed to replace.

        This is the fix for the data-loss bug: the unpartitioned rewrite path
        used to call ``table.overwrite(arrow)`` with no ``overwrite_filter``,
        which defaults to ``AlwaysTrue`` — so compacting ``"v > 30"`` on a
        6-row table replaced all 6 rows with the 3 matching ones and reported
        success. The overwrite must be scoped to the same predicate the scan
        was scoped to.

        Returns ``AlwaysTrue()`` when the compaction is genuinely whole-table.
        """
        preds = []

        if filter_expr is not None:
            if isinstance(filter_expr, str):
                from pyiceberg.expressions import parser
                preds.append(parser.parse(filter_expr))
            elif hasattr(filter_expr, "pushdown"):
                pushed, fully = filter_expr.pushdown()
                if not fully:
                    raise CompactionError(
                        "Compaction filters must translate fully to Iceberg "
                        "predicates; a partially-pushed filter would rewrite "
                        "(and therefore delete) rows outside the intended scope."
                    )
                preds.append(pushed)
            else:
                preds.append(filter_expr)

        if partition_filter:
            for col, val in partition_filter.items():
                preds.append(_eq_or_isnull(col, val))

        return _conjunction(preds)


    def bin_pack(
        self,
        target_file_size_mb: int = 128,
        filter_expr: Optional[str] = None,
        min_input_files: int = 1,
        partition_filter: Optional[Dict[str, Any]] = None,
        deduplicate: bool = False,
        **kwargs
    ) -> Dict[str, int]:
        """
        Compact small files into larger files (Bin-packing).
        Safe implementation: Compacts one partition at a time to manage memory.

        Args:
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter to select files to compact
            min_input_files: Minimum number of files required in a partition to trigger compaction
            partition_filter: Dict of column=value to filter specific partitions (e.g. {'cat': 'A'})
            deduplicate: Whether to deduplicate fully identical rows during compaction

        Returns:
            Stats on compacted files
        """
        # The scope filter is the single source of truth for "which rows may
        # this compaction touch". It drives BOTH the read scan and the
        # overwrite_filter, so a scoped compaction can never delete rows
        # outside its scope.
        scope_filter = self._scope_filter(filter_expr, partition_filter)
        scoped = not isinstance(scope_filter, AlwaysTrue)
        scan = self.table.scan(row_filter=scope_filter)

        # We used to gather per-partition stats here from the manifest entries,
        # then never use them — the actual compaction loop later re-scans the
        # table by unique partition values. That dead bookkeeping has been
        # removed; per-partition min_input_files is enforced inside
        # process_partition() below where we have the real file counts.

        # 3. Global Options (Compression, Retries, Dry Run setup)
        import random
        import threading
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        from pyiceberg.exceptions import CommitFailedException
        from pyiceberg.table import TableProperties

        max_workers = kwargs.get("max_workers", 1)
        dry_run = kwargs.get("dry_run", False)
        retries = kwargs.get("retries", 3)
        compression = kwargs.get("compression", None)

        # PyIceberg Table objects are not thread-safe for concurrent commits +
        # refresh - both call paths mutate the shared metadata. Serialize the
        # commit window with a lock; the read/transform stages still parallelize.
        commit_lock = threading.Lock()

        # `target_file_size_mb` used to be accepted and completely ignored -
        # the primary knob of a bin-packing compactor was decorative. It maps
        # onto Iceberg's own writer property, which PyIceberg honours when it
        # splits an Arrow table into data files.
        write_properties: Dict[str, str] = {}
        if target_file_size_mb:
            write_properties[TableProperties.WRITE_TARGET_FILE_SIZE_BYTES] = str(
                int(target_file_size_mb) * 1024 * 1024
            )
        if compression:
            write_properties["write.parquet.compression-codec"] = str(compression)

        def _apply_write_properties() -> None:
            """Persist the writer settings this compaction run needs."""
            if not write_properties:
                return
            try:
                with self.table.transaction() as txn:
                    txn.set_properties(write_properties)
            except Exception as e:
                logger.warning("Failed to apply compaction write properties: %s", e)

        # Dry Run Logic (Unified)
        if dry_run:
            logger.info("Dry run: analysing %s", self.table.name())

            tasks = list(scan.plan_files())
            total_files = len(tasks)
            total_bytes = sum(t.file.file_size_in_bytes for t in tasks)

            unique_partitions = {str(t.file.partition) for t in tasks}
            total_partitions = len(unique_partitions) if unique_partitions else (
                1 if total_files > 0 else 0
            )

            should_skip = False
            skipped_partitions = 0
            if total_partitions <= 1 and total_files < min_input_files:
                should_skip = True
                skipped_partitions = 1

            return {
                "strategy": "dry_run",
                "total_files": total_files,
                "input_bytes": total_bytes,
                "estimated_partitions": total_partitions,
                "skipped_partitions": skipped_partitions,
                "target_file_size_bytes": int(target_file_size_mb) * 1024 * 1024
                if target_file_size_mb
                else None,
                "scoped": scoped,
                "would_compact": not should_skip and total_files > 0,
                "message": "Dry Run: No data was modified.",
            }

        # Original Logic (Unpartitioned vs Partitioned)
        spec = self.table.spec()
        schema = self.table.schema()
        source_col_names = [schema.find_field(f.source_id).name for f in spec.fields]

        def _sorted(arrow_tbl):
            """Apply the table's identity sort order, if any."""
            try:
                sort_order = self.table.sort_order()
                if not (sort_order and sort_order.fields):
                    return arrow_tbl
                identity_fields = [
                    sf for sf in sort_order.fields if str(sf.transform) == "identity"
                ]
                if not identity_fields:
                    return arrow_tbl
                from pyiceberg.table.sorting import SortDirection

                sort_cols = [schema.find_field(sf.source_id).name for sf in identity_fields]
                # SortDirection is a plain enum - it has no `is_ascending`
                # attribute. Reading one raised AttributeError, which the
                # surrounding try/except swallowed, so the table's sort order
                # was silently never applied during compaction.
                descending = [sf.direction == SortDirection.DESC for sf in identity_fields]
                logger.debug("Applying sort order %s", sort_cols)
                return (
                    pl.from_arrow(arrow_tbl).sort(sort_cols, descending=descending).to_arrow()
                )
            except Exception as e:
                logger.warning("Failed to apply sort order: %s", e)
                return arrow_tbl

        if not source_col_names:
            # ---- Unpartitioned ----
            arrow_table = scan.to_arrow()
            if arrow_table.num_rows == 0:
                return {"rewritten_rows": 0, "input_bytes": 0, "scoped": scoped}

            try:
                plan = list(scan.plan_files())
                input_bytes = sum(t.file.file_size_in_bytes for t in plan)
                global_count = len(plan)
            except Exception as e:
                logger.debug("plan_files() unavailable (%s); estimating from Arrow", e)
                input_bytes = arrow_table.nbytes
                global_count = 0

            if 0 < global_count < min_input_files:
                return {
                    "rewritten_rows": 0,
                    "message": "Skipped unpartitioned (fewer than min files)",
                    "input_bytes": input_bytes,
                    "scoped": scoped,
                }

            if deduplicate:
                df = pl.from_arrow(arrow_table)
                original_rows = df.height
                df = df.unique()
                logger.info("Deduplicated: %d -> %d rows", original_rows, df.height)
                arrow_table = df.to_arrow()

            arrow_table = _sorted(arrow_table)

            _apply_write_properties()

            # CRITICAL: scope the overwrite to exactly the rows we read. Passing
            # no overwrite_filter defaults to AlwaysTrue and replaces the WHOLE
            # table with the filtered subset - i.e. silently deletes every
            # non-matching row. See _scope_filter().
            self.table.overwrite(arrow_table, overwrite_filter=scope_filter)
            invalidate_query_cache(self.table.name())

            return {
                "rewritten_rows": arrow_table.num_rows,
                "strategy": "bin_pack_scoped" if scoped else "bin_pack_full",
                "deduplicated": deduplicate,
                "input_bytes": input_bytes,
                "scoped": scoped,
            }

        # ---- Partitioned ----
        partition_dist_scan = self.table.scan(
            row_filter=scope_filter,
            selected_fields=tuple(source_col_names),
        )
        partitions_df = pl.from_arrow(partition_dist_scan.to_arrow()).unique()

        _apply_write_properties()

        def process_partition(row):
            part_filter = _conjunction(
                [_eq_or_isnull(col, val) for col, val in row.items()]
            )

            # Count files & bytes for the min_input_files gate.
            part_bytes = 0
            try:
                part_files_count = 0
                for task in self.table.scan(row_filter=part_filter).plan_files():
                    part_files_count += 1
                    part_bytes += task.file.file_size_in_bytes

                if part_files_count < min_input_files:
                    return {"skipped": True}
            except Exception as e:
                logger.debug("Could not plan files for partition %s: %s", row, e)

            part_arrow = self.table.scan(row_filter=part_filter).to_arrow()
            if part_arrow.num_rows == 0:
                return {"skipped": True}

            if deduplicate:
                part_arrow = pl.from_arrow(part_arrow).unique().to_arrow()

            part_arrow = _sorted(part_arrow)

            # Rewrite with retries. Hold ``commit_lock`` for the whole commit +
            # refresh cycle so concurrent workers can't race against each other
            # on the shared Table object.
            attempt = 0
            while attempt <= retries:
                try:
                    with commit_lock:
                        self.table.overwrite(part_arrow, overwrite_filter=part_filter)
                    break
                except CommitFailedException as e:
                    attempt += 1
                    if attempt > retries:
                        logger.error(
                            "All %d retries failed for partition %s: %s", retries, row, e
                        )
                        raise
                    sleep_time = random.uniform(0.1, 1.0) * attempt
                    logger.warning(
                        "Commit conflict for partition %s; retrying in %.2fs "
                        "(attempt %d/%d)",
                        row,
                        sleep_time,
                        attempt,
                        retries,
                    )
                    time.sleep(sleep_time)
                    with commit_lock:
                        self.table.refresh()

            return {"rewritten_rows": part_arrow.num_rows, "skipped": False, "bytes": part_bytes}

        results = []
        partitions_list = partitions_df.to_dicts()

        if max_workers > 1:
            logger.info(
                "Compacting %d partitions in parallel (workers=%d)",
                len(partitions_list),
                max_workers,
            )
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_partition, p) for p in partitions_list]
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        logger.error("Error compacting partition: %s", e)
                        raise CompactionError(f"Partition compaction failed: {e}") from e
        else:
            for p in partitions_list:
                results.append(process_partition(p))

        invalidate_query_cache(self.table.name())

        skipped_partitions_count = sum(1 for r in results if r.get("skipped"))
        rewritten_partitions = sum(1 for r in results if not r.get("skipped"))
        total_rows = sum(r.get("rewritten_rows", 0) for r in results)
        total_input_bytes = sum(r.get("bytes", 0) for r in results)

        return {
            "rewritten_rows": total_rows,
            "strategy": "bin_pack_partitioned",
            "skipped_partitions": skipped_partitions_count,
            "rewritten_partitions": rewritten_partitions,
            "deduplicated": deduplicate,
            "parallel": max_workers > 1,
            "input_bytes": total_input_bytes,
            "scoped": scoped,
        }

    def sort(
        self,
        sort_order: List[Any],
        filter_expr: Optional[Any] = None,
        target_file_size_mb: int = 128,
        descending: Optional[List[bool]] = None,
    ) -> Dict[str, Any]:
        """
        Rewrite data files sorted by ``sort_order``.

        Like Spark's ``rewrite_data_files(strategy => 'sort')``: the rows are
        read, sorted, and written back. Sorted files cluster values so Iceberg's
        per-file min/max statistics prune more aggressively on those columns.

        Args:
            sort_order: Column names in priority order.
            filter_expr: Optional Iceberg predicate scoping the rewrite. As in
                :meth:`bin_pack`, the overwrite is scoped to exactly the same
                predicate, so a filtered sort never deletes non-matching rows.
            target_file_size_mb: Target output file size.
            descending: Optional per-column sort directions.

        Returns:
            Stats dict with ``rewritten_rows``, ``strategy`` and ``columns``.
        """
        if not sort_order:
            raise CompactionError("sort() requires at least one column")

        scope_filter = self._scope_filter(filter_expr, None)
        scoped = not isinstance(scope_filter, AlwaysTrue)

        df = pl.from_arrow(self.table.scan(row_filter=scope_filter).to_arrow())
        if df.height == 0:
            return {
                "rewritten_rows": 0,
                "strategy": "noop",
                "columns": list(sort_order),
                "scoped": scoped,
            }

        missing = [c for c in sort_order if c not in df.columns]
        if missing:
            raise CompactionError(f"Sort columns not found in table: {missing}")

        if target_file_size_mb:
            from pyiceberg.table import TableProperties
            try:
                with self.table.transaction() as txn:
                    txn.set_properties({
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: str(
                            int(target_file_size_mb) * 1024 * 1024
                        )
                    })
            except Exception as e:
                logger.warning("Failed to set target file size: %s", e)

        sorted_df = df.sort(list(sort_order), descending=descending or False)
        self.table.overwrite(sorted_df.to_arrow(), overwrite_filter=scope_filter)
        invalidate_query_cache(self.table.name())

        return {
            "rewritten_rows": sorted_df.height,
            "strategy": "sort",
            "columns": list(sort_order),
            "scoped": scoped,
        }

    def enable_bloom_filters(self, columns: List[str], fpp: float = 0.01) -> Dict[str, Any]:
        """
        Enable Bloom Filters for specific columns to speed up point lookups.

        Args:
            columns: List of column names to index.
            fpp: False positive probability (default 0.01).
        """
        try:
             with self.table.transaction() as txn:
                 # Set fpp global
                 txn.set_properties({"write.parquet.bloom-filter-fpp": str(fpp)})

                 # Enable for specific columns
                 updates = {}
                 for col in columns:
                     updates[f"write.parquet.bloom-filter-enabled.column.{col}"] = "true"
                 txn.set_properties(updates)

             return {
                 "status": "enabled",
                 "columns": columns,
                 "fpp": fpp
             }
        except Exception as e:
            raise CompactionError(f"Failed to enable bloom filters: {e}") from e

    def z_order_optimize(
        self,
        columns: List[str],
        target_file_size_mb: int = 128,
        filter_expr: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Approximate Z-Order clustering by sorting hierarchically on ``columns``.

        True Z-Order would interleave the bits of each row's column ranks so
        that points close in N-D space land close in the 1-D sort order; we
        don't ship a vectorized bit-interleave in pure Polars, so the rewrite
        falls back to a hierarchical sort. The returned ``strategy`` makes the
        distinction explicit so callers can tell what they actually got.

        Args:
            columns: Columns to cluster by, in priority order.
            target_file_size_mb: Kept for signature compatibility — the actual
                file size is governed by Iceberg's writer config, not this
                method. Pass via ``compression``/table properties if you need
                tighter control.
            filter_expr: Optional scan filter limiting which rows are rewritten.

        Returns:
            Stats dict including the strategy used and number of rewritten rows.
        """
        # Same scoping rule as bin_pack: whatever we read is exactly what we're
        # allowed to replace. Without the overwrite_filter, a filtered z-order
        # rewrite replaced the whole table with the matching subset.
        scope_filter = self._scope_filter(filter_expr, None)
        scoped = not isinstance(scope_filter, AlwaysTrue)

        scan = self.table.scan(row_filter=scope_filter)
        df = pl.from_arrow(scan.to_arrow())

        if df.height == 0:
            return {
                "rewritten_rows": 0,
                "strategy": "noop",
                "columns": list(columns),
                "scoped": scoped,
            }

        if target_file_size_mb:
            from pyiceberg.table import TableProperties
            try:
                with self.table.transaction() as txn:
                    txn.set_properties({
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES: str(
                            int(target_file_size_mb) * 1024 * 1024
                        )
                    })
            except Exception as e:
                logger.warning("Failed to set target file size: %s", e)

        sorted_df = df.sort(columns)
        self.table.overwrite(sorted_df.to_arrow(), overwrite_filter=scope_filter)
        invalidate_query_cache(self.table.name())

        return {
            "rewritten_rows": df.height,
            "strategy": "z_order_approx (hierarchical sort)",
            "columns": list(columns),
            "scoped": scoped,
            "note": "True bit-interleaved Z-Order requires a native extension; rewrite used a hierarchical sort on the given columns.",
        }

    def rewrite_manifests(self, target_size_mb: int = 8) -> dict:
        """
        Rewrite manifest files to optimize metadata (native implementation).

        Args:
            target_size_mb: Target size for manifest files in MB

        Returns:
            Stats on rewritten manifests
        """
        try:
            # Get current snapshot
            current_snapshot = self.table.current_snapshot()
            if not current_snapshot:
                return {"rewritten_manifests": 0, "message": "No snapshots to optimize"}

            # Get all manifest files
            manifests = list(current_snapshot.manifests(self.table.io))

            if len(manifests) <= 1:
                return {"rewritten_manifests": 0, "message": "Only one manifest, no optimization needed"}

            # Calculate total entries across all manifests
            total_entries = sum(m.added_files_count or 0 for m in manifests)

            # Estimate if rewriting would help
            # (many small manifests vs few large ones)
            avg_entries_per_manifest = total_entries / len(manifests) if manifests else 0

            if avg_entries_per_manifest > 100:  # Arbitrary threshold
                return {
                    "rewritten_manifests": 0,
                    "message": f"Manifests already well-sized ({avg_entries_per_manifest:.0f} entries/manifest)"
                }

            # Native implementation would require:
            # 1. Reading all manifest entries
            # 2. Combining into fewer, larger manifests
            # 3. Writing new manifest files
            # 4. Creating new snapshot with updated manifest list

            # This is complex and requires direct metadata manipulation
            # For now, we'll check if PyIceberg supports it
            if hasattr(self.table, 'rewrite_manifests'):
                result = self.table.rewrite_manifests()
                if hasattr(result, 'commit'):
                    result.commit()
                return {
                    "rewritten_manifests": len(manifests),
                    "original_count": len(manifests)
                }
            else:
                # Return diagnostic info for manual optimization
                return {
                    "rewritten_manifests": 0,
                    "message": "Manifest rewriting not supported by PyIceberg",
                    "manifest_count": len(manifests),
                    "total_entries": total_entries,
                    "avg_entries_per_manifest": avg_entries_per_manifest,
                    "recommendation": "Consider upgrading PyIceberg or using Spark for manifest optimization"
                }

        except Exception as e:
            raise UnsupportedOperationError(f"Manifest rewriting not supported: {e}") from e
