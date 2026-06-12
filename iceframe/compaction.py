"""
Advanced compaction strategies for Iceberg tables.
"""

from typing import Optional, List, Dict, Any
import polars as pl
import pyarrow as pa
from pyiceberg.table import Table

class CompactionManager:
    """
    Manage table compaction (rewrite data files).
    """
    
    def __init__(self, table: Table):
        self.table = table
        
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
        # Build scan with filters. We use the scan both for dry-run analysis and
        # — when the table is unpartitioned — to read all the data we'll rewrite.
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)

        # Apply partition_filter dict if provided
        from pyiceberg.expressions import EqualTo, And, AlwaysTrue, IsNull

        def _eq_or_isnull_top(col: str, val: Any):
            return IsNull(col) if val is None else EqualTo(col, val)

        if partition_filter:
            manual_filter = AlwaysTrue()
            for col, val in partition_filter.items():
                if manual_filter == AlwaysTrue():
                    manual_filter = _eq_or_isnull_top(col, val)
                else:
                    manual_filter = And(manual_filter, _eq_or_isnull_top(col, val))
            scan = scan.filter(manual_filter)

        # We used to gather per-partition stats here from the manifest entries,
        # then never use them — the actual compaction loop later re-scans the
        # table by unique partition values. That dead bookkeeping has been
        # removed; per-partition min_input_files is enforced inside
        # process_partition() below where we have the real file counts.

        # 3. Global Options (Compression, Retries, Dry Run setup)
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import threading
        import time
        import random
        from pyiceberg.exceptions import CommitFailedException

        max_workers = kwargs.get("max_workers", 1)
        dry_run = kwargs.get("dry_run", False)
        retries = kwargs.get("retries", 3)
        compression = kwargs.get("compression", None)

        # PyIceberg Table objects are not thread-safe for concurrent commits +
        # refresh — both call paths mutate the shared metadata. Serialize the
        # commit window with a lock; the read/transform stages still parallelize.
        commit_lock = threading.Lock()
        
        # Apply compression if requested (Global setting)
        if compression:
             try:
                 print(f"Setting table compression to: {compression}")
                 with self.table.transaction() as txn:
                     txn.set_properties({"write.parquet.compression-codec": compression})
             except Exception as e:
                 print(f"Warning: Failed to set compression: {e}")

        # Dry Run Logic (Unified)
        if dry_run:
            print("Dry Run: Analyzing table...")
            total_files = 0
            total_bytes = 0
            total_partitions = 0
            skipped_partitions = 0
            
            # Scan all files to get simplified stats
            # We can use plan_files() which returns DataFile objects
            current_partition = None
            files_in_partition = 0
            
            # Note: plan_files() order is not guaranteed to be grouped by partition unless sorted?
            # But usually it iterates manifest entries.
            # Best approach: Use the stats gathering loop we already have, but enhanced.
            
            input_scan = self.table.scan()
            if filter_expr:
                input_scan = input_scan.filter(filter_expr)
                
            tasks = list(input_scan.plan_files())
            total_files = len(tasks)
            total_bytes = sum(t.file.file_size_in_bytes for t in tasks)
            
            # Estimate partitions (rough count via unique partition keys)
            unique_partitions = set()
            for t in tasks:
                # p_key = str(t.file.partition) # Rough key
                # Or better, just count total work
                unique_partitions.add(str(t.file.partition))
            
            total_partitions = len(unique_partitions) if unique_partitions else (1 if total_files > 0 else 0)
            
            # Estimate skipping based on average files per partition (simplified for dry run)
            # or logic: if unpartitioned and count < min_files -> skip
            should_skip = False
            if total_partitions <= 1:
                if total_files < min_input_files:
                    should_skip = True
                    skipped_partitions = 1
            else:
                # Complex to estimate skipped partitions exactly without doing the full aggregation loop
                # Let's just return global stats
                pass

            return {
                "strategy": "dry_run",
                "total_files": total_files,
                "input_bytes": total_bytes,
                "estimated_partitions": total_partitions,
                "would_compact": not should_skip and total_files > 0,
                "message": "Dry Run: No data was modified."
            }

        # Original Logic (Unpartitioned vs Partitioned)
        spec = self.table.spec()
        schema = self.table.schema()
        source_col_ids = [f.source_id for f in spec.fields]
        source_col_names = [schema.find_field(id).name for id in source_col_ids]
        
        if not source_col_names:
            # Unpartitioned Logic
            arrow_table = scan.to_arrow()
            if arrow_table.num_rows == 0:
                 return {"rewritten_rows": 0, "input_bytes": 0}
            
            input_bytes = arrow_table.nbytes # Approximation or get from scan stats?
            # Better: scan plan_files for bytes
            try:
                input_bytes = sum(t.file.file_size_in_bytes for t in scan.plan_files())
            except:
                input_bytes = arrow_table.nbytes

            # File count comes straight from plan_files; we no longer carry
            # stale partition_stats from a removed analysis block.
            try:
                global_count = sum(1 for _ in scan.plan_files())
            except Exception:
                global_count = 0

            if global_count < min_input_files and global_count > 0:
                 return {"rewritten_rows": 0, "message": "Skipped unpartitioned (fewer than min files)", "input_bytes": input_bytes}

            # Deduplication
            if deduplicate:
                df = pl.from_arrow(arrow_table)
                original_rows = df.height
                df = df.unique()
                print(f"Deduplicated: {original_rows} -> {df.height} rows")
                arrow_table = df.to_arrow()

            # Apply Sort Order (Unpartitioned)
            try:
                sort_order = self.table.sort_order()
                if sort_order and sort_order.fields:
                    sort_cols = []
                    for sf in sort_order.fields:
                         if str(sf.transform) == "identity":
                              field_name = schema.find_field(sf.source_id).name
                              sort_cols.append(field_name)
                    if sort_cols:
                        print(f"Applying sort order: {sort_cols}")
                        df = pl.from_arrow(arrow_table)
                        df = df.sort(sort_cols, descending=[not sf.direction.is_ascending for sf in sort_order.fields if str(sf.transform) == "identity"])
                        arrow_table = df.to_arrow()
            except Exception as e:
                print(f"Warning: Failed to apply sort order: {e}")

            self.table.overwrite(arrow_table)
            return {
                "rewritten_rows": arrow_table.num_rows, 
                "strategy": "bin_pack_full", 
                "deduplicated": deduplicate,
                "input_bytes": input_bytes
            }
            
        # Partitioned Logic
        partition_dist_scan = self.table.scan(selected_fields=tuple(source_col_names))
        if filter_expr:
             partition_dist_scan = partition_dist_scan.filter(filter_expr)
             
        if partition_filter:
             manual_filter = AlwaysTrue()
             for col, val in partition_filter.items():
                 if manual_filter == AlwaysTrue():
                     manual_filter = EqualTo(col, val)
                 else:
                     manual_filter = And(manual_filter, EqualTo(col, val))
             partition_dist_scan = partition_dist_scan.filter(manual_filter)

        partitions_df = pl.from_arrow(partition_dist_scan.to_arrow()).unique()
        
        
        from pyiceberg.expressions import IsNull

        def _eq_or_isnull(col: str, val: Any):
            # EqualTo(col, None) is invalid in Iceberg — nullable partition
            # columns require IsNull.
            return IsNull(col) if val is None else EqualTo(col, val)

        def process_partition(row):
            # Build Partition Filter
            part_filter = AlwaysTrue()
            for col, val in row.items():
                if part_filter == AlwaysTrue():
                    part_filter = _eq_or_isnull(col, val)
                else:
                    part_filter = And(part_filter, _eq_or_isnull(col, val))
            
            # Check file count & bytes
            part_bytes = 0
            try:
                part_files_count = 0
                part_scan = self.table.scan(row_filter=part_filter)
                for task in part_scan.plan_files():
                    part_files_count += 1
                    part_bytes += task.file.file_size_in_bytes
                    # Optimization: break early if we just needed count? 
                    # But now we need bytes for metrics.
                
                if part_files_count < min_input_files:
                    return {"skipped": True}
            except:
                pass

            # Read Partition
            part_arrow = self.table.scan(row_filter=part_filter).to_arrow()
            if part_arrow.num_rows == 0:
                return {"skipped": True}
            
            # Deduplication
            if deduplicate:
                df = pl.from_arrow(part_arrow)
                df = df.unique()
                part_arrow = df.to_arrow()
                
            # Apply Sort Order
            try:
                sort_order = self.table.sort_order()
                if sort_order and sort_order.fields:
                    sort_cols = []
                    for sf in sort_order.fields:
                         if str(sf.transform) == "identity":
                              field_name = schema.find_field(sf.source_id).name
                              sort_cols.append(field_name)
                    if sort_cols:
                        df = pl.from_arrow(part_arrow)
                        df = df.sort(sort_cols, descending=[not sf.direction.is_ascending for sf in sort_order.fields if str(sf.transform) == "identity"])
                        part_arrow = df.to_arrow()
            except:
                pass
            
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
                        print(f"Error: All {retries} retries failed for partition {row}. Last error: {e}")
                        raise e
                    sleep_time = random.uniform(0.1, 1.0) * attempt
                    print(f"Commit conflict for partition {row}. Retrying in {sleep_time:.2f}s (Attempt {attempt}/{retries})...")
                    time.sleep(sleep_time)
                    with commit_lock:
                        self.table.refresh()
            
            return {"rewritten_rows": part_arrow.num_rows, "skipped": False, "bytes": part_bytes}

        results = []
        partitions_list = partitions_df.to_dicts()
        
        if max_workers > 1:
            print(f"Compacting {len(partitions_list)} partitions in parallel (workers={max_workers})...")
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_partition, p) for p in partitions_list]
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Error compacting partition: {e}")
        else:
            for p in partitions_list:
                results.append(process_partition(p))

        # Aggregate stats
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
            "input_bytes": total_input_bytes
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
            raise RuntimeError(f"Failed to enable bloom filters: {e}")
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
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)

        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)

        if df.height == 0:
            return {"rewritten_rows": 0, "strategy": "noop", "columns": list(columns)}

        sorted_df = df.sort(columns)
        self.table.overwrite(sorted_df.to_arrow())

        return {
            "rewritten_rows": df.height,
            "strategy": "z_order_approx (hierarchical sort)",
            "columns": list(columns),
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
            raise NotImplementedError(f"Manifest rewriting not supported: {e}")
