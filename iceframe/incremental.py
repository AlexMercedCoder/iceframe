"""
Incremental processing for IceFrame.
"""

import logging

import polars as pl
from pyiceberg.table import Table

from iceframe.exceptions import ValidationError
from iceframe.utils import from_arrow_dataframe

logger = logging.getLogger(__name__)


class IncrementalReader:
    """
    Handles incremental reads and change data capture (CDC) for Iceberg tables.
    """

    def __init__(self, table: Table):
        self.table = table

    def read_incremental(
        self,
        since_snapshot_id: int | None = None,
        since_timestamp: int | None = None,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Read only data added since a specific snapshot or timestamp.

        Args:
            since_snapshot_id: Read data added after this snapshot ID
            since_timestamp: Read data added after this timestamp (milliseconds since epoch)
            columns: Optional list of columns to select

        Returns:
            Polars DataFrame with incremental data
        """
        if since_snapshot_id is None and since_timestamp is None:
            raise ValidationError("Must specify either since_snapshot_id or since_timestamp")

        # Get current snapshot
        current_snapshot = self.table.current_snapshot()
        if not current_snapshot:
            return pl.DataFrame()

        # Determine starting snapshot
        start_snapshot_id: int | None
        if since_snapshot_id:
            start_snapshot_id = since_snapshot_id
        else:
            assert since_timestamp is not None
            start_snapshot_id = self._find_snapshot_by_timestamp(since_timestamp)

        if start_snapshot_id is None:
            raise ValidationError("No snapshot found at or before the requested starting point")

        # Manifest-level incremental scan: read only the data files ADDED by
        # snapshots after `start_snapshot_id`.
        #
        # This used to compute `start_snapshot_id` and then throw it away,
        # scanning the entire table and returning every row — an "incremental"
        # read that was really a full read.
        added_paths = self._files_added_after(start_snapshot_id)
        if not added_paths:
            return self._empty_frame(columns)

        df = self._read_files(added_paths, columns)
        return df

    def _ancestry(self, snapshot_id: int | None = None) -> list[int]:
        """Snapshot ids from ``snapshot_id`` (or current) back to the root."""
        if snapshot_id is None:
            current = self.table.current_snapshot()
            if current is None:
                return []
            snapshot_id = current.snapshot_id

        by_id = {s.snapshot_id: s for s in self.table.snapshots()}
        chain: list[int] = []
        cursor: int | None = snapshot_id
        while cursor is not None and cursor in by_id:
            chain.append(cursor)
            cursor = by_id[cursor].parent_snapshot_id
        return chain

    def _files_added_after(
        self, start_snapshot_id: int, end_snapshot_id: int | None = None
    ) -> list[str]:
        """Data-file paths added strictly after ``start_snapshot_id``."""
        from pyiceberg.manifest import ManifestEntryStatus

        chain = self._ancestry(end_snapshot_id)
        if start_snapshot_id not in chain:
            raise ValidationError(
                f"Snapshot {start_snapshot_id} is not an ancestor of the target "
                "snapshot; incremental reads require a linear history."
            )

        # chain is newest -> oldest; take everything above the start snapshot.
        target_ids = set(chain[: chain.index(start_snapshot_id)])
        if not target_ids:
            return []

        by_id = {s.snapshot_id: s for s in self.table.snapshots()}
        paths: list[str] = []
        seen = set()
        for snapshot_id in target_ids:
            snapshot = by_id[snapshot_id]
            for manifest in snapshot.manifests(self.table.io):
                for entry in manifest.fetch_manifest_entry(self.table.io, discard_deleted=True):
                    if entry.status != ManifestEntryStatus.ADDED:
                        continue
                    if entry.snapshot_id is not None and entry.snapshot_id not in target_ids:
                        continue
                    path = entry.data_file.file_path
                    if path not in seen:
                        seen.add(path)
                        paths.append(path)
        return paths

    def _empty_frame(self, columns: list[str] | None = None) -> pl.DataFrame:
        """An empty frame with the table's (optionally projected) schema."""
        empty = from_arrow_dataframe(self.table.scan(limit=0).to_arrow())
        if columns:
            empty = empty.select([c for c in columns if c in empty.columns])
        return empty

    def _read_files(self, paths: list[str], columns: list[str] | None = None) -> pl.DataFrame:
        """Read a set of Iceberg data files into one Polars frame."""
        import pyarrow.parquet as pq

        io = self.table.io
        frames: list[pl.DataFrame] = []
        for path in paths:
            try:
                # FileIO's reader accessor is `new_input`; `new_input_file`
                # does not exist on PyArrowFileIO.
                input_file = io.new_input(path)
                with input_file.open() as handle:
                    arrow_table = pq.read_table(handle, columns=columns)
            except Exception as e:
                logger.warning("Could not read incremental data file %s: %s", path, e)
                continue
            frames.append(from_arrow_dataframe(arrow_table))

        if not frames:
            return self._empty_frame(columns)
        return pl.concat(frames, how="vertical_relaxed")

    def get_changes(
        self,
        from_snapshot_id: int,
        to_snapshot_id: int | None = None,
        columns: list[str] | None = None,
        primary_keys: list[str] | None = None,
    ) -> dict[str, pl.DataFrame]:
        """
        Get changes (inserts, updates, deletes) between two snapshots.

        Args:
            from_snapshot_id: Starting snapshot ID
            to_snapshot_id: Ending snapshot ID (defaults to current)
            columns: Optional list of columns to select

        Returns:
            Dictionary with 'added', 'deleted', 'modified' DataFrames
        """
        if to_snapshot_id is None:
            current = self.table.current_snapshot()
            to_snapshot_id = current.snapshot_id if current else None

        if not to_snapshot_id:
            return {"added": pl.DataFrame(), "deleted": pl.DataFrame(), "modified": pl.DataFrame()}

        # Read data at both snapshots
        # Note: PyIceberg's snapshot() method allows time-travel reads
        from_scan = self.table.scan(snapshot_id=from_snapshot_id)
        to_scan = self.table.scan(snapshot_id=to_snapshot_id)

        if columns:
            from_scan = from_scan.select(*columns)
            to_scan = to_scan.select(*columns)

        from_df = from_arrow_dataframe(from_scan.to_arrow())
        to_df = from_arrow_dataframe(to_scan.to_arrow())

        if primary_keys:
            missing = [key for key in primary_keys if key not in from_df.columns]
            if missing:
                raise ValidationError(f"Primary key columns not found: {missing}")
            if (
                from_df.select(primary_keys).is_duplicated().any()
                or to_df.select(primary_keys).is_duplicated().any()
            ):
                raise ValidationError("primary_keys must uniquely identify rows")

            added = to_df.join(from_df.select(primary_keys), on=primary_keys, how="anti")
            deleted = from_df.join(to_df.select(primary_keys), on=primary_keys, how="anti")
            value_columns = [c for c in to_df.columns if c not in primary_keys]
            if value_columns:
                joined = from_df.join(to_df, on=primary_keys, how="inner", suffix="__after")
                changed = pl.lit(False)
                for column in value_columns:
                    predicate = ~pl.col(column).eq_missing(pl.col(f"{column}__after"))
                    changed = predicate if changed is None else changed | predicate
                modified = joined.filter(changed).select(
                    primary_keys + [pl.col(f"{c}__after").alias(c) for c in value_columns]
                )
            else:
                modified = pl.DataFrame(schema=to_df.schema)
            return {"added": added, "deleted": deleted, "modified": modified}

        # Added: rows in 'to' but not in 'from'
        added = to_df.join(from_df, how="anti", on=to_df.columns)

        # Deleted: rows in 'from' but not in 'to'
        deleted = from_df.join(to_df, how="anti", on=from_df.columns)

        # Full-row semantics cannot distinguish an update from one delete plus
        # one insert. Callers needing updates should supply primary_keys.
        modified = pl.DataFrame()

        return {"added": added, "deleted": deleted, "modified": modified}

    def _find_snapshot_by_timestamp(self, timestamp_ms: int) -> int | None:
        """Find the snapshot closest to (but before) the given timestamp"""
        snapshots = list(self.table.metadata.snapshots)

        # Find the latest snapshot before the timestamp
        best_snapshot = None
        for snapshot in snapshots:
            if snapshot.timestamp_ms <= timestamp_ms:
                if best_snapshot is None or snapshot.timestamp_ms > best_snapshot.timestamp_ms:
                    best_snapshot = snapshot

        return best_snapshot.snapshot_id if best_snapshot else None
