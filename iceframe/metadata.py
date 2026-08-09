"""
Metadata tables, exposed as Polars DataFrames.

Thin, typed wrapper over PyIceberg's ``Table.inspect``. This replaces
hand-parsing ``current_snapshot.summary`` and gives users the same
introspection Spark users expect::

    ice.inspect("db.events").snapshots()
    ice.inspect("db.events").files()
    ice.inspect("db.events").partitions()
"""

import logging
from typing import Any, List, Optional

import polars as pl
from pyiceberg.table import Table

from iceframe.exceptions import UnsupportedOperationError

logger = logging.getLogger(__name__)

#: The metadata tables PyIceberg exposes via ``Table.inspect``.
METADATA_TABLES = (
    "snapshots",
    "entries",
    "refs",
    "partitions",
    "manifests",
    "metadata_log_entries",
    "history",
    "files",
    "data_files",
    "delete_files",
)


class MetadataInspector:
    """Access Iceberg metadata tables for a single table as Polars frames."""

    def __init__(self, table: Table):
        self.table = table

    def _frame(self, name: str, **kwargs: Any) -> pl.DataFrame:
        inspector = self.table.inspect
        method = getattr(inspector, name, None)
        if method is None:
            raise UnsupportedOperationError(
                f"Metadata table {name!r} is not available in this PyIceberg version. "
                f"Available: {', '.join(self.available())}"
            )
        return pl.from_arrow(method(**kwargs))

    def available(self) -> List[str]:
        """Metadata table names actually supported by the installed PyIceberg."""
        return [name for name in METADATA_TABLES if hasattr(self.table.inspect, name)]

    def snapshots(self) -> pl.DataFrame:
        """One row per snapshot: id, parent, timestamp, operation, summary."""
        return self._frame("snapshots")

    def entries(self, snapshot_id: Optional[int] = None) -> pl.DataFrame:
        """Raw manifest entries (status, snapshot, data file, readable metrics)."""
        return self._frame("entries", snapshot_id=snapshot_id)

    def refs(self) -> pl.DataFrame:
        """Branches and tags."""
        return self._frame("refs")

    def partitions(self, snapshot_id: Optional[int] = None) -> pl.DataFrame:
        """Per-partition record and file counts."""
        return self._frame("partitions", snapshot_id=snapshot_id)

    def manifests(self) -> pl.DataFrame:
        """Manifest files of the current snapshot."""
        return self._frame("manifests")

    def metadata_log_entries(self) -> pl.DataFrame:
        """The table's metadata-file history."""
        return self._frame("metadata_log_entries")

    def history(self) -> pl.DataFrame:
        """Snapshot history with ancestry information."""
        return self._frame("history")

    def files(self, snapshot_id: Optional[int] = None) -> pl.DataFrame:
        """All files (data + delete) in a snapshot."""
        return self._frame("files", snapshot_id=snapshot_id)

    def data_files(self, snapshot_id: Optional[int] = None) -> pl.DataFrame:
        """Data files only."""
        return self._frame("data_files", snapshot_id=snapshot_id)

    def delete_files(self, snapshot_id: Optional[int] = None) -> pl.DataFrame:
        """Delete files only."""
        return self._frame("delete_files", snapshot_id=snapshot_id)


__all__ = ["MetadataInspector", "METADATA_TABLES"]
