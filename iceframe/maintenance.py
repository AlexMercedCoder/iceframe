"""
Table maintenance operations
"""

from typing import Optional
from datetime import datetime, timedelta
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from iceframe.utils import normalize_table_identifier


class TableMaintenance:
    """Handle Iceberg table maintenance operations"""
    
    def __init__(self, catalog: Catalog):
        """
        Initialize TableMaintenance.
        
        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog
    
    def _get_table(self, table_name: str) -> Table:
        """Get table by name"""
        namespace, table = normalize_table_identifier(table_name)
        return self.catalog.load_table(f"{namespace}.{table}")
    
    def expire_snapshots(
        self,
        table_name: str,
        older_than_days: int = 7,
        retain_last: int = 1,
    ) -> None:
        """
        Expire old snapshots from a table.
        
        Args:
            table_name: Name of the table
            older_than_days: Remove snapshots older than this many days
            retain_last: Always retain at least this many snapshots
        """
        table = self._get_table(table_name)
        
        # Calculate timestamp threshold
        expire_timestamp_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
        # Expire snapshots
        try:
            table.expire_snapshots(
                older_than=expire_timestamp_ms,
                retain_last=retain_last,
            )
        except AttributeError:
            # Fallback for different PyIceberg versions
            print(f"Snapshot expiration not supported for table {table_name}")
    
    def remove_orphan_files(
        self,
        table_name: str,
        older_than_days: int = 3,
    ) -> None:
        """
        Remove orphaned data files from a table.
        
        Args:
            table_name: Name of the table
            older_than_days: Remove files older than this many days
        """
        table = self._get_table(table_name)
        
        # Calculate timestamp threshold
        older_than_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
        # Remove orphan files
        try:
            table.remove_orphan_files(older_than_ms=older_than_ms)
        except AttributeError:
            print(f"Orphan file removal not supported for table {table_name}")
    
    def compact_data_files(
        self,
        table_name: str,
        target_file_size_mb: int = 512,
    ) -> None:
        """
        Compact small data files into larger ones.
        
        Args:
            table_name: Name of the table
            target_file_size_mb: Target file size in MB
        """
        table = self._get_table(table_name)
        
        target_size_bytes = target_file_size_mb * 1024 * 1024
        
        # Rewrite data files
        try:
            table.rewrite_data_files(
                target_file_size_bytes=target_size_bytes,
            )
        except AttributeError:
            print(f"Data file compaction not supported for table {table_name}")
    
    def rewrite_manifests(self, table_name: str) -> None:
        """
        Rewrite manifest files to optimize metadata.
        
        Args:
            table_name: Name of the table
        """
        table = self._get_table(table_name)
        
        try:
            table.rewrite_manifests()
        except AttributeError:
            print(f"Manifest rewriting not supported for table {table_name}")
