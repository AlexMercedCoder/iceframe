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
        
        # Use simple calculation if specific logic needed, but let GC handle it
        older_than_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
        from iceframe.gc import GarbageCollector
        gc = GarbageCollector(table)
        gc.expire_snapshots(older_than_ms=older_than_ms, retain_last=retain_last)
    
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
        
        older_than_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
        from iceframe.gc import GarbageCollector
        gc = GarbageCollector(table)
        gc.remove_orphan_files(older_than_ms=older_than_ms)
    
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
        
        from iceframe.compaction import CompactionManager
        compactor = CompactionManager(table)
        compactor.bin_pack(target_file_size_mb=target_file_size_mb)
    
    def rewrite_manifests(self, table_name: str) -> None:
        """
        Rewrite manifest files to optimize metadata.
        
        Args:
            table_name: Name of the table
        """
        table = self._get_table(table_name)
        
        from iceframe.compaction import CompactionManager
        compactor = CompactionManager(table)
        compactor.rewrite_manifests()
