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
        filter_expr: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Compact small files into larger files (Bin-packing).
        
        Args:
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter to select files to compact
            
        Returns:
            Stats on compacted files
        """
        # 1. Identify files to compact
        # Scan table to find small files
        # This is a simplified implementation using rewrite_data_files if available
        # or manual read-write-replace
        
        try:
            # PyIceberg might have basic support
            # self.table.rewrite_data_files()
            pass
        except AttributeError:
            pass
            
        # Manual implementation:
        # 1. Scan data to get file list
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)
            
        # 2. Read data into memory (Polars)
        # Warning: This loads data into memory. For large tables, needs distributed engine.
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        if df.height == 0:
            return {"rewritten_files": 0, "added_files": 0}
            
        # 3. Write new files
        # We use overwrite to replace existing data
        # Ideally we should target specific partitions
        
        # For this implementation, we'll use overwrite which replaces data
        # This effectively compacts the data read into new files
        self.table.overwrite(df.to_arrow())
        
        return {
            "rewritten_rows": df.height,
            "strategy": "bin_pack"
        }

    def sort(
        self,
        sort_order: List[str],
        target_file_size_mb: int = 128,
        filter_expr: Optional[str] = None
    ) -> Dict[str, int]:
        """
        Compact and sort files (Z-order approximation if multiple columns).
        
        Args:
            sort_order: List of columns to sort by
            target_file_size_mb: Target size in MB
            filter_expr: Optional filter
            
        Returns:
            Stats
        """
        # 1. Read data
        scan = self.table.scan()
        if filter_expr:
            scan = scan.filter(filter_expr)
            
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        if df.height == 0:
            return {"rewritten_files": 0}
            
        # 2. Sort data
        sorted_df = df.sort(sort_order)
        
        # 3. Overwrite
        self.table.overwrite(sorted_df.to_arrow())
        
        return {
            "rewritten_rows": df.height,
            "strategy": "sort",
            "sort_order": str(sort_order)
        }

    def rewrite_manifests(self) -> None:
        """
        Rewrite manifest files to optimize metadata.
        """
        try:
            # PyIceberg support check
            if hasattr(self.table, "rewrite_manifests"):
                self.table.rewrite_manifests().commit()
            else:
                # Fallback or error if not supported
                # Currently PyIceberg doesn't expose this widely in public API for all catalogs
                # but it's a standard Iceberg operation
                raise NotImplementedError("Manifest rewriting not supported by this PyIceberg version")
        except AttributeError:
            raise NotImplementedError("Manifest rewriting not supported by this PyIceberg version")
