"""
Table statistics and metadata for IceFrame.
"""

from typing import Dict, Any, Optional
import polars as pl
from pyiceberg.table import Table

class TableStats:
    """
    Provides statistics and metadata for Iceberg tables.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def get_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive table statistics.
        
        Returns:
            Dictionary with table statistics
        """
        metadata = self.table.metadata
        current_snapshot = self.table.current_snapshot()
        
        stats = {
            "table_name": self.table.name(),
            "schema": {
                "fields": len(self.table.schema().fields),
                "columns": [f.name for f in self.table.schema().fields]
            },
            "snapshots": {
                "count": len(list(metadata.snapshots)),
                "current_snapshot_id": current_snapshot.snapshot_id if current_snapshot else None
            },
            "partition_spec": {
                "fields": len(self.table.spec().fields),
                "spec_id": self.table.spec().spec_id
            },
            "sort_order": {
                "fields": len(self.table.sort_order().fields) if self.table.sort_order() else 0
            }
        }
        
        # Add snapshot-level stats if available
        if current_snapshot:
            summary = current_snapshot.summary
            if summary:
                stats["data"] = {
                    "total_records": int(summary.get("total-records", 0)) if summary.get("total-records") else None,
                    "total_data_files": int(summary.get("total-data-files", 0)) if summary.get("total-data-files") else None,
                    "total_delete_files": int(summary.get("total-delete-files", 0)) if summary.get("total-delete-files") else None,
                    "total_size_bytes": int(summary.get("total-size", 0)) if summary.get("total-size") else None
                }
                
        return stats
        
    def profile_column(self, column_name: str) -> Dict[str, Any]:
        """
        Profile a specific column with statistics.
        
        Args:
            column_name: Name of the column to profile
            
        Returns:
            Dictionary with column statistics
        """
        # Read the column data
        scan = self.table.scan().select(column_name)
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        if column_name not in df.columns:
            raise ValueError(f"Column '{column_name}' not found in table")
            
        col = df[column_name]
        
        profile = {
            "column_name": column_name,
            "data_type": str(col.dtype),
            "null_count": col.null_count(),
            "non_null_count": len(col) - col.null_count(),
            "total_count": len(col)
        }
        
        # Add type-specific stats
        if col.dtype in [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64]:
            profile["numeric_stats"] = {
                "min": col.min(),
                "max": col.max(),
                "mean": col.mean(),
                "median": col.median(),
                "std_dev": col.std()
            }
        elif col.dtype == pl.Utf8:
            profile["string_stats"] = {
                "min_length": col.str.len_chars().min(),
                "max_length": col.str.len_chars().max(),
                "avg_length": col.str.len_chars().mean()
            }
            
        # Distinct count (can be expensive for large tables)
        try:
            profile["distinct_count"] = col.n_unique()
        except:
            profile["distinct_count"] = None
            
        return profile
