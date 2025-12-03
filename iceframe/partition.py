"""
Partition management for IceFrame.
"""

from typing import Any, Optional
from pyiceberg.table import Table
from pyiceberg.transforms import Transform, IdentityTransform, BucketTransform, TruncateTransform, YearTransform, MonthTransform, DayTransform, HourTransform

class PartitionManager:
    """
    Manages partitioning for Iceberg tables.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def add_partition_field(self, source_col: str, transform: str = "identity", transform_arg: Optional[int] = None, name: Optional[str] = None) -> None:
        """
        Add a partition field to the table.
        
        Args:
            source_col: Name of the source column
            transform: Transform type ("identity", "bucket", "truncate", "year", "month", "day", "hour")
            transform_arg: Argument for transform (e.g., number of buckets, truncation width)
            name: Optional name for the partition field
        """
        iceberg_transform = self._create_transform(transform, transform_arg)
        with self.table.update_spec() as update:
            update.add_field(source_col, iceberg_transform, name)
            
    def drop_partition_field(self, name: str) -> None:
        """
        Drop a partition field.
        
        Args:
            name: Name of the partition field (or transform string if name not set)
        """
        with self.table.update_spec() as update:
            update.remove_field(name)
            
    def _create_transform(self, transform: str, arg: Optional[int]) -> Transform:
        """Create Iceberg transform object"""
        transform = transform.lower()
        if transform == "identity":
            return IdentityTransform()
        elif transform == "bucket":
            if arg is None:
                raise ValueError("Bucket transform requires an argument (number of buckets)")
            return BucketTransform(arg)
        elif transform == "truncate":
            if arg is None:
                raise ValueError("Truncate transform requires an argument (width)")
            return TruncateTransform(arg)
        elif transform == "year":
            return YearTransform()
        elif transform == "month":
            return MonthTransform()
        elif transform == "day":
            return DayTransform()
        elif transform == "hour":
            return HourTransform()
        else:
            raise ValueError(f"Unsupported transform: {transform}")
