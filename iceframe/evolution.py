"""
Partition evolution support.
"""

from typing import Optional
from pyiceberg.table import Table
from pyiceberg.transforms import Transform

class PartitionEvolution:
    """
    Manage partition evolution.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def add_identity_partition(self, source_column: str) -> None:
        """Add an identity partition field"""
        with self.table.update_spec() as update:
            update.add_identity(source_column)
            
    def add_bucket_partition(self, source_column: str, num_buckets: int) -> None:
        """Add a bucket partition field"""
        with self.table.update_spec() as update:
            update.add_bucket(source_column, num_buckets)
            
    def add_truncate_partition(self, source_column: str, width: int) -> None:
        """Add a truncate partition field"""
        with self.table.update_spec() as update:
            update.add_truncate(source_column, width)
            
    def add_year_partition(self, source_column: str) -> None:
        """Add a year partition field"""
        with self.table.update_spec() as update:
            update.add_year(source_column)
            
    def add_month_partition(self, source_column: str) -> None:
        """Add a month partition field"""
        with self.table.update_spec() as update:
            update.add_month(source_column)
            
    def add_day_partition(self, source_column: str) -> None:
        """Add a day partition field"""
        with self.table.update_spec() as update:
            update.add_day(source_column)
            
    def add_hour_partition(self, source_column: str) -> None:
        """Add an hour partition field"""
        with self.table.update_spec() as update:
            update.add_hour(source_column)
            
    def remove_partition(self, source_column: str) -> None:
        """Remove a partition field"""
        with self.table.update_spec() as update:
            update.remove_field(source_column)
