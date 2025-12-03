"""
Schema evolution for IceFrame.
"""

from typing import Any, Optional, Union
from pyiceberg.table import Table
from pyiceberg.types import IcebergType, StringType, LongType, IntegerType, DoubleType, FloatType, BooleanType, DateType, TimestampType
import pyarrow as pa

class SchemaEvolution:
    """
    Manages schema evolution for Iceberg tables.
    """
    
    def __init__(self, table: Table):
        self.table = table
        
    def add_column(self, name: str, type_str: str, doc: Optional[str] = None) -> None:
        """
        Add a new column to the table.
        
        Args:
            name: Name of the new column
            type_str: Type of the new column (e.g., "string", "int", "long")
            doc: Optional documentation for the column
        """
        iceberg_type = self._parse_type(type_str)
        with self.table.update_schema() as update:
            update.add_column(name, iceberg_type, doc=doc)
            
    def drop_column(self, name: str) -> None:
        """
        Drop a column from the table.
        
        Args:
            name: Name of the column to drop
        """
        with self.table.update_schema() as update:
            update.delete_column(name)
            
    def rename_column(self, old_name: str, new_name: str) -> None:
        """
        Rename a column.
        
        Args:
            old_name: Current name of the column
            new_name: New name for the column
        """
        with self.table.update_schema() as update:
            update.rename_column(old_name, new_name)
            
    def update_column_type(self, name: str, new_type_str: str) -> None:
        """
        Update the type of a column.
        
        Args:
            name: Name of the column
            new_type_str: New type for the column (must be compatible)
        """
        iceberg_type = self._parse_type(new_type_str)
        with self.table.update_schema() as update:
            update.update_column(name, field_type=iceberg_type)
            
    def _parse_type(self, type_str: str) -> IcebergType:
        """Parse string type to IcebergType"""
        type_str = type_str.lower()
        if type_str == "string":
            return StringType()
        elif type_str == "int" or type_str == "integer":
            return IntegerType()
        elif type_str == "long":
            return LongType()
        elif type_str == "double":
            return DoubleType()
        elif type_str == "float":
            return FloatType()
        elif type_str == "boolean" or type_str == "bool":
            return BooleanType()
        elif type_str == "date":
            return DateType()
        elif type_str == "timestamp":
            return TimestampType()
        else:
            raise ValueError(f"Unsupported type: {type_str}")
