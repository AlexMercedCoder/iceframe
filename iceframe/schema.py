"""
Schema evolution for IceFrame.
"""

from typing import Any, Optional, Union, Dict, List
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
            
    def sync_schema(self, df: 'pl.DataFrame', allow_drops: bool = False) -> Dict[str, List[str]]:
        """
        Synchronize table schema with DataFrame schema.
        
        Args:
            df: Polars DataFrame with new schema
            allow_drops: If True, drop columns missing in df (CAUTION)
            
        Returns:
            Dict with changes applied {"added": [], "updated": [], "dropped": []}
        """
        import polars as pl
        
        changes = {"added": [], "updated": [], "dropped": []}
        
        current_schema = self.table.schema()
        # Map current fields: name -> field
        # Note: only top-level fields for simplicity (nested support is complex)
        current_fields = {f.name: f for f in current_schema.fields}
        
        new_schema = df.schema
        # Polars schema is dict-like {name: DataType}
        
        for name, dtype in new_schema.items():
            if name not in current_fields:
                # Add new column
                iceberg_type = self._polars_to_iceberg(dtype)
                with self.table.update_schema() as update:
                    update.add_column(name, iceberg_type)
                changes["added"].append(name)
            else:
                # Check for type promotion (e.g. int -> long)
                # This requires comparing PyIceberg types with Polars types
                current_type = current_fields[name].field_type
                new_type = self._polars_to_iceberg(dtype)
                
                # Simple string representation comparison or direct type check
                if current_type != new_type:
                    # Attempt update (PyIceberg will validate compatibility)
                    try:
                        with self.table.update_schema() as update:
                            update.update_column(name, field_type=new_type)
                        changes["updated"].append(f"{name}: {current_type} -> {new_type}")
                    except Exception:
                        # Incompatible type change
                        pass
                        
        if allow_drops:
            for name in current_fields:
                if name not in new_schema:
                    try:
                        with self.table.update_schema() as update:
                            update.delete_column(name)
                        changes["dropped"].append(name)
                    except Exception as e:
                        print(f"Failed to drop column {name}: {e}")
                        
        return changes

    def _polars_to_iceberg(self, pl_type) -> IcebergType:
        """Convert Polars type to IcebergType"""
        import polars as pl
        
        # String/Utf8
        if pl_type == pl.Utf8 or pl_type == pl.String:
            return StringType()
        # Integers
        elif pl_type in (pl.Int8, pl.Int16, pl.Int32):
            return IntegerType()
        elif pl_type == pl.Int64:
            return LongType()
        # Floats
        elif pl_type == pl.Float32:
            return FloatType()
        elif pl_type == pl.Float64:
            return DoubleType()
        # Boolean
        elif pl_type == pl.Boolean:
            return BooleanType()
        # Date/Time
        elif pl_type == pl.Date:
            return DateType()
        elif pl_type == pl.Datetime:
            return TimestampType()
        else:
            # Default to string for unknown/complex types
            return StringType()

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
