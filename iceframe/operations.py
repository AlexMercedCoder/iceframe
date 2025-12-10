"""
Table operations for CRUD functionality
"""

from typing import Dict, Any, Optional, List, Union
import pyarrow as pa
import polars as pl
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, DayTransform, MonthTransform, YearTransform
from pyiceberg.types import NestedField, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, TimestampType, DateType

from iceframe.utils import normalize_table_identifier


class TableOperations:
    """Handle table CRUD operations"""
    
    def __init__(self, catalog: Catalog):
        """
        Initialize TableOperations.
        
        Args:
            catalog: PyIceberg catalog instance
        """
        self.catalog = catalog
    
    def _convert_schema(self, schema: Union[Schema, pa.Schema, pl.DataFrame, Dict[str, Any]]) -> Schema:
        """
        Convert various schema formats to PyIceberg Schema.
        
        Args:
            schema: Schema in various formats
            
        Returns:
            PyIceberg Schema object
        """
        if isinstance(schema, Schema):
            return schema
        
        if isinstance(schema, pa.Schema):
            # Convert PyArrow schema to PyIceberg schema
            return Schema(*self._pyarrow_to_iceberg_fields(schema))
        
        if isinstance(schema, pl.DataFrame):
            # Infer schema from Polars DataFrame
            return self._convert_schema(schema.to_arrow().schema)
        
        if isinstance(schema, dict):
            # Convert dict to schema
            fields = []
            for i, (name, type_str) in enumerate(schema.items()):
                field_type = self._string_to_iceberg_type(type_str)
                fields.append(NestedField(field_id=i + 1, name=name, field_type=field_type, required=False))
            return Schema(*fields)
        
        raise ValueError(f"Unsupported schema type: {type(schema)}")
    
    def _pyarrow_to_iceberg_fields(self, pa_schema: pa.Schema) -> List[NestedField]:
        """Convert PyArrow schema fields to PyIceberg fields"""
        fields = []
        for i, field in enumerate(pa_schema):
            iceberg_type = self._pyarrow_to_iceberg_type(field.type)
            fields.append(
                NestedField(
                    field_id=i + 1,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                )
            )
        return fields
    
    def _pyarrow_to_iceberg_type(self, pa_type):
        """Convert PyArrow type to PyIceberg type"""
        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return StringType()
        elif pa.types.is_int32(pa_type):
            return IntegerType()
        elif pa.types.is_int64(pa_type):
            return LongType()
        elif pa.types.is_float32(pa_type):
            return FloatType()
        elif pa.types.is_float64(pa_type):
            return DoubleType()
        elif pa.types.is_boolean(pa_type):
            return BooleanType()
        elif pa.types.is_timestamp(pa_type):
            return TimestampType()
        elif pa.types.is_date(pa_type):
            return DateType()
        else:
            # Default to string for unsupported types
            return StringType()
    
    def _string_to_iceberg_type(self, type_str: str):
        """Convert string type name to PyIceberg type"""
        type_map = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "float": FloatType(),
            "double": DoubleType(),
            "boolean": BooleanType(),
            "timestamp": TimestampType(),
            "date": DateType(),
        }
        return type_map.get(type_str.lower(), StringType())
    
    def create_table(
        self,
        table_name: str,
        schema: Union[Schema, pa.Schema, pl.DataFrame, Dict[str, Any]],
        partition_spec: Optional[List[tuple]] = None,
        sort_order: Optional[List[str]] = None,
        properties: Optional[Dict[str, str]] = None,
    ) -> Table:
        """
        Create a new Iceberg table.
        
        Args:
            table_name: Name of the table
            schema: Table schema
            partition_spec: Optional partition specification
            sort_order: Optional sort order
            properties: Optional table properties
            
        Returns:
            Created Table object
        """
        namespace, table = normalize_table_identifier(table_name)
        
        # Convert schema
        iceberg_schema = self._convert_schema(schema)
        
        # Create table
        full_table_name = f"{namespace}.{table}"
        
        # Ensure namespace exists
        try:
            self.catalog.create_namespace(namespace)
        except Exception:
            # Namespace might already exist
            pass
        
        # Create the table
        table_obj = self.catalog.create_table(
            identifier=full_table_name,
            schema=iceberg_schema,
            properties=properties or {},
        )
        
        return table_obj
    
    def get_table(self, table_name: str) -> Table:
        """Get a table by name"""
        namespace, table = normalize_table_identifier(table_name)
        return self.catalog.load_table(f"{namespace}.{table}")
    
    def read_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[int] = None,
    ) -> pl.DataFrame:
        """
        Read data from a table.
        
        Args:
            table_name: Name of the table
            columns: Optional column selection
            filter_expr: Optional filter expression
            limit: Optional row limit
            snapshot_id: Optional snapshot ID for time travel
            as_of_timestamp: Optional timestamp for time travel
            
        Returns:
            Polars DataFrame
        """
        table = self.get_table(table_name)
        
        # Start with a scan
        scan = table.scan()
        
        # Apply column selection
        if columns:
            scan = scan.select(*columns)
        
        # Apply filter (PyIceberg uses different filter syntax)
        # For now, we'll read all data and filter with Polars
        
        # Apply time travel
        if snapshot_id:
            scan = scan.use_snapshot(snapshot_id)
        elif as_of_timestamp:
            scan = scan.use_ref(str(as_of_timestamp))
        
        # Execute scan and convert to Polars
        arrow_table = scan.to_arrow()
        df = pl.from_arrow(arrow_table)
        
        # Apply filter if provided (using Polars)
        if filter_expr:
            # Simple filter parsing - can be enhanced
            df = df.filter(pl.sql_expr(filter_expr))
        
        # Apply limit
        if limit:
            df = df.head(limit)
        
        return df
    
    def scan_batches(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
        limit: Optional[int] = None,
        snapshot_id: Optional[int] = None,
        as_of_timestamp: Optional[int] = None,
        batch_size: Optional[int] = None,
    ):
        """
        Scan table and return an iterator of PyArrow RecordBatches.
        
        Args:
            table_name: Name of the table
            columns: Optional column selection
            filter_expr: Optional filter expression
            limit: Optional row limit
            snapshot_id: Optional snapshot ID
            as_of_timestamp: Optional timestamp
            batch_size: Optional batch size hint
            
        Returns:
            Iterator of PyArrow RecordBatches
        """
        table = self.get_table(table_name)
        from pyiceberg.expressions import AlwaysTrue
        
        # Build scan arguments, filtering out None values to avoid issues with some PyIceberg versions
        scan_args = {
            "row_filter": filter_expr if filter_expr is not None else AlwaysTrue(),
            "selected_fields": tuple(columns) if columns else ("*",),
            "limit": limit,
            "snapshot_id": snapshot_id,
        }
        if as_of_timestamp is not None:
            scan_args["as_of_timestamp"] = as_of_timestamp
            
        scan = table.scan(**scan_args)
        
        # Note: PyIceberg's to_arrow_batch_reader() returns a pa.RecordBatchReader
        # which is an iterator of RecordBatches
        return scan.to_arrow_batch_reader()
    
    def append_to_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
        branch: Optional[str] = None,
    ) -> None:
        """
        Append data to a table.
        
        Args:
            table_name: Name of the table
            data: Data to append
            branch: Optional branch name to write to
        """
        table = self.get_table(table_name)
        
        # Convert data to PyArrow table
        if isinstance(data, pl.DataFrame):
            arrow_data = data.to_arrow()
        elif isinstance(data, pa.Table):
            arrow_data = data
        elif isinstance(data, dict):
            arrow_data = pa.Table.from_pydict(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        # Append to table
        # Note: PyIceberg's append API might not directly support 'branch' arg in all versions
        # If supported, we pass it. If not, we might need to set WAP properties.
        try:
            # Try passing branch if supported by PyIceberg version
            if branch:
                # Check if append supports branch argument (newer PyIceberg)
                import inspect
                sig = inspect.signature(table.append)
                if 'branch' in sig.parameters:
                    table.append(arrow_data, branch=branch)
                    return
                
                # Fallback: Use WAP properties if branch arg not supported
                # This sets write.wap.enabled=true and write.wap.id=<branch>
                with table.transaction() as txn:
                    txn.set_properties({
                        "write.wap.enabled": "true",
                        "write.wap.id": branch
                    })
                    txn.append(arrow_data)
                return

            table.append(arrow_data)
        except TypeError:
            # Fallback for older versions
            table.append(arrow_data)
    
    def overwrite_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
    ) -> None:
        """Overwrite table data"""
        table = self.get_table(table_name)
        
        # Convert data to PyArrow table
        if isinstance(data, pl.DataFrame):
            arrow_data = data.to_arrow()
        elif isinstance(data, pa.Table):
            arrow_data = data
        elif isinstance(data, dict):
            arrow_data = pa.Table.from_pydict(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
        
        # Overwrite table
        table.overwrite(arrow_data)
    
    def delete_from_table(self, table_name: str, filter_expr: str) -> None:
        """Delete rows from a table"""
        table = self.get_table(table_name)
        
        # PyIceberg delete API
        # Note: This is a simplified version - actual implementation may vary
        # based on PyIceberg version
        table.delete(filter_expr)
    
    def drop_table(self, table_name: str) -> None:
        """Drop a table"""
        namespace, table = normalize_table_identifier(table_name)
        self.catalog.drop_table(f"{namespace}.{table}")
    
    def list_tables(self, namespace: str = "default") -> List[str]:
        """List all tables in a namespace"""
        try:
            tables = self.catalog.list_tables(namespace)
            return [str(t) for t in tables]
        except Exception:
            return []
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        try:
            self.get_table(table_name)
            return True
        except Exception:
            return False
