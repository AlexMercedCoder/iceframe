"""
Core IceFrame class - Main entry point for the library
"""

from typing import Dict, Any, Optional, List, Union
import pyarrow as pa
import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table

from iceframe.utils import validate_catalog_config, normalize_table_identifier, format_table_identifier
from iceframe.operations import TableOperations
from iceframe.maintenance import TableMaintenance
from iceframe.export import DataExporter


class IceFrame:
    """
    Main class for interacting with Apache Iceberg tables.
    
    Provides a DataFrame-like API for CRUD operations, maintenance, and exports.
    """
    
    def __init__(self, catalog_config: Dict[str, Any]):
        """
        Initialize IceFrame with catalog configuration.
        
        Args:
            catalog_config: Dictionary containing catalog configuration.
                Required keys: 'uri', 'type'
                Optional keys: 'token', 'oauth2-server-uri', 'warehouse',
                              'header.X-Iceberg-Access-Delegation'
        
        Example:
            >>> config = {
            ...     "uri": "https://catalog.dremio.cloud/api/iceberg",
            ...     "oauth2-server-uri": "https://login.dremio.cloud/oauth/token",
            ...     "token": "your_token",
            ...     "warehouse": "firstproject",
            ...     "header.X-Iceberg-Access-Delegation": "vended-credentials",
            ...     "type": "rest"
            ... }
            >>> ice = IceFrame(catalog_config=config)
        """
        validate_catalog_config(catalog_config)
        self.catalog_config = catalog_config
        self.catalog = load_catalog("default", **catalog_config)
        
        # Initialize helper classes
        self._operations = TableOperations(self.catalog)
        self._maintenance = TableMaintenance(self.catalog)
        self._exporter = DataExporter()
    
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
            table_name: Name of the table (can include namespace: 'namespace.table')
            schema: Table schema - can be PyIceberg Schema, PyArrow Schema,
                   Polars DataFrame (schema inferred), or dict mapping column names to types
            partition_spec: Optional list of partition field tuples
            sort_order: Optional list of sort field names
            properties: Optional table properties
            
        Returns:
            Created Iceberg Table object
            
        Example:
            >>> # Create with PyArrow schema
            >>> schema = pa.schema([
            ...     pa.field("id", pa.int64()),
            ...     pa.field("name", pa.string()),
            ...     pa.field("created_at", pa.timestamp("us"))
            ... ])
            >>> table = ice.create_table("my_namespace.my_table", schema)
        """
        return self._operations.create_table(
            table_name=table_name,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )
    
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
        Read data from an Iceberg table.
        
        Args:
            table_name: Name of the table to read
            columns: Optional list of columns to select
            filter_expr: Optional filter expression
            limit: Optional row limit
            snapshot_id: Optional snapshot ID for time travel
            as_of_timestamp: Optional timestamp for time travel
            
        Returns:
            Polars DataFrame containing the data
            
        Example:
            >>> df = ice.read_table("my_namespace.my_table", columns=["id", "name"])
            >>> df = ice.read_table("my_table", limit=100)
        """
        return self._operations.read_table(
            table_name=table_name,
            columns=columns,
            filter_expr=filter_expr,
            limit=limit,
            snapshot_id=snapshot_id,
            as_of_timestamp=as_of_timestamp,
        )
    
    def append_to_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
    ) -> None:
        """
        Append data to an existing table.
        
        Args:
            table_name: Name of the table
            data: Data to append (Polars DataFrame, PyArrow Table, or dict)
            
        Example:
            >>> df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> ice.append_to_table("my_table", df)
        """
        self._operations.append_to_table(table_name, data)
    
    def overwrite_table(
        self,
        table_name: str,
        data: Union[pl.DataFrame, pa.Table, Dict[str, list]],
    ) -> None:
        """
        Overwrite table data.
        
        Args:
            table_name: Name of the table
            data: Data to write (Polars DataFrame, PyArrow Table, or dict)
            
        Example:
            >>> df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>> ice.overwrite_table("my_table", df)
        """
        self._operations.overwrite_table(table_name, data)
    
    def delete_from_table(
        self,
        table_name: str,
        filter_expr: str,
    ) -> None:
        """
        Delete rows from a table based on filter expression.
        
        Args:
            table_name: Name of the table
            filter_expr: Filter expression for rows to delete
            
        Example:
            >>> ice.delete_from_table("my_table", "id < 100")
        """
        self._operations.delete_from_table(table_name, filter_expr)
    
    def drop_table(self, table_name: str) -> None:
        """
        Drop (delete) a table from the catalog.
        
        Args:
            table_name: Name of the table to drop
            
        Example:
            >>> ice.drop_table("my_namespace.my_table")
        """
        self._operations.drop_table(table_name)
    
    def list_tables(self, namespace: str = "default") -> List[str]:
        """
        List all tables in a namespace.
        
        Args:
            namespace: Namespace to list tables from
            
        Returns:
            List of table names
            
        Example:
            >>> tables = ice.list_tables("my_namespace")
        """
        return self._operations.list_tables(namespace)
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists.
        
        Args:
            table_name: Name of the table
            
        Returns:
            True if table exists, False otherwise
            
        Example:
            >>> if ice.table_exists("my_table"):
            ...     print("Table exists!")
        """
        return self._operations.table_exists(table_name)
    
    def get_table(self, table_name: str) -> Table:
        """
        Get the underlying PyIceberg Table object.
        
        Args:
            table_name: Name of the table
            
        Returns:
            PyIceberg Table object
            
        Example:
            >>> table = ice.get_table("my_table")
            >>> print(table.schema())
        """
        return self._operations.get_table(table_name)
    
    # Maintenance operations
    
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
            
        Example:
            >>> ice.expire_snapshots("my_table", older_than_days=30, retain_last=5)
        """
        self._maintenance.expire_snapshots(table_name, older_than_days, retain_last)
    
    def remove_orphan_files(self, table_name: str, older_than_days: int = 3) -> None:
        """
        Remove orphaned data files from a table.
        
        Args:
            table_name: Name of the table
            older_than_days: Remove files older than this many days
            
        Example:
            >>> ice.remove_orphan_files("my_table", older_than_days=7)
        """
        self._maintenance.remove_orphan_files(table_name, older_than_days)
    
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
            
        Example:
            >>> ice.compact_data_files("my_table", target_file_size_mb=256)
        """
        self._maintenance.compact_data_files(table_name, target_file_size_mb)
    
    # Export operations
    
    def to_parquet(
        self,
        table_name: str,
        output_path: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
    ) -> None:
        """
        Export table data to Parquet file.
        
        Args:
            table_name: Name of the table
            output_path: Path to output Parquet file
            columns: Optional list of columns to export
            filter_expr: Optional filter expression
            
        Example:
            >>> ice.to_parquet("my_table", "/tmp/output.parquet")
        """
        df = self.read_table(table_name, columns=columns, filter_expr=filter_expr)
        self._exporter.to_parquet(df, output_path)
    
    def to_csv(
        self,
        table_name: str,
        output_path: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
    ) -> None:
        """
        Export table data to CSV file.
        
        Args:
            table_name: Name of the table
            output_path: Path to output CSV file
            columns: Optional list of columns to export
            filter_expr: Optional filter expression
            
        Example:
            >>> ice.to_csv("my_table", "/tmp/output.csv")
        """
        df = self.read_table(table_name, columns=columns, filter_expr=filter_expr)
        self._exporter.to_csv(df, output_path)
    
    def to_json(
        self,
        table_name: str,
        output_path: str,
        columns: Optional[List[str]] = None,
        filter_expr: Optional[str] = None,
    ) -> None:
        """
        Export table data to JSON file.
        
        Args:
            table_name: Name of the table
            output_path: Path to output JSON file
            columns: Optional list of columns to export
            filter_expr: Optional filter expression
            
        Example:
            >>> ice.to_json("my_table", "/tmp/output.json")
        """
        df = self.read_table(table_name, columns=columns, filter_expr=filter_expr)
        self._exporter.to_json(df, output_path)

    def query(self, table_name: str) -> 'QueryBuilder':
        """
        Start a query builder for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            QueryBuilder instance
        """
        from iceframe.query import QueryBuilder
        return QueryBuilder(self._operations, table_name)

    # Namespace Management
    
    @property
    def namespaces(self):
        """Access namespace manager"""
        from iceframe.namespace import NamespaceManager
        return NamespaceManager(self.catalog)

    def create_namespace(self, name: str, properties: Optional[Dict[str, str]] = None) -> None:
        """Create a new namespace"""
        self.namespaces.create_namespace(name, properties)
        
    def drop_namespace(self, name: str) -> None:
        """Drop a namespace"""
        self.namespaces.drop_namespace(name)
        
    def list_namespaces(self, parent: Optional[str] = None) -> List[tuple]:
        """List namespaces"""
        return self.namespaces.list_namespaces(parent)

    # Schema Evolution
    
    def alter_table(self, table_name: str) -> 'SchemaEvolution':
        """
        Get schema evolution interface for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            SchemaEvolution instance
        """
        from iceframe.schema import SchemaEvolution
        table = self.get_table(table_name)
        return SchemaEvolution(table)

    # Partition Management
    
    def partition_by(self, table_name: str) -> 'PartitionManager':
        """
        Get partition management interface for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            PartitionManager instance
        """
        from iceframe.partition import PartitionManager
        table = self.get_table(table_name)
        return PartitionManager(table)
