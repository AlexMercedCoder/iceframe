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
from iceframe.pool import CatalogPool
from iceframe.parallel import ParallelExecutor
from iceframe.memory import MemoryManager


class IceFrame:
    """
    Main class for interacting with Apache Iceberg tables.
    
    Provides a DataFrame-like API for CRUD operations, maintenance, and exports.
    """
    
    def __init__(self, catalog_config: Dict[str, Any], pool_size: int = 5):
        """
        Initialize IceFrame with catalog configuration.
        
        Args:
            catalog_config: Dictionary containing catalog configuration.
                           Must include 'uri' and 'type' keys.
            pool_size: Size of connection pool (default: 5)
                           
        Example:
            >>> config = {
            ...     "uri": "http://localhost:8181",
            ...     "type": "rest",
            ...     "warehouse": "s3://my-bucket/warehouse"
            ... }
            >>> ice = IceFrame(config)
        """
        validate_catalog_config(catalog_config)
        self.catalog_config = catalog_config
        
        # Initialize connection pool
        self._pool = CatalogPool(catalog_config, pool_size=pool_size)
        self.catalog = self._pool.get_connection()
        
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

    # Data Quality
    
    @property
    def validator(self):
        """Access data validator"""
        from iceframe.quality import DataValidator
        return DataValidator()

    # Incremental Processing
    
    def read_incremental(
        self,
        table_name: str,
        since_snapshot_id: Optional[int] = None,
        since_timestamp: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Read only data added since a specific snapshot or timestamp.
        
        Args:
            table_name: Name of the table
            since_snapshot_id: Read data added after this snapshot ID
            since_timestamp: Read data added after this timestamp (ms since epoch)
            columns: Optional list of columns to select
            
        Returns:
            Polars DataFrame with incremental data
        """
        from iceframe.incremental import IncrementalReader
        table = self.get_table(table_name)
        reader = IncrementalReader(table)
        return reader.read_incremental(since_snapshot_id, since_timestamp, columns)
        
    def get_changes(
        self,
        table_name: str,
        from_snapshot_id: int,
        to_snapshot_id: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> Dict[str, pl.DataFrame]:
        """
        Get changes (inserts, deletes) between two snapshots.
        
        Args:
            table_name: Name of the table
            from_snapshot_id: Starting snapshot ID
            to_snapshot_id: Ending snapshot ID (defaults to current)
            columns: Optional list of columns to select
            
        Returns:
            Dictionary with 'added', 'deleted', 'modified' DataFrames
        """
        from iceframe.incremental import IncrementalReader
        table = self.get_table(table_name)
        reader = IncrementalReader(table)
        return reader.get_changes(from_snapshot_id, to_snapshot_id, columns)

    # Table Statistics
    
    def stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive table statistics.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        from iceframe.stats import TableStats
        table = self.get_table(table_name)
        stats_obj = TableStats(table)
        return stats_obj.get_stats()
        
    def validate_data(self, table_name: str, constraints: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Validate data in a table against constraints.
        
        Args:
            table_name: Name of the table
            constraints: List of constraints to check
            
        Returns:
            Dictionary with validation results
        """
        from iceframe.quality import DataValidator
        
        df = self.read_table(table_name)
        validator = DataValidator()
        return validator.validate(df, constraints)

    # Scalability Features
    
    def read_tables_parallel(
        self,
        table_names: List[str],
        max_workers: int = 4,
        **read_kwargs
    ) -> Dict[str, pl.DataFrame]:
        """
        Read multiple tables in parallel.
        
        Args:
            table_names: List of table names to read
            max_workers: Number of worker threads
            **read_kwargs: Arguments passed to read_table
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        from iceframe.parallel import ParallelExecutor
        executor = ParallelExecutor(max_workers=max_workers)
        return executor.read_tables_parallel(self, table_names, **read_kwargs)
        
    def read_table_chunked(
        self,
        table_name: str,
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None,
        max_memory_mb: Optional[int] = None
    ):
        """
        Read table in chunks to manage memory usage.
        
        Args:
            table_name: Name of the table
            chunk_size: Number of rows per chunk
            columns: Optional columns to select
            max_memory_mb: Optional memory limit in MB
            
        Yields:
            DataFrame chunks
        """
        from iceframe.memory import MemoryManager
        manager = MemoryManager(max_memory_mb=max_memory_mb)
        return manager.read_table_chunked(self, table_name, chunk_size, columns)
        
    def profile_column(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Profile a specific column with statistics.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column to profile
            
        Returns:
            Dictionary with column statistics
        """
        from iceframe.stats import TableStats
        table = self.get_table(table_name)
        stats_obj = TableStats(table)
        return stats_obj.profile_column(column_name)

    # Branching Support
    
    def create_branch(self, table_name: str, branch_name: str, snapshot_id: Optional[int] = None) -> None:
        """
        Create a new branch.
        
        Args:
            table_name: Name of the table
            branch_name: Name of the branch
            snapshot_id: Snapshot ID to branch from (defaults to current)
        """
        from iceframe.branching import BranchManager
        table = self.get_table(table_name)
        manager = BranchManager(table)
        manager.create_branch(branch_name, snapshot_id)
        
    def tag_snapshot(self, table_name: str, snapshot_id: int, tag_name: str) -> None:
        """
        Tag a specific snapshot.
        
        Args:
            table_name: Name of the table
            snapshot_id: Snapshot ID to tag
            tag_name: Name for the tag
        """
        from iceframe.branching import BranchManager
        table = self.get_table(table_name)
        manager = BranchManager(table)
        manager.tag_snapshot(snapshot_id, tag_name)
