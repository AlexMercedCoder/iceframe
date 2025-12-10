"""
Core IceFrame class - Main entry point for the library
"""

from typing import Dict, Any, Optional, List, Union, Type
import pyarrow as pa
import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
try:
    from pydantic import BaseModel
    HAS_PYDANTIC = True
except ImportError:
    HAS_PYDANTIC = False
    BaseModel = Any # type: ignore


from iceframe.utils import validate_catalog_config, normalize_table_identifier, format_table_identifier
from iceframe.operations import TableOperations

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
        

        self._operations = TableOperations(self.catalog)
        self._exporter = DataExporter()

    def _repr_html_(self) -> str:
        """HTML representation for Jupyter Notebooks"""
        uri = self.catalog_config.get("uri", "unknown")
        catalog_type = self.catalog_config.get("type", "unknown")
        warehouse = self.catalog_config.get("warehouse", "unknown")
        
        # Get namespaces
        try:
            namespaces = self.list_namespaces()
            ns_list = "<ul>" + "".join([f"<li>{ns[0]}</li>" for ns in namespaces]) + "</ul>"
        except Exception:
            ns_list = "Could not list namespaces"
            
        return f"""
        <div style="border: 1px solid #e0e0e0; border-radius: 4px; padding: 10px;">
            <h3>IceFrame Connection</h3>
            <p><strong>URI:</strong> {uri}</p>
            <p><strong>Type:</strong> {catalog_type}</p>
            <p><strong>Warehouse:</strong> {warehouse}</p>
            <hr>
            <h4>Namespaces</h4>
            {ns_list}
        </div>
        """

    
    def create_table(
        self,
        table_name: str,
        schema: Union[Schema, pa.Schema, pl.DataFrame, Dict[str, Any], Type[BaseModel]],
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
        if HAS_PYDANTIC and isinstance(schema, type) and issubclass(schema, BaseModel):
            from iceframe.pydantic import to_iceberg_schema
            schema = to_iceberg_schema(schema)

        return self._operations.create_table(
            table_name=table_name,
            schema=schema,
            partition_spec=partition_spec,
            sort_order=sort_order,
            properties=properties,
        )
    
    def create_table_from_delta(
        self,
        table_name: str,
        path: str,
        version: Optional[int] = None,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Delta Lake table.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Delta table
            version: Optional Delta table version
            **kwargs: Additional arguments for read_delta
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_delta
        df = read_delta(path, version=version, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_lance(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Lance dataset.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Lance dataset
            **kwargs: Additional arguments for read_lance
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_lance
        df = read_lance(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_vortex(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Vortex file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Vortex file
            **kwargs: Additional arguments for read_vortex
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_vortex
        df = read_vortex(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_excel(
        self,
        table_name: str,
        path: str,
        sheet_name: str = "Sheet1",
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an Excel file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Excel file
            sheet_name: Name of the sheet to read
            **kwargs: Additional arguments for read_excel
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_excel
        df = read_excel(path, sheet_name=sheet_name, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_gsheets(
        self,
        table_name: str,
        url: str,
        credentials: Any = None,
        sheet_name: Optional[str] = None,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Google Sheet.
        
        Args:
            table_name: Name of the new Iceberg table
            url: URL of the Google Sheet
            credentials: Path to service account JSON or credentials object
            sheet_name: Optional worksheet name
            **kwargs: Additional arguments for read_gsheets
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_gsheets
        df = read_gsheets(url, credentials=credentials, sheet_name=sheet_name, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_hudi(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Hudi table.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Hudi table
            **kwargs: Additional arguments for read_hudi
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_hudi
        df = read_hudi(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_csv(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a CSV file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to CSV file
            **kwargs: Additional arguments for read_csv
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_csv
        df = read_csv(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_json(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a JSON file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to JSON file
            **kwargs: Additional arguments for read_json
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_json
        df = read_json(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_parquet(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Parquet file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Parquet file
            **kwargs: Additional arguments for read_parquet
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_parquet
        df = read_parquet(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_ipc(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an IPC/Arrow file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to IPC file
            **kwargs: Additional arguments for read_ipc
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_ipc
        df = read_ipc(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_avro(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an Avro file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Avro file
            **kwargs: Additional arguments for read_avro
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_avro
        df = read_avro(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table
    
    def create_table_from_orc(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an ORC file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to ORC file
            **kwargs: Additional arguments for read_orc
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_orc
        df = read_orc(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

        return table

    def create_table_from_sql(
        self,
        table_name: str,
        query: str,
        connection_uri: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a SQL query.
        
        Args:
            table_name: Name of the new Iceberg table
            query: SQL query
            connection_uri: Database connection URI
            **kwargs: Additional arguments for read_sql
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_sql
        df = read_sql(query, connection_uri, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_xml(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an XML file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to XML file
            **kwargs: Additional arguments for read_xml
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_xml
        df = read_xml(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_sas(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a SAS file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to SAS file
            **kwargs: Additional arguments for read_sas
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_sas
        df = read_sas(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_spss(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an SPSS file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to SPSS file
            **kwargs: Additional arguments for read_spss
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_spss
        df = read_spss(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_stata(
        self,
        table_name: str,
        path: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a Stata file.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to Stata file
            **kwargs: Additional arguments for read_stata
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_stata
        df = read_stata(path, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_api(
        self,
        table_name: str,
        url: str,
        json_key: Optional[str] = None,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a REST API.
        
        Args:
            table_name: Name of the new Iceberg table
            url: URL to fetch data from
            json_key: Optional key to extract list of records
            **kwargs: Additional arguments for read_api
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_api
        df = read_api(url, json_key=json_key, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_huggingface(
        self,
        table_name: str,
        dataset_name: str,
        split: str = "train",
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from a HuggingFace dataset.
        
        Args:
            table_name: Name of the new Iceberg table
            dataset_name: Name of the dataset
            split: Split to read
            **kwargs: Additional arguments for read_huggingface
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_huggingface
        df = read_huggingface(dataset_name, split=split, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_html(
        self,
        table_name: str,
        url: str,
        match: Optional[str] = None,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from an HTML table.
        
        Args:
            table_name: Name of the new Iceberg table
            url: URL or HTML string
            match: Optional regex to match table
            **kwargs: Additional arguments for read_html
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_html
        df = read_html(url, match=match, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_clipboard(
        self,
        table_name: str,
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from the clipboard.
        
        Args:
            table_name: Name of the new Iceberg table
            **kwargs: Additional arguments for read_clipboard
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_clipboard
        df = read_clipboard(**kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def create_table_from_folder(
        self,
        table_name: str,
        path: str,
        pattern: str = "*",
        **kwargs
    ) -> Table:
        """
        Create an Iceberg table from files in a folder.
        
        Args:
            table_name: Name of the new Iceberg table
            path: Path to folder
            pattern: Glob pattern
            **kwargs: Additional arguments for read_folder
            
        Returns:
            Created Iceberg Table
        """
        from iceframe.ingest import read_folder
        df = read_folder(path, pattern=pattern, **kwargs)
        table = self.create_table(table_name, schema=df)
        self.append_to_table(table_name, df)
        return table

    def insert_from_file(
        self,
        table_name: str,
        path: str,
        format: Optional[str] = None,
        branch: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Insert data from a file into an existing table.
        
        Args:
            table_name: Name of the table
            path: Path to the file
            format: Optional format (csv, json, parquet, etc.). If None, inferred from extension.
            branch: Optional branch name
            **kwargs: Additional arguments for the specific read function
        """
        import os
        from iceframe import ingest
        
        if format is None:
            _, ext = os.path.splitext(path)
            format = ext.lower().lstrip('.')
            
        if format == 'csv':
            df = ingest.read_csv(path, **kwargs)
        elif format in ['json', 'ndjson']:
            df = ingest.read_json(path, **kwargs)
        elif format == 'parquet':
            df = ingest.read_parquet(path, **kwargs)
        elif format in ['ipc', 'arrow', 'feather']:
            df = ingest.read_ipc(path, **kwargs)
        elif format == 'avro':
            df = ingest.read_avro(path, **kwargs)
        elif format == 'orc':
            df = ingest.read_orc(path, **kwargs)
        elif format in ['xls', 'xlsx', 'excel']:
            df = ingest.read_excel(path, **kwargs)
        elif format == 'delta':
            df = ingest.read_delta(path, **kwargs)
        elif format == 'lance':
            df = ingest.read_lance(path, **kwargs)
        elif format == 'vortex':
            df = ingest.read_vortex(path, **kwargs)
        elif format == 'hudi':
            df = ingest.read_hudi(path, **kwargs)
        elif format == 'xml':
            df = ingest.read_xml(path, **kwargs)
        elif format in ['sas', 'sas7bdat']:
            df = ingest.read_sas(path, **kwargs)
        elif format in ['sav', 'spss']:
            df = ingest.read_spss(path, **kwargs)
        elif format in ['dta', 'stata']:
            df = ingest.read_stata(path, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {format}")
            
        self.append_to_table(table_name, df, branch=branch)

    def query_datafusion(self, sql: str, tables: Optional[list[str]] = None) -> pl.DataFrame:
        """
        Execute a SQL query using Apache DataFusion.
        
        Args:
            sql: SQL query to execute
            tables: List of table names to register before querying. 
                   If None, attempts to parse table names from SQL (basic) or requires manual registration.
                   
        Returns:
            Polars DataFrame result
        """
        from iceframe.datafusion_ops import DataFusionManager
        
        dfm = DataFusionManager(self)
        
        if tables:
            for table in tables:
                dfm.register_table(table)
        
        return dfm.query(sql)
    
    @property
    def distribute(self):
        """
        Access distributed processing capabilities (Ray).
        
        Returns:
            RayExecutor instance
        """
        from iceframe.distributed import RayExecutor
        # Initialize with default args or allow config via properties?
        # For now, default initialization.
        return RayExecutor()

    @property
    def viz(self):
        """
        Access visualization capabilities.
        
        Returns:
            Visualizer instance
        """
        from iceframe.visualization import Visualizer
        return Visualizer(self)

    @property
    def quality(self):
        """
        Access data quality validator.
        
        Returns:
            DataValidator instance
        """
        from iceframe.quality import DataValidator
        return DataValidator(self)

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
        branch: Optional[str] = None,
    ) -> None:
        """
        Append data to an existing Iceberg table.
        
        Args:
            table_name: Name of the table
            data: Data to append (Polars DataFrame, PyArrow Table, or dict)
            branch: Optional branch name to write to (for WAP pattern)
            
        Example:
            >>> df = pl.DataFrame({"id": [1], "name": ["Alice"]})
            >>> ice.append_to_table("my_table", df)
        """
        self._operations.append_to_table(table_name, data, branch=branch)

    def insert_items(self, table_name: str, items: List[BaseModel], branch: Optional[str] = None) -> None:
        """
        Insert a list of Pydantic models into a table.
        
        Args:
            table_name: Name of the table
            items: List of Pydantic model instances
            branch: Optional branch name
        """
        if not items:
            return
            
        # Convert items to dicts
        data = [item.model_dump() for item in items]
        
        # Create DataFrame
        df = pl.DataFrame(data)
        
        self.append_to_table(table_name, df, branch=branch)
    
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
        from iceframe.gc import GarbageCollector
        from datetime import datetime, timedelta
        
        table = self.get_table(table_name)
        gc = GarbageCollector(table)
        
        older_than_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
        gc.expire_snapshots(older_than_ms=older_than_ms, retain_last=retain_last)
    
    def remove_orphan_files(self, table_name: str, older_than_days: int = 3) -> None:
        """
        Remove orphaned data files from a table.
        
        Args:
            table_name: Name of the table
            older_than_days: Remove files older than this many days
            
        Example:
            >>> ice.remove_orphan_files("my_table", older_than_days=7)
        """
        from iceframe.gc import GarbageCollector
        from datetime import datetime, timedelta
        
        table = self.get_table(table_name)
        gc = GarbageCollector(table)
        
        older_than_ms = int(
            (datetime.now() - timedelta(days=older_than_days)).timestamp() * 1000
        )
        
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
            
        Example:
            >>> ice.compact_data_files("my_table", target_file_size_mb=256)
        """
        from iceframe.compaction import CompactionManager
        
        table = self.get_table(table_name)
        compactor = CompactionManager(table)
        
        compactor.bin_pack(target_file_size_mb=target_file_size_mb)
    
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

    # Advanced Features
    
    def create_view(self, view_name: str, sql: str, replace: bool = False) -> Any:
        """Create a view"""
        from iceframe.views import ViewManager
        manager = ViewManager(self.catalog)
        return manager.create_view(view_name, sql, replace=replace)
        
    def drop_view(self, view_name: str) -> None:
        """Drop a view"""
        from iceframe.views import ViewManager
        manager = ViewManager(self.catalog)
        manager.drop_view(view_name)
        
    def call_procedure(self, table_name: str, procedure_name: str, **kwargs) -> Any:
        """Call a stored procedure on a table"""
        from iceframe.procedures import StoredProcedures
        table = self.get_table(table_name)
        procs = StoredProcedures(table)
        return procs.call(procedure_name, **kwargs)
        
    def evolve_partition(self, table_name: str) -> 'PartitionEvolution':
        """Get partition evolution helper"""
        from iceframe.evolution import PartitionEvolution
        table = self.get_table(table_name)
        return PartitionEvolution(table)

    def register_table(self, table_name: str, metadata_location: str) -> Any:
        """Register an existing table"""
        from iceframe.catalog_ops import CatalogOperations
        ops = CatalogOperations(self.catalog)
        return ops.register_table(table_name, metadata_location)
        
    def add_files(self, table_name: str, file_paths: List[str]) -> None:
        """Add existing data files to table"""
        from iceframe.ingestion import DataIngestion
        table = self.get_table(table_name)
        ingestion = DataIngestion(table)
        ingestion.add_files(file_paths)
        
    def rollback_to_snapshot(self, table_name: str, snapshot_id: int) -> None:
        """Rollback to snapshot"""
        from iceframe.rollback import RollbackManager
        table = self.get_table(table_name)
        rm = RollbackManager(table)
        rm.rollback_to_snapshot(snapshot_id)
        
    def rollback_to_timestamp(self, table_name: str, timestamp_ms: int) -> None:
        """Rollback to timestamp"""
        from iceframe.rollback import RollbackManager
        table = self.get_table(table_name)
        rm = RollbackManager(table)
        rm.rollback_to_timestamp(timestamp_ms)

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
