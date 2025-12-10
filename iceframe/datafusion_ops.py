"""
DataFusion integration for IceFrame.
"""

from typing import Optional, Any
import polars as pl
import pyarrow as pa

try:
    import datafusion
    DATAFUSION_AVAILABLE = True
except ImportError:
    DATAFUSION_AVAILABLE = False

class DataFusionManager:
    """
    Manage DataFusion context and execution.
    """
    
    def __init__(self, ice_frame):
        """
        Initialize DataFusion manager.
        
        Args:
            ice_frame: IceFrame instance
        """
        if not DATAFUSION_AVAILABLE:
            raise ImportError("datafusion is required. Install with 'pip install iceframe[datafusion]'")
            
        self.ice_frame = ice_frame
        self.ctx = datafusion.SessionContext()
        
    def register_table(self, table_name: str, alias: Optional[str] = None):
        """
        Register an Iceberg table with DataFusion.
        
        Args:
            table_name: Name of the Iceberg table
            alias: Optional alias for the table in SQL
        """
        # For now, we'll scan the table to Arrow and register it.
        # Ideally, we'd use a native Iceberg provider for DataFusion if available,
        # but registering the Arrow dataset is a good start.
        # Using scan_batches to get a RecordBatchReader is efficient.
        
        batch_reader = self.ice_frame._operations.scan_batches(table_name)
        # DataFusion can register a RecordBatchReader or PyArrow Table
        # Converting to Table first is safer for now as batch reader support varies
        table = pa.Table.from_batches(batch_reader)
        
        self.ctx.register_table(alias or table_name, table)
        
    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute SQL query using DataFusion.
        
        Args:
            sql: SQL query string
            
        Returns:
            Polars DataFrame result
        """
        df_result = self.ctx.sql(sql)
        # Convert DataFusion DataFrame to PyArrow Table then Polars
        return pl.from_arrow(df_result.to_arrow_table())
