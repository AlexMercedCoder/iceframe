"""
Async operations for IceFrame.
"""

import asyncio
from typing import Any, Dict, Optional, List
import polars as pl
from pyiceberg.catalog import Catalog

class AsyncIceFrame:
    """
    Async version of IceFrame for non-blocking operations.
    
    Note: This wraps synchronous operations in async executors.
    True async support would require async PyIceberg client.
    """
    
    def __init__(self, catalog_config: Dict[str, Any]):
        """Initialize with catalog configuration"""
        from iceframe.core import IceFrame
        self._ice_frame = IceFrame(catalog_config)
        
    async def read_table_async(
        self,
        table_name: str,
        limit: Optional[int] = None,
        columns: Optional[List[str]] = None
    ) -> pl.DataFrame:
        """
        Read table asynchronously.
        
        Args:
            table_name: Name of the table
            limit: Optional row limit
            columns: Optional column selection
            
        Returns:
            Polars DataFrame
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._ice_frame.read_table(table_name, limit=limit, columns=columns)
        )
        
    async def append_to_table_async(
        self,
        table_name: str,
        data: pl.DataFrame
    ) -> None:
        """
        Append data to table asynchronously.
        
        Args:
            table_name: Name of the table
            data: Polars DataFrame to append
        """
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            lambda: self._ice_frame.append_to_table(table_name, data)
        )
        
    async def query_async(self, table_name: str):
        """
        Get async query builder.
        
        Args:
            table_name: Name of the table
            
        Returns:
            AsyncQueryBuilder instance
        """
        return AsyncQueryBuilder(self._ice_frame, table_name)
        
    async def stats_async(self, table_name: str) -> Dict[str, Any]:
        """
        Get table statistics asynchronously.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with table statistics
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._ice_frame.stats(table_name)
        )


class AsyncQueryBuilder:
    """Async version of QueryBuilder"""
    
    def __init__(self, ice_frame, table_name: str):
        self._ice_frame = ice_frame
        self._query_builder = ice_frame.query(table_name)
        
    def select(self, *exprs):
        """Select columns"""
        self._query_builder.select(*exprs)
        return self
        
    def filter(self, expr):
        """Filter rows"""
        self._query_builder.filter(expr)
        return self
        
    def join(self, other_table: str, on, how: str = "inner"):
        """Join with another table"""
        self._query_builder.join(other_table, on, how)
        return self
        
    async def execute_async(self) -> pl.DataFrame:
        """Execute query asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._query_builder.execute
        )
