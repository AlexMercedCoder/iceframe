"""
Async operations for IceFrame.

PyIceberg has no async client, so this module runs synchronous IceFrame calls
on a **bounded** thread pool owned by the wrapper. It used to hand every call
to ``run_in_executor(None, ...)``, i.e. the interpreter-wide default executor,
which has no IceFrame-specific bound and is shared with unrelated code.

The wrapper is honest about what it is: concurrency for I/O-bound catalog and
scan calls, not true async I/O.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Union

import polars as pl

logger = logging.getLogger(__name__)

#: Default worker count for the wrapper's own pool.
DEFAULT_MAX_WORKERS = 8


class AsyncIceFrame:
    """
    Async facade over :class:`~iceframe.core.IceFrame`.

    Args:
        catalog_config: A catalog config dict **or** an existing ``IceFrame``.
        max_workers: Size of the dedicated thread pool.

    Usable as an async context manager so the pool is shut down deterministically::

        async with AsyncIceFrame(config) as ice:
            df = await ice.read_table("db.events")
    """

    def __init__(
        self,
        catalog_config: Union[Dict[str, Any], "IceFrame"],  # noqa: F821
        max_workers: int = DEFAULT_MAX_WORKERS,
    ):
        from iceframe.core import IceFrame

        if isinstance(catalog_config, IceFrame):
            self._ice_frame = catalog_config
        else:
            self._ice_frame = IceFrame(catalog_config)

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="iceframe-async"
        )

    @property
    def ice_frame(self):
        """The wrapped synchronous IceFrame."""
        return self._ice_frame

    async def _run(self, fn, *args, **kwargs):
        """Run ``fn`` on this wrapper's own bounded pool."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, lambda: fn(*args, **kwargs))

    def close(self) -> None:
        """Shut the thread pool down."""
        self._executor.shutdown(wait=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self.close()
        return False

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
        return await self._run(
            self._ice_frame.read_table, table_name, limit=limit, columns=columns
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
        await self._run(self._ice_frame.append_to_table, table_name, data)

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
        return await self._run(self._ice_frame.stats, table_name)


    # Ergonomic aliases without the redundant `_async` suffix.
    read_table = read_table_async
    append_to_table = append_to_table_async
    query = query_async
    stats = stats_async


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
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._query_builder.execute)

    #: Ergonomic alias.
    execute = execute_async
