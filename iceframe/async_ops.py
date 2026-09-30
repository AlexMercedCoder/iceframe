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
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Union, cast

import polars as pl

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from iceframe.core import IceFrame

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
        catalog_config: Union[dict[str, Any], "IceFrame"],
        max_workers: int = DEFAULT_MAX_WORKERS,
    ):
        from iceframe.core import IceFrame

        self._catalog_config = None
        self._thread_state = threading.local()
        self._closed = False
        if isinstance(catalog_config, IceFrame):
            self._ice_frame = catalog_config
        else:
            # Keep the convenient synchronous handle, but create executor-local
            # clients lazily. SQLite/SQLAlchemy and several catalog clients bind
            # connection state to the creating thread; moving that handle into
            # a worker caused reads to hang indefinitely.
            self._catalog_config = dict(catalog_config)
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
        if self._closed:
            raise RuntimeError("AsyncIceFrame is closed")
        future = self._executor.submit(fn, *args, **kwargs)
        # Some Arrow/Polars scans complete in a worker but fail to wake an
        # asyncio ``run_in_executor`` waiter (reproduced on Python 3.11 with the
        # SQLite catalog). Polling the concurrent future keeps cancellation and
        # the event loop responsive without depending on that callback bridge.
        try:
            while not future.done():
                await asyncio.sleep(0.005)
        except asyncio.CancelledError:
            future.cancel()
            raise
        return future.result()

    def _thread_ice_frame(self):
        if self._catalog_config is None:
            return self._ice_frame
        client = getattr(self._thread_state, "ice_frame", None)
        if client is None:
            from iceframe.core import IceFrame

            client = IceFrame(self._catalog_config)
            self._thread_state.ice_frame = client
        return client

    async def _run_method(self, method_name, *args, **kwargs):
        return await self._run(
            lambda: getattr(self._thread_ice_frame(), method_name)(*args, **kwargs)
        )

    def close(self) -> None:
        """Shut the thread pool down."""
        if not self._closed:
            self._closed = True
            self._executor.shutdown(wait=True, cancel_futures=True)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        self.close()
        return False

    async def read_table_async(
        self, table_name: str, limit: int | None = None, columns: list[str] | None = None
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
        return cast(
            pl.DataFrame,
            await self._run_method("read_table", table_name, limit=limit, columns=columns),
        )

    async def append_to_table_async(self, table_name: str, data: pl.DataFrame) -> None:
        """
        Append data to table asynchronously.

        Args:
            table_name: Name of the table
            data: Polars DataFrame to append
        """
        await self._run_method("append_to_table", table_name, data)

    async def query_async(self, table_name: str):
        """
        Get async query builder.

        Args:
            table_name: Name of the table

        Returns:
            AsyncQueryBuilder instance
        """
        return AsyncQueryBuilder(self, table_name)

    async def stats_async(self, table_name: str) -> dict[str, Any]:
        """
        Get table statistics asynchronously.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table statistics
        """
        return cast(dict[str, Any], await self._run_method("stats", table_name))

    # Ergonomic aliases without the redundant `_async` suffix.
    read_table = read_table_async
    append_to_table = append_to_table_async
    query = query_async
    stats = stats_async


class AsyncQueryBuilder:
    """Async version of QueryBuilder"""

    def __init__(self, async_ice_frame: AsyncIceFrame, table_name: str):
        self._async_ice_frame = async_ice_frame
        self._table_name = table_name
        self._steps: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def select(self, *exprs):
        """Select columns"""
        self._steps.append(("select", exprs, {}))
        return self

    def filter(self, expr):
        """Filter rows"""
        self._steps.append(("filter", (expr,), {}))
        return self

    def join(self, other_table: str, on, how: str = "inner"):
        """Join with another table"""
        self._steps.append(("join", (other_table, on), {"how": how}))
        return self

    async def execute_async(self) -> pl.DataFrame:
        """Execute query asynchronously"""

        def execute():
            query = self._async_ice_frame._thread_ice_frame().query(self._table_name)
            for method, args, kwargs in self._steps:
                query = getattr(query, method)(*args, **kwargs)
            return query.execute()

        return cast(pl.DataFrame, await self._async_ice_frame._run(execute))

    #: Ergonomic alias.
    execute = execute_async
