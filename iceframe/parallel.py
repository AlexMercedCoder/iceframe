"""
Parallel table operations for IceFrame.
"""

from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import polars as pl


class ParallelExecutor:
    """
    Execute table operations in parallel using thread pool.
    """

    def __init__(self, max_workers: int = 4):
        """
        Initialize parallel executor.

        Args:
            max_workers: Maximum number of worker threads
        """
        self.max_workers = max_workers

    def read_tables_parallel(
        self, ice_frame, table_names: list[str], return_exceptions: bool = False, **read_kwargs
    ) -> dict[str, pl.DataFrame]:
        """
        Read multiple tables in parallel.

        Args:
            ice_frame: IceFrame instance
            table_names: List of table names to read
            **read_kwargs: Arguments to pass to read_table

        Returns:
            Dictionary mapping table names to DataFrames
        """
        results = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_table = {
                executor.submit(ice_frame.read_table, table_name, **read_kwargs): table_name
                for table_name in table_names
            }

            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    results[table_name] = future.result()
                except Exception as e:
                    if not return_exceptions:
                        raise
                    results[table_name] = e

        return results

    def execute_parallel(
        self, func: Callable, items: list[Any], return_exceptions: bool = False, **kwargs
    ) -> list[Any]:
        """
        Execute a function in parallel over a list of items.

        Args:
            func: Function to execute
            items: List of items to process
            **kwargs: Additional arguments to pass to func

        Returns:
            List of results
        """
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(func, item, **kwargs) for item in items]

            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    if not return_exceptions:
                        raise
                    results.append(e)

        return results
