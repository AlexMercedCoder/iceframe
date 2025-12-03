"""
Parallel table operations for IceFrame.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Callable
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
        self,
        ice_frame,
        table_names: List[str],
        **read_kwargs
    ) -> Dict[str, pl.DataFrame]:
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
                    results[table_name] = {"error": str(e)}
                    
        return results
        
    def execute_parallel(
        self,
        func: Callable,
        items: List[Any],
        **kwargs
    ) -> List[Any]:
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
                    results.append({"error": str(e)})
                    
        return results
