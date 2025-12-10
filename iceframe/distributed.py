"""
Distributed processing using Ray.
"""

from typing import Any, Callable, List, Dict, Optional
import polars as pl

try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

class RayExecutor:
    """
    Execute tasks in parallel using Ray.
    """
    
    def __init__(self, address: Optional[str] = None, **ray_init_kwargs):
        """
        Initialize Ray executor.
        
        Args:
            address: Ray cluster address (auto-detect if None)
            **ray_init_kwargs: Additional arguments for ray.init()
        """
        if not RAY_AVAILABLE:
            raise ImportError("ray is required. Install with 'pip install iceframe[distributed]'")
            
        if not ray.is_initialized():
            ray.init(address=address, **ray_init_kwargs)
            
    def map(self, func: Callable, items: List[Any], **kwargs) -> List[Any]:
        """
        Apply a function to a list of items in parallel.
        
        Args:
            func: Function to apply
            items: List of items to process
            **kwargs: Additional arguments to pass to func
            
        Returns:
            List of results
        """
        # Define remote function wrapper
        @ray.remote
        def wrapper(item, **kw):
            return func(item, **kw)
            
        # Launch tasks
        futures = [wrapper.remote(item, **kwargs) for item in items]
        
        # Get results
        return ray.get(futures)
        
    def read_tables_parallel(
        self,
        ice_frame_config: Dict[str, Any],
        table_names: List[str],
        **read_kwargs
    ) -> Dict[str, pl.DataFrame]:
        """
        Read multiple tables in parallel using Ray.
        
        Args:
            ice_frame_config: Configuration to re-initialize IceFrame on workers
            table_names: List of table names to read
            **read_kwargs: Arguments for read_table
            
        Returns:
            Dictionary mapping table names to DataFrames
        """
        # We pass config instead of the object because IceFrame might not be picklable 
        # or we want fresh connections on workers.
        
        @ray.remote
        def read_task(config, table, kwargs):
            from iceframe.core import IceFrame
            ice = IceFrame(config)
            return ice.read_table(table, **kwargs)
            
        futures = {
            table: read_task.remote(ice_frame_config, table, read_kwargs)
            for table in table_names
        }
        
        results = {}
        for table, future in futures.items():
            try:
                results[table] = ray.get(future)
            except Exception as e:
                results[table] = None # Or handle error appropriately
                
        return results

    def shutdown(self):
        """Shutdown Ray"""
        if ray.is_initialized():
            ray.shutdown()
