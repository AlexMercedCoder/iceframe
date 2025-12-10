"""
Memory management for IceFrame.
"""

from typing import Iterator, Optional, List
import polars as pl

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

class MemoryManager:
    """
    Manage memory usage for large table operations.
    """
    
    def __init__(self, max_memory_mb: Optional[int] = None):
        """
        Initialize memory manager.
        
        Args:
            max_memory_mb: Maximum memory to use in MB (None = no limit)
        """
        self.max_memory_mb = max_memory_mb
        
    def get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB"""
        if not PSUTIL_AVAILABLE:
            return 0.0  # Return 0 if psutil not available
        process = psutil.Process()
        return process.memory_info().rss / (1024 * 1024)
        
    def check_memory_limit(self):
        """Check if memory limit is exceeded"""
        if self.max_memory_mb:
            current_mb = self.get_memory_usage_mb()
            if current_mb > self.max_memory_mb:
                raise MemoryError(f"Memory limit exceeded: {current_mb:.2f}MB > {self.max_memory_mb}MB")
                
    def read_table_chunked(
        self,
        ice_frame,
        table_name: str,
        chunk_size: int = 10000,
        columns: Optional[List[str]] = None
    ) -> Iterator[pl.DataFrame]:
        """
        Read table in chunks to manage memory.
        
        Args:
            ice_frame: IceFrame instance
            table_name: Name of the table
            chunk_size: Number of rows per chunk
            columns: Optional column selection
            
        """
        # Use scan_batches for true lazy reading
        # Note: chunk_size is a hint, actual batch size depends on file layout
        batch_reader = ice_frame._operations.scan_batches(
            table_name, 
            columns=columns
        )
        
        for batch in batch_reader:
            self.check_memory_limit()
            yield pl.from_arrow(batch)
