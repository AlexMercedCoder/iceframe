"""
Connection pooling for IceFrame catalog connections.
"""

from typing import Dict, Any, Optional
from queue import Queue, Empty
import threading
import time

class CatalogPool:
    """
    Simple connection pool for catalog instances.
    """
    
    def __init__(self, catalog_config: Dict[str, Any], pool_size: int = 5, timeout: int = 30):
        """
        Initialize connection pool.
        
        Args:
            catalog_config: Catalog configuration
            pool_size: Number of connections in pool
            timeout: Connection timeout in seconds
        """
        self.catalog_config = catalog_config
        self.pool_size = pool_size
        self.timeout = timeout
        self._pool = Queue(maxsize=pool_size)
        self._lock = threading.Lock()
        self._initialized = False
        
    def _create_connection(self):
        """Create a new catalog connection"""
        from pyiceberg.catalog import load_catalog
        return load_catalog("pooled", **self.catalog_config)
        
    def _initialize_pool(self):
        """Initialize the connection pool"""
        with self._lock:
            if not self._initialized:
                for _ in range(self.pool_size):
                    self._pool.put(self._create_connection())
                self._initialized = True
                
    def get_connection(self):
        """
        Get a connection from the pool.
        
        Returns:
            Catalog connection
        """
        if not self._initialized:
            self._initialize_pool()
            
        try:
            return self._pool.get(timeout=self.timeout)
        except Empty:
            # Pool exhausted, create new connection
            return self._create_connection()
            
    def return_connection(self, conn):
        """
        Return a connection to the pool.
        
        Args:
            conn: Catalog connection to return
        """
        try:
            self._pool.put_nowait(conn)
        except:
            # Pool full, discard connection
            pass
            
    def close_all(self):
        """Close all connections in the pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                # Catalog connections don't have explicit close
                del conn
            except Empty:
                break
