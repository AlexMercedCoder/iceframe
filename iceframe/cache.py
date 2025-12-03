"""
Query result caching for IceFrame.
"""

import hashlib
import json
import time
from typing import Optional, Dict, Any
import polars as pl

class QueryCache:
    """
    In-memory cache for query results with TTL support.
    """
    
    def __init__(self, max_size: int = 100):
        """
        Initialize cache.
        
        Args:
            max_size: Maximum number of cached queries
        """
        self._cache: Dict[str, Dict[str, Any]] = {}
        self.max_size = max_size
        
    def _generate_key(self, table_name: str, query_params: Dict[str, Any]) -> str:
        """Generate cache key from query parameters"""
        key_data = {
            "table": table_name,
            "params": query_params
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
        
    def get(self, table_name: str, query_params: Dict[str, Any]) -> Optional[pl.DataFrame]:
        """
        Get cached result if available and not expired.
        
        Args:
            table_name: Name of the table
            query_params: Query parameters
            
        Returns:
            Cached DataFrame or None
        """
        key = self._generate_key(table_name, query_params)
        
        if key not in self._cache:
            return None
            
        entry = self._cache[key]
        
        # Check TTL
        if entry["expires_at"] and time.time() > entry["expires_at"]:
            del self._cache[key]
            return None
            
        return entry["data"]
        
    def put(self, table_name: str, query_params: Dict[str, Any], data: pl.DataFrame, ttl: Optional[int] = None):
        """
        Cache query result.
        
        Args:
            table_name: Name of the table
            query_params: Query parameters
            data: DataFrame to cache
            ttl: Time to live in seconds (None = no expiration)
        """
        key = self._generate_key(table_name, query_params)
        
        # Evict oldest if at capacity
        if len(self._cache) >= self.max_size and key not in self._cache:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k]["created_at"])
            del self._cache[oldest_key]
        
        expires_at = time.time() + ttl if ttl else None
        
        self._cache[key] = {
            "data": data,
            "created_at": time.time(),
            "expires_at": expires_at
        }
        
    def invalidate(self, table_name: str):
        """
        Invalidate all cached queries for a table.
        
        Args:
            table_name: Name of the table
        """
        keys_to_remove = []
        for key, entry in self._cache.items():
            # Check if this cache entry is for the specified table
            # We need to reconstruct the table name from the key
            # For simplicity, we'll clear all cache (could be optimized)
            keys_to_remove.append(key)
            
        for key in keys_to_remove:
            if key in self._cache:
                del self._cache[key]
                
    def clear(self):
        """Clear all cached queries"""
        self._cache.clear()
        
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "entries": [
                {
                    "created_at": entry["created_at"],
                    "expires_at": entry["expires_at"],
                    "rows": entry["data"].height
                }
                for entry in self._cache.values()
            ]
        }


class DiskCache(QueryCache):
    """
    Disk-based cache using diskcache library.
    """
    
    def __init__(self, cache_dir: str = ".iceframe_cache", max_size_gb: float = 1.0):
        """
        Initialize disk cache.
        
        Args:
            cache_dir: Directory for cache files
            max_size_gb: Maximum cache size in GB
        """
        try:
            import diskcache
            self.cache = diskcache.Cache(cache_dir, size_limit=int(max_size_gb * 1024**3))
        except ImportError:
            raise ImportError("diskcache required for disk caching. Install with: pip install 'iceframe[cache]'")
            
    def _generate_key(self, table_name: str, query_params: Dict[str, Any]) -> str:
        """Generate cache key"""
        key_data = {
            "table": table_name,
            "params": query_params
        }
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()
        
    def get(self, table_name: str, query_params: Dict[str, Any]) -> Optional[pl.DataFrame]:
        """Get cached result"""
        key = self._generate_key(table_name, query_params)
        
        entry = self.cache.get(key)
        if entry is None:
            return None
            
        # Check TTL
        if entry["expires_at"] and time.time() > entry["expires_at"]:
            del self.cache[key]
            return None
            
        # Deserialize DataFrame
        return pl.read_parquet(entry["data_path"])
        
    def put(self, table_name: str, query_params: Dict[str, Any], data: pl.DataFrame, ttl: Optional[int] = None):
        """Cache query result to disk"""
        import tempfile
        import os
        
        key = self._generate_key(table_name, query_params)
        
        # Write DataFrame to temp parquet file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as f:
            data.write_parquet(f.name)
            data_path = f.name
            
        expires_at = time.time() + ttl if ttl else None
        
        self.cache.set(key, {
            "data_path": data_path,
            "created_at": time.time(),
            "expires_at": expires_at
        })
        
    def clear(self):
        """Clear disk cache"""
        self.cache.clear()
