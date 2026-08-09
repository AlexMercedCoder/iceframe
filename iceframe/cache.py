"""
Query result caching for IceFrame.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, Optional

import polars as pl

logger = logging.getLogger(__name__)


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

    @staticmethod
    def _stable(obj: Any) -> Any:
        """Coerce values into something json.dumps can hash deterministically."""
        if isinstance(obj, (str, int, float, bool)) or obj is None:
            return obj
        if isinstance(obj, dict):
            return {str(k): QueryCache._stable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [QueryCache._stable(v) for v in obj]
        return repr(obj)

    def _generate_key(self, table_name: str, query_params: Dict[str, Any]) -> str:
        """Generate cache key from query parameters"""
        key_data = {
            "table": table_name,
            "params": self._stable(query_params),
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
            "table": table_name,
            "data": data,
            "created_at": time.time(),
            "expires_at": expires_at,
        }

    def invalidate(self, table_name: str):
        """
        Invalidate cached queries for the given table only.

        Previously ignored the argument and dropped the entire cache — that
        defeated the point of per-table invalidation. We now tag each entry
        with its source table and remove only matching keys.
        """
        keys_to_remove = [k for k, e in self._cache.items() if e.get("table") == table_name]
        for key in keys_to_remove:
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
    Disk-based cache that stores parquet files under ``cache_dir`` and tracks
    them via the ``diskcache`` index.

    Previous version (a) skipped ``super().__init__``, so inherited methods
    like ``invalidate``/``stats`` referenced a missing ``self._cache``; and
    (b) wrote parquet to the OS tempdir via ``NamedTemporaryFile(delete=False)``
    and never deleted those files on eviction/overwrite/expiry — an unbounded
    disk leak that ignored ``cache_dir`` entirely.
    """

    def __init__(self, cache_dir: str = ".iceframe_cache", max_size_gb: float = 1.0):
        super().__init__(max_size=10**9)  # disk capacity bound is enforced by diskcache
        try:
            import diskcache  # noqa: F401
        except ImportError:
            raise ImportError(
                "diskcache required for disk caching. Install with: pip install 'iceframe[cache]'"
            ) from None
        import os
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir = os.path.abspath(cache_dir)
        # Parquet payloads live under <cache_dir>/data/<key>.parquet; the
        # diskcache index in <cache_dir>/index/ holds metadata pointers.
        self._data_dir = os.path.join(self.cache_dir, "data")
        os.makedirs(self._data_dir, exist_ok=True)
        import diskcache
        self.cache = diskcache.Cache(
            os.path.join(self.cache_dir, "index"),
            size_limit=int(max_size_gb * 1024**3),
        )

    def _payload_path(self, key: str) -> str:
        import os
        return os.path.join(self._data_dir, f"{key}.parquet")

    def _delete_payload(self, key: str) -> None:
        import os
        path = self._payload_path(key)
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Could not delete cache payload %s: %s", path, e)

    def get(self, table_name: str, query_params: Dict[str, Any]) -> Optional[pl.DataFrame]:
        key = self._generate_key(table_name, query_params)
        entry = self.cache.get(key)
        if entry is None:
            return None
        if entry.get("expires_at") and time.time() > entry["expires_at"]:
            self.cache.pop(key, None)
            self._delete_payload(key)
            return None
        return pl.read_parquet(entry["data_path"])

    def put(self, table_name: str, query_params: Dict[str, Any], data: pl.DataFrame, ttl: Optional[int] = None):
        key = self._generate_key(table_name, query_params)
        # If a previous payload existed at this key, delete it before overwrite.
        old = self.cache.get(key)
        if old is not None:
            self._delete_payload(key)

        data_path = self._payload_path(key)
        data.write_parquet(data_path)

        expires_at = time.time() + ttl if ttl else None
        self.cache.set(key, {
            "table": table_name,
            "data_path": data_path,
            "created_at": time.time(),
            "expires_at": expires_at,
        })

    def invalidate(self, table_name: str):
        for key in list(self.cache):
            entry = self.cache.get(key)
            if entry is not None and entry.get("table") == table_name:
                self.cache.pop(key, None)
                self._delete_payload(key)

    def clear(self):
        for key in list(self.cache):
            self._delete_payload(key)
        self.cache.clear()

    def stats(self) -> Dict[str, Any]:
        return {
            "size": len(self.cache),
            "cache_dir": self.cache_dir,
        }


# --------------------------------------------------------------------------
# Process-wide query cache registry.
#
# Lives here rather than in query.py so that write paths in operations.py can
# invalidate it without importing query.py (which imports operations.py).
# --------------------------------------------------------------------------

_QUERY_CACHE: QueryCache = QueryCache(max_size=128)


def set_query_cache(cache: QueryCache) -> None:
    """Replace the process-wide query cache (e.g. install a :class:`DiskCache`)."""
    global _QUERY_CACHE
    _QUERY_CACHE = cache


def get_query_cache() -> QueryCache:
    """Return the process-wide query cache used by ``QueryBuilder.cache(ttl)``."""
    return _QUERY_CACHE


def invalidate_query_cache(table_name: str) -> None:
    """
    Drop every cached query result for ``table_name``.

    Called from every IceFrame write path (append / overwrite / delete /
    update / merge / upsert / compaction). Without it, a cached read taken
    before a write kept serving pre-write rows for the life of the process.
    """
    try:
        _QUERY_CACHE.invalidate(table_name)
    except Exception:  # pragma: no cover - a broken cache must not fail writes
        logger.warning("Failed to invalidate query cache for %s", table_name, exc_info=True)
