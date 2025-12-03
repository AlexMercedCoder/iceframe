"""
Monitoring and observability for IceFrame.
"""

import time
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class QueryMetrics:
    """Metrics for a single query execution"""
    query_id: str
    table_name: str
    start_time: float
    end_time: Optional[float] = None
    rows_scanned: int = 0
    rows_returned: int = 0
    bytes_scanned: int = 0
    cache_hit: bool = False
    error: Optional[str] = None
    
    def duration_ms(self) -> float:
        """Get query duration in milliseconds"""
        if self.end_time:
            return (self.end_time - self.start_time) * 1000
        return 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "query_id": self.query_id,
            "table_name": self.table_name,
            "duration_ms": self.duration_ms(),
            "rows_scanned": self.rows_scanned,
            "rows_returned": self.rows_returned,
            "bytes_scanned": self.bytes_scanned,
            "cache_hit": self.cache_hit,
            "error": self.error,
            "timestamp": datetime.fromtimestamp(self.start_time).isoformat()
        }

class MetricsCollector:
    """
    Collect and track query metrics.
    """
    
    def __init__(self):
        self.metrics: Dict[str, QueryMetrics] = {}
        self._query_counter = 0
        
    def start_query(self, table_name: str) -> str:
        """
        Start tracking a query.
        
        Returns:
            Query ID
        """
        self._query_counter += 1
        query_id = f"query_{self._query_counter}_{int(time.time())}"
        
        self.metrics[query_id] = QueryMetrics(
            query_id=query_id,
            table_name=table_name,
            start_time=time.time()
        )
        
        return query_id
        
    def end_query(
        self,
        query_id: str,
        rows_returned: int = 0,
        cache_hit: bool = False,
        error: Optional[str] = None
    ):
        """
        End query tracking.
        
        Args:
            query_id: Query ID from start_query
            rows_returned: Number of rows returned
            cache_hit: Whether result came from cache
            error: Error message if query failed
        """
        if query_id in self.metrics:
            metric = self.metrics[query_id]
            metric.end_time = time.time()
            metric.rows_returned = rows_returned
            metric.cache_hit = cache_hit
            metric.error = error
            
    def get_stats(self) -> Dict[str, Any]:
        """Get aggregate statistics"""
        if not self.metrics:
            return {"total_queries": 0}
            
        durations = [m.duration_ms() for m in self.metrics.values() if m.end_time]
        cache_hits = sum(1 for m in self.metrics.values() if m.cache_hit)
        errors = sum(1 for m in self.metrics.values() if m.error)
        
        return {
            "total_queries": len(self.metrics),
            "avg_duration_ms": sum(durations) / len(durations) if durations else 0,
            "cache_hit_rate": cache_hits / len(self.metrics) if self.metrics else 0,
            "error_rate": errors / len(self.metrics) if self.metrics else 0,
            "total_rows_returned": sum(m.rows_returned for m in self.metrics.values())
        }
        
    def get_recent_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent query metrics"""
        sorted_metrics = sorted(
            self.metrics.values(),
            key=lambda m: m.start_time,
            reverse=True
        )
        return [m.to_dict() for m in sorted_metrics[:limit]]
