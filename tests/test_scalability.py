"""
Tests for scalability features
"""

import pytest
import polars as pl
from iceframe.cache import QueryCache
from iceframe.parallel import ParallelExecutor
from iceframe.memory import MemoryManager
from iceframe.optimizer import QueryOptimizer
from iceframe.monitoring import MetricsCollector

def test_query_cache():
    """Test query caching"""
    cache = QueryCache(max_size=10)
    
    # Create test data
    df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
    
    # Cache data
    query_params = {"filter": "id > 1"}
    cache.put("test_table", query_params, df, ttl=60)
    
    # Retrieve from cache
    cached_df = cache.get("test_table", query_params)
    assert cached_df is not None
    assert cached_df.height == 3
    
    # Test cache miss
    miss_df = cache.get("test_table", {"filter": "id > 2"})
    assert miss_df is None

def test_parallel_executor(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test parallel table operations"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Create test data
    import datetime
    data = pl.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"],
        "age": [20, 30],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Test parallel read
    executor = ParallelExecutor(max_workers=2)
    results = executor.read_tables_parallel(ice_frame, [test_table_name])
    
    assert test_table_name in results
    assert results[test_table_name].height == 2

def test_memory_manager():
    """Test memory management"""
    manager = MemoryManager(max_memory_mb=1000)
    
    # Test memory usage check
    usage_mb = manager.get_memory_usage_mb()
    if usage_mb == 0.0:
        pytest.skip("psutil not available or returning 0")
    assert usage_mb > 0

def test_query_optimizer():
    """Test query optimization"""
    from iceframe.expressions import Column
    
    optimizer = QueryOptimizer()
    
    # Test column projection
    select_exprs = [Column("id"), Column("name")]
    filter_exprs = [Column("age")]
    group_by_exprs = []
    
    columns = optimizer.optimize_column_projection(select_exprs, filter_exprs, group_by_exprs)
    assert set(columns) == {"id", "name", "age"}

def test_metrics_collector():
    """Test metrics collection"""
    collector = MetricsCollector()
    
    # Start query
    query_id = collector.start_query("test_table")
    assert query_id in collector.metrics
    
    # End query
    collector.end_query(query_id, rows_returned=100)
    
    # Get stats
    stats = collector.get_stats()
    assert stats["total_queries"] == 1
    assert stats["total_rows_returned"] == 100
