# Scalability Features

IceFrame includes comprehensive scalability features for high-performance data processing.

## Query Result Caching

Cache query results to avoid redundant computation:

```python
from iceframe.cache import QueryCache

# In-memory cache
cache = QueryCache(max_size=100)

# Use with queries
result = ice.query("users").filter(Column("age") > 30).cache(ttl=3600).execute()
```

**Install**: No additional dependencies required

## Parallel Table Operations

Read multiple tables concurrently:

```python
from iceframe.parallel import ParallelExecutor

executor = ParallelExecutor(max_workers=4)
results = executor.read_tables_parallel(ice, ["users", "orders", "products"])
```

**Install**: No additional dependencies required

## Connection Pooling

Reuse catalog connections for better performance:

```python
from iceframe.pool import CatalogPool

pool = CatalogPool(catalog_config, pool_size=5)
conn = pool.get_connection()
# Use connection
pool.return_connection(conn)
```

**Install**: No additional dependencies required

## Memory Management

Process large tables in chunks:

```python
from iceframe.memory import MemoryManager

manager = MemoryManager(max_memory_mb=1000)

# Read in chunks
for chunk in manager.read_table_chunked(ice, "huge_table", chunk_size=10000):
    process(chunk)
```

**Install**: `pip install "iceframe[monitoring]"` (for psutil)

## Query Optimization

Automatic query optimization:

```python
from iceframe.optimizer import QueryOptimizer

optimizer = QueryOptimizer()
analysis = optimizer.analyze_query("users", select_exprs, filter_exprs, group_by_exprs)
print(analysis["suggestions"])
```

**Install**: No additional dependencies required

## Monitoring & Observability

Track query performance:

```python
from iceframe.monitoring import MetricsCollector

collector = MetricsCollector()
query_id = collector.start_query("users")
# Execute query
collector.end_query(query_id, rows_returned=1000)

stats = collector.get_stats()
print(f"Avg duration: {stats['avg_duration_ms']}ms")
```

**Install**: `pip install "iceframe[monitoring]"` (for psutil, prometheus-client)

## Streaming Support

Stream data to Iceberg tables:

```python
from iceframe.streaming import StreamingWriter, stream_from_kafka

# Micro-batch streaming
writer = StreamingWriter(ice, "events", batch_size=1000)
writer.write({"id": 1, "event": "click"})
writer.flush()

# Kafka integration
stream_from_kafka(ice, "kafka-topic", "events_table", kafka_config)
```

**Install**: `pip install "iceframe[streaming]"` (for kafka-python)

## Data Skipping

Skip unnecessary data files using statistics:

```python
from iceframe.skipping import DataSkipper

skipper = DataSkipper()
stats = skipper.get_stats()
print(f"Skip rate: {stats['skip_rate']:.2%}")
```

**Install**: No additional dependencies required

## Catalog Federation

Query across multiple catalogs:

```python
from iceframe.federation import CatalogFederation

federation = CatalogFederation()
federation.add_catalog("prod", prod_config)
federation.add_catalog("dev", dev_config)

# Read from specific catalog
df = federation.read_table("prod", "users")

# Union across catalogs
combined = federation.union_tables([
    ("prod", "users"),
    ("dev", "users")
])
```

**Install**: No additional dependencies required

## Installation

Install all scalability features:

```bash
pip install "iceframe[cache,streaming,monitoring]"
```

Or install individually as needed.
