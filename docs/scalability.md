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

## Catalog lifecycle

A synchronous `IceFrame` owns one catalog handle. The removed `CatalogPool`
opened unused connections and provided no throughput benefit. Async operations
use bounded workers with executor-local handles where a client is thread-bound.

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

Inspect a fluent query before execution:

```python
plan = ice.query("events").select("id").limit(10).explain()
```

The plan identifies pushed predicates/projections/limits and local operations.
Joins and column-rule merges materialize locally; use a distributed engine when
their inputs exceed one host's memory.

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

# Kafka integration (JSON messages)
written = stream_from_kafka(
    ice,
    "kafka-topic",
    "events_table",
    {"bootstrap_servers": "localhost:9092", "group_id": "iceframe-events",
     "auto_offset_reset": "earliest"},
    batch_size=1000,
    flush_interval_seconds=60,   # buffered rows wait at most this long, even if the topic goes quiet
    idle_timeout_seconds=None,   # stop after this long with no messages (None: run until interrupted)
    max_records=None,            # stop after this many records
)
```

`stream_from_kafka` is **at-least-once**. Offsets are committed only after
the records they cover have been appended, and auto-commit is always turned
off, so a crash or a failed append can replay records but never loses them.
Give the consumer a `group_id`, since offsets can only be committed for a
group. If a replay would create duplicates you care about, land the stream in
a staging table and `upsert` into the target on a key.

Before 0.15.0 the consumer auto-committed offsets as messages were read,
so records buffered but not yet written were lost if the process died; the
flush interval was only checked when a new message arrived; and a failed
append was retried during shutdown.

**Install**: `pip install "iceframe[streaming]"` (kafka-python 2.1 or later)

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
