# Lazy Reading

IceFrame supports true lazy reading of Iceberg tables, allowing you to process datasets larger than memory by streaming data in batches.

## Usage

Use the `read_table_chunked` method from the memory manager (or via `ice.memory`) to iterate over the table in chunks.

```python
from iceframe.memory import MemoryManager

# Initialize memory manager with a limit (optional)
mem = MemoryManager(max_memory_mb=1024) # 1GB limit

# Iterate over the table
for chunk in mem.read_table_chunked(ice, "my_huge_table", chunk_size=10_000):
    # Process chunk (Polars DataFrame)
    print(chunk.head())
    
    # Perform aggregations, write to another table, etc.
```

## How it Works

Unlike standard reading, batch reading streams Arrow batches from storage and
re-batches them to the requested `chunk_size`. The separate `ice.lazy()` helper
is only a lazy Polars plan over an already materialized Iceberg scan; use
`scan_batches()` or `read_table_chunked()` for datasets larger than memory.

## Memory Safety

The `MemoryManager` can be configured with a `max_memory_mb` limit. It checks memory usage before yielding each chunk and raises a `MemoryError` if the limit is exceeded, helping prevent OOM crashes.
