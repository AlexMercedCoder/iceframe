# Streaming Auto-Compaction

IceFrame's `StreamingWriter` can automatically compact small files created during streaming ingestion, preventing the "small file problem" that degrades read performance.

## Usage

Enable auto-compaction on the writer by calling `enable_auto_compaction`.

```python
from iceframe.streaming import StreamingWriter

writer = StreamingWriter(ice, "my_table", batch_size=1000)

# Run compaction (bin-packing) after every 10 flushes
writer.enable_auto_compaction(every_n_flushes=10)

# Write data...
for record in stream:
    writer.write(record)
    
writer.close()
```

## How it Works

When enabled, the writer tracks successful flushes. At the threshold it calls
the public `ice.compact_data_files(table)` facade. A compaction failure is
logged and raised; the preceding append may already be committed, so callers
must handle that partial workflow explicitly.

## Requirements

No Spark process is required. IceFrame reads scoped data locally and commits
rewritten files through PyIceberg, so memory and runtime scale with the affected
partition. Use an external distributed engine for compaction beyond one host's
capacity.
