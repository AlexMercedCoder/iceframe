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

When enabled, the writer tracks the number of flushes (writes to Iceberg). Once the threshold `every_n_flushes` is reached, it triggers a `bin_pack` compaction job on the table using `iceframe.compaction`. This consolidates the small data files into larger, more efficient files.

## Requirements

This feature relies on the `iceframe.compaction` module. Ensure your environment supports compaction (which typically requires Spark or a compatible engine, though IceFrame's native compaction uses PyIceberg's rewrite_data_files when available).
