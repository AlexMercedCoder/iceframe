# Safe Compaction

IceFrame provides robust utilities for compacting small files (bin-packing) to improve read performance, with built-in safety mechanisms for large tables.

## The Small Files Problem

Frequent updates and inserts can lead to many small files, degrading query performance. Compaction rewrites these into fewer, larger files.

## Safe Bin-Packing

IceFrame's `compact_data_files` (or `bin_pack`) uses a partition-aware strategy to manage memory usage. Instead of reading the entire table, it processes one partition at a time.

### Usage

```python
# Compact the table, targeting 128MB files
ice.compact_data_files("sales", target_file_size_mb=128)
```

### Features

-   **Partition-by-Partition**: Reads and rewrites one partition at a time to prevent OOM errors on large tables.
-   **Smart Partition Skipping**: Analyzing partition stats (file count) to avoid compacting healthy partitions unnecessarily.
-   **Filtering**: Optionally compact only specific partitions/files (programmatic API).

### Advanced Configuration

You can tune the compaction process to skip partitions that don't need optimization.

```python
# Only compact partitions that have at least 5 files
ice.compact_data_files("sales", target_file_size_mb=128, min_input_files=5)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `target_file_size_mb` | int | 128 | Target output file size in MB. |
| `min_input_files` | int | 1 | Minimum number of files in a partition to trigger compaction. Partitions with fewer files are skipped. |
| `partition_filter` | dict | None | Dictionary of col=value to only compact specific partitions. Example: `{"region": "us"}` |
| `deduplicate` | bool | False | If True, drops duplicate rows within the compacted partition. |
| `max_workers` | int | 1 | Number of threads to process partitions in parallel. **Note**: May cause commit conflicts on high concurrency. |
| `dry_run` | bool | False | If True, performs analysis and returns planned stats without writing data. |
| `retries` | int | 3 | Number of times to retry a compaction commit if it fails due to conflict. |
| `compression` | str | None | Compression codec to apply (e.g., "zstd", "snappy", "gzip"). Optimizes storage. |

### Targeted Compaction Example

```python
# 1. Target specific partition
ice.compact_data_files("sales", partition_filter={"date": "2024-01-01"})

# 2. Remove duplicates while compacting
ice.compact_data_files("sales", deduplicate=True)

# 3. Parallel Compaction (Experimental)
ice.compact_data_files("sales", max_workers=4)

# 4. Dry Run (Estimate work)
stats = ice.compact_data_files("sales", dry_run=True)
print(stats)
```

## Bloom Filters
Enable Bloom Filters on high-cardinality columns (like IDs) to drastically speed up point lookups (`id = 123`).

```python
# Configure bloom filters
# fpp (False Positive Probability): Probability that a filter mistakenly claims data exists in a file.
# Lower fpp = Larger filter size, fewer false positives. Default is 0.01 (1%).
ice.configure_bloom_filters("sales", columns=["id"], fpp=0.01)
```

## Sort Order Preservation
`bin_pack` automatically detects the table's sort order (if defined via `create_table(..., sort_order=...)`) and applies it during compaction.

## Z-Order Clustering (Approximate)
Optimize data layout for multi-column queries using hierarchical sorting (approximation of Z-Order).

```python
# Cluster data by 'region' and 'date'
ice.z_order_optimize("sales", columns=["region", "date"])
```

## Sort Compaction

You can also sort data during compaction (Z-Order approximation) to improve skipping during queries.

```python
# Sort by region and date during compaction
ice.get_table("sales").compaction.sort(
    sort_order=["region", "date"],
    target_file_size_mb=128
)
```
