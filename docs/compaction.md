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
-   **Scoped rewrites**: `filter_expr` / `partition_filter` restrict the rewrite to matching rows *and* scope the `overwrite_filter` to the same predicate.
-   **Honours `target_file_size_mb`**: sets Iceberg's `write.target-file-size-bytes` for the rewrite.

> **Fixed in 0.13.0 — silent data loss.** Before 0.13.0 the unpartitioned
> rewrite path called `table.overwrite(arrow_table)` with **no**
> `overwrite_filter`. `overwrite` defaults to `AlwaysTrue`, so a compaction
> scoped by `filter_expr` replaced the **entire table** with the matching
> subset — a 6-row table compacted with `"v > 30"` was left with 3 rows, and
> the call reported success. `z_order_optimize` had the same shape. Both are
> fixed and covered by regression tests. Upgrade before using scoped
> compaction.

### Filters must be fully pushable

A compaction filter defines which rows may be *replaced*. A filter that can only
be partially pushed to Iceberg would scope the overwrite to a **superset** of
the intended rows, deleting data outside the scope. IceFrame therefore rejects
such filters with `CompactionError` rather than applying them approximately:

```python
from iceframe import col, CompactionError

# Column-to-column comparisons cannot be pushed to Iceberg.
try:
    ice.compact_data_files("sales", filter_expr=(col("a") > col("b")))
except CompactionError as e:
    print(e)
```

Iceberg predicate strings (`"v > 30"`) and simple `Expression`s are fine.

### Advanced Configuration

You can tune the compaction process to skip partitions that don't need optimization.

```python
# Only compact partitions that have at least 5 files
ice.compact_data_files("sales", target_file_size_mb=128, min_input_files=5)
```

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `target_file_size_mb` | int | 128 | Target output file size in MB. Sets `write.target-file-size-bytes` on the table for the rewrite. (Before 0.13.0 this argument was accepted and completely ignored.) |
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
