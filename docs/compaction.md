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
-   **Filtering**: Optionally compact only specific partitions/files (programmatic API).

## Sort Compaction

You can also sort data during compaction (Z-Order approximation) to improve skipping during queries.

```python
# Sort by region and date during compaction
ice.get_table("sales").compaction.sort(
    sort_order=["region", "date"],
    target_file_size_mb=128
)
```
