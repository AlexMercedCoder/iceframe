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

## Sort Compaction

You can also sort data during compaction (Z-Order approximation) to improve skipping during queries.

```python
# Sort by region and date during compaction
ice.get_table("sales").compaction.sort(
    sort_order=["region", "date"],
    target_file_size_mb=128
)
```
