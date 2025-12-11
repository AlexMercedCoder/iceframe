# Scalable Updates

IceFrame provides an optimized strategies for updating Iceberg tables, particularly benefiting large, partitioned datasets.

## Partition-Pruned Updates

Standard `UPDATE` operations in many Iceberg clients involve rewriting the entire table (Copy-on-Write) or complex Merge-on-Read operations. IceFrame implements a "Smart Copy-on-Write" strategy that minimizes I/O by only rewriting affected partitions.

### How it Works

1.  **Identification**: The query builder scans the table using your `filter` to identify which partitions contain matching rows.
2.  **Pruning**: It reads *only* the data files belonging to those partitions into memory.
3.  **Update**: The update logic (`with_columns`, `when/then`) is applied in memory.
4.  **Overwrite**: Only the affected partitions are overwritten in the Iceberg table using an `overwrite_filter`.

### Usage

```python
# Update status for US region only
# This will only read/rewrite the 'region=US' partition(s)
ice.query("sales") \
   .filter(col("region") == "US") \
   .filter(col("amount") > 1000) \
   .update({"status": "high_value"})
```

### Performance Benefits

-   **Unpartitioned Table**: Full table rewrite (Standard CoW).
-   **Partitioned Table**: Proportional to the size of affected partitions. If you update 1 day of data in a 10-year dataset partitioned by day, it is ~3650x faster.

## Best Practices

-   **Partitioning**: Ensure your table is partitioned by columns frequently used in update filters (e.g., `date`, `region`).
-   **Filter Specificity**: Include partition columns in your filter whenever possible to help the pruner.
