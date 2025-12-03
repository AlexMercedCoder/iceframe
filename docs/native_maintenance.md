# Native Maintenance Operations

IceFrame provides native implementations of critical maintenance operations that work independently of PyIceberg version or catalog support.

## Native Orphan File Removal

Remove data files that are no longer referenced by any snapshot.

```python
from iceframe.gc import GarbageCollector

# Get table
table = ice.get_table("my_table")
gc = GarbageCollector(table)

# Dry run - list orphans without deleting
orphans = gc.remove_orphan_files(dry_run=True)
print(f"Found {len(orphans)} orphaned files")

# Remove orphans older than 3 days
import time
three_days_ago_ms = int((time.time() - 3 * 86400) * 1000)
removed = gc.remove_orphan_files(older_than_ms=three_days_ago_ms)
print(f"Removed {len(removed)} orphaned files")
```

### How It Works

1. **Scan Manifests**: Reads all manifest files from the current snapshot to build a set of referenced data files
2. **List Storage**: Lists all files in the table's data directory
3. **Find Orphans**: Computes the difference (files in storage - files in manifests)
4. **Age Filter**: Optionally filters by file modification time
5. **Delete**: Removes orphaned files (unless `dry_run=True`)

### Use Cases

- **After Failed Writes**: Clean up files from failed write operations
- **After Compaction**: Remove old data files after rewriting
- **Storage Optimization**: Reclaim storage from deleted/replaced files

> [!TIP]
> Always run with `dry_run=True` first to verify which files will be removed.

## Native Snapshot Expiration

Expire old snapshots to reduce metadata size and improve query planning performance.

```python
from iceframe.gc import GarbageCollector
import time

table = ice.get_table("my_table")
gc = GarbageCollector(table)

# Expire snapshots older than 7 days, keeping at least 5
seven_days_ago_ms = int((time.time() - 7 * 86400) * 1000)

try:
    expired = gc.expire_snapshots(
        older_than_ms=seven_days_ago_ms,
        retain_last=5
    )
    print(f"Expired {len(expired)} snapshots")
except NotImplementedError as e:
    print(f"Snapshot expiration not supported: {e}")
```

### How It Works

1. **List Snapshots**: Gets all snapshots from table metadata
2. **Apply Retention**: Filters snapshots based on age and retention count
3. **Expire**: Uses PyIceberg's `expire_snapshots` if available, otherwise raises `NotImplementedError`

> [!NOTE]
> Snapshot expiration requires PyIceberg 0.7.0+ or catalog support. If not available, the operation will raise `NotImplementedError`.

### Use Cases

- **Metadata Optimization**: Reduce table metadata size
- **Query Performance**: Improve query planning by reducing snapshot count
- **Compliance**: Meet data retention policies

## Comparison with PyIceberg

| Operation | PyIceberg | IceFrame Native |
|:----------|:----------|:----------------|
| **Orphan File Removal** | `table.remove_orphan_files()` (v0.7+) | ✅ Works on any version |
| **Snapshot Expiration** | `table.expire_snapshots()` (catalog-dependent) | ⚠️ Wraps PyIceberg, adds retry logic |
| **Dry Run Support** | Limited | ✅ Full support |
| **Age Filtering** | Basic | ✅ Enhanced with file stat checks |

## Best Practices

### 1. Schedule Regular Maintenance

```python
# Weekly orphan file cleanup
gc.remove_orphan_files(older_than_ms=three_days_ago_ms)

# Monthly snapshot expiration
gc.expire_snapshots(older_than_ms=thirty_days_ago_ms, retain_last=10)
```

### 2. Use Dry Run First

```python
# Always verify before deleting
orphans = gc.remove_orphan_files(dry_run=True)
if len(orphans) > 1000:
    print("Warning: Large number of orphans detected!")
    # Investigate before proceeding
```

### 3. Monitor Storage Savings

```python
import os

# Calculate storage before
orphans = gc.remove_orphan_files(dry_run=True)
total_size = sum(os.path.getsize(f) for f in orphans if os.path.exists(f))
print(f"Potential savings: {total_size / 1024**3:.2f} GB")

# Perform cleanup
gc.remove_orphan_files()
```

## Limitations

- **Snapshot Expiration**: Requires PyIceberg 0.7.0+ or catalog support
- **Concurrent Writes**: Orphan detection may miss files from concurrent write operations
- **Distributed Storage**: File listing performance depends on storage system (S3, HDFS, etc.)

## See Also

- [Maintenance Guide](maintenance.md)
- [Garbage Collection](docs/advanced_features.md#garbage-collection)
- [Table Optimization](docs/advanced_features.md#compaction)
