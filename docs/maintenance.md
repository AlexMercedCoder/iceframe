# Table Maintenance

Iceberg tables require periodic maintenance to ensure optimal performance and manage storage costs. IceFrame provides simple methods for common maintenance tasks.

## Expiring Snapshots

Remove old table snapshots to free up space and keep metadata size manageable.

```python
# Remove snapshots older than 7 days, keeping at least the last 1
ice.expire_snapshots("my_table", older_than_days=7, retain_last=1)
```

## Removing Orphan Files

Clean up data files that are no longer referenced by any snapshot (e.g., from failed writes).

```python
# Remove orphan files older than 3 days
ice.remove_orphan_files("my_table", older_than_days=3)
```

## Compacting Data Files

Combine small data files into larger ones to improve read performance (compaction).

```python
# Compact files to target size of 512 MB
ice.compact_data_files("my_table", target_file_size_mb=512)
```

> [!TIP]
> Run compaction regularly on tables with frequent small updates (streaming ingestion).

## Best Practices

1. **Schedule Maintenance**: Run these operations periodically (e.g., daily or weekly) via a scheduler like Airflow.
2. **Order of Operations**:
   1. `expire_snapshots`
   2. `remove_orphan_files`
   3. `compact_data_files`
