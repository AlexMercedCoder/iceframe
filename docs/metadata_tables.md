# Metadata Tables

Added in **0.13.0**. `ice.inspect(table)` exposes Iceberg's metadata tables as
Polars DataFrames, built on PyIceberg's `Table.inspect`.

This replaces hand-parsing `current_snapshot.summary`, whose keys are optional
and populated inconsistently by different engines.

```python
inspector = ice.inspect("db.events")

inspector.snapshots()             # id, parent, timestamp, operation, summary
inspector.files()                 # every data + delete file in a snapshot
inspector.data_files()
inspector.delete_files()
inspector.partitions()            # per-partition record and file counts
inspector.manifests()
inspector.history()               # snapshot history with ancestry
inspector.refs()                  # branches and tags
inspector.entries()               # raw manifest entries
inspector.metadata_log_entries()
```

All of these return `pl.DataFrame`, so they compose with the rest of Polars:

```python
# Which partitions have the most small files?
(
    ice.inspect("db.events")
    .files()
    .filter(pl.col("file_size_in_bytes") < 8 * 1024 * 1024)
    .group_by("partition")
    .len()
    .sort("len", descending=True)
)
```

## Time travel

Several inspectors accept a `snapshot_id`:

```python
ice.inspect("db.events").files(snapshot_id=123456789)
ice.inspect("db.events").partitions(snapshot_id=123456789)
```

## Checking availability

Which metadata tables exist depends on the installed PyIceberg version:

```python
ice.inspect("db.events").available()
# ['snapshots', 'entries', 'refs', 'partitions', 'manifests', ...]
```

Asking for one that isn't available raises `UnsupportedOperationError` listing
what is.

## Row counts without a scan

`ice.count_rows(table)` uses the metadata tables and only falls back to a full
scan if they're unavailable:

```python
ice.count_rows("db.events")   # reads metadata, not data
```

## Relationship to `ice.stats()`

`ice.stats(table)` still returns the summary dict it always did, but its
`total_records`, `total_data_files` and `total_size_bytes` are now derived from
the metadata tables when available, falling back to the snapshot summary
otherwise.

## See also

- [Table Statistics](statistics.md)
- [Table Maintenance](maintenance.md)
