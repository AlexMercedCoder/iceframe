# Incremental Processing

IceFrame supports incremental reads and change data capture (CDC) to efficiently process only new or changed data.

## Reading Incremental Data

Read only data added since a specific snapshot:

```python
# Get current snapshot ID
table = ice.get_table("logs")
snapshot_id = table.current_snapshot().snapshot_id

# ... time passes, more data is added ...

# Read only new data
new_data = ice.read_incremental(
    "logs",
    since_snapshot_id=snapshot_id
)
```

Or use a timestamp:

```python
import time

timestamp_ms = int(time.time() * 1000)
# ... later ...
new_data = ice.read_incremental(
    "logs",
    since_timestamp=timestamp_ms
)
```

## Change Data Capture (CDC)

Track changes between two snapshots:

```python
changes = ice.get_changes(
    "users",
    from_snapshot_id=snapshot1,
    to_snapshot_id=snapshot2
)

print(f"Added: {changes['added'].height} rows")
print(f"Deleted: {changes['deleted'].height} rows")
```
