# Rollback & Snapshot Management

IceFrame provides tools to manage table history, allowing you to rollback to previous states or manage branch pointers.

## Rollback

Revert the table state to a specific snapshot or point in time.

```python
# Rollback to a specific snapshot ID
ice.rollback_to_snapshot("my_table", 1234567890)

# Rollback to a timestamp (milliseconds since epoch)
ice.rollback_to_timestamp("my_table", 1704067200000)
```

## Snapshot Management

Explicitly set the current snapshot of the table. This is useful for advanced workflows like cherry-picking or manually moving branch pointers.

```python
# Set the current snapshot of the table
ice.call_procedure("my_table", "set_current_snapshot", snapshot_id=9876543210)
```

> [!WARNING]
> Rolling back changes the current state of the table. Ensure you have the correct snapshot ID or timestamp before proceeding.
