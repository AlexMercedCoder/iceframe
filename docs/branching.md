# Branching and Tagging

IceFrame supports table branching and snapshot tagging (requires PyIceberg 0.6.0+ and catalog support).

## Creating Branches

```python
# Create branch from current snapshot
ice.create_branch("users", "experiment")

# Create branch from specific snapshot
ice.create_branch("users", "stable", snapshot_id=12345)
```

## Tagging Snapshots

```python
# Tag a snapshot for reference
table = ice.get_table("users")
snapshot_id = table.current_snapshot().snapshot_id

ice.tag_snapshot("users", snapshot_id, "v1.0")
```

## Use Cases

- **Experimentation**: Create branches for testing schema changes
- **Rollback**: Tag stable snapshots for easy rollback
- **Versioning**: Tag releases for reproducibility
