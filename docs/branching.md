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

## Fast-Forwarding (Publishing)

You can fast-forward a branch (e.g., `main`) to another branch (e.g., `audit_branch`). This is essential for the Write-Audit-Publish (WAP) pattern.

```python
from iceframe.branching import BranchManager

# Initialize manager
table = ice.get_table("users")
manager = BranchManager(table)

# Fast-forward main to audit_branch
manager.fast_forward("main", "audit_branch")
```

## Write-Audit-Publish (WAP) Pattern

IceFrame supports the WAP pattern to ensure data quality:

1.  **Write**: Write data to a branch (e.g., `audit_branch`).
2.  **Audit**: Validate data in the branch.
3.  **Publish**: Fast-forward `main` to the branch.

```python
# 1. Write to branch
ice.append_to_table("users", new_data, branch="audit_branch")

# 2. Audit (Validate)
# ... run checks ...

# 3. Publish
manager.fast_forward("main", "audit_branch")
```
