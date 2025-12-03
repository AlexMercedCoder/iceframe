# Partition Management

IceFrame allows you to manage table partitioning to optimize query performance.

## Accessing Partition Manager

```python
# Get partition manager for a table
partitioner = ice.partition_by("logs")
```

## Adding Partition Fields

IceFrame supports various transforms: `identity`, `bucket`, `truncate`, `year`, `month`, `day`, `hour`.

```python
# Partition by 'category' (identity)
ice.partition_by("logs").add_partition_field("category")

# Partition by day of 'timestamp'
ice.partition_by("logs").add_partition_field("timestamp", "day", name="day_ts")

# Partition by bucket of 'user_id'
ice.partition_by("logs").add_partition_field("user_id", "bucket", 16, name="user_bucket")
```

## Dropping Partition Fields

```python
# Drop partition field
ice.partition_by("logs").drop_partition_field("category")
```
