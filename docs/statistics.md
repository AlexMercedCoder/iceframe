# Table Statistics

IceFrame provides comprehensive table statistics and column profiling.

## Table-Level Statistics

Get overall table metadata:

```python
stats = ice.stats("users")

print(f"Total snapshots: {stats['snapshots']['count']}")
print(f"Columns: {stats['schema']['columns']}")
print(f"Total records: {stats['data']['total_records']}")
```

## Column Profiling

Profile individual columns:

```python
# Numeric column
profile = ice.profile_column("users", "age")
print(f"Min: {profile['numeric_stats']['min']}")
print(f"Max: {profile['numeric_stats']['max']}")
print(f"Mean: {profile['numeric_stats']['mean']}")

# String column
profile = ice.profile_column("users", "name")
print(f"Avg length: {profile['string_stats']['avg_length']}")
print(f"Distinct values: {profile['distinct_count']}")
```
