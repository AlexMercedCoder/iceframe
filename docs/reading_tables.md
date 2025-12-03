# Reading Tables

IceFrame provides a simple API to read Iceberg tables into Polars DataFrames.

## Basic Reading

```python
df = ice.read_table("my_table")
```

## Column Selection

Read only specific columns to improve performance.

```python
df = ice.read_table("users", columns=["id", "email"])
```

## Filtering

Filter data at the source (predicate pushdown).

```python
# Filter expression using SQL-like syntax
df = ice.read_table("sales", filter_expr="amount > 100 AND region = 'US'")
```

## Limiting Results

Limit the number of rows returned.

```python
df = ice.read_table("logs", limit=100)
```

## Time Travel

Read the table as it existed at a specific point in time.

### By Snapshot ID

```python
df = ice.read_table("my_table", snapshot_id=123456789012345)
```

### By Timestamp

```python
# Read as of 1 hour ago
timestamp_ms = int((time.time() - 3600) * 1000)
df = ice.read_table("my_table", as_of_timestamp=timestamp_ms)
```

## Accessing Underlying Table

For advanced operations, you can access the underlying PyIceberg Table object.

```python
table = ice.get_table("my_table")
# Use PyIceberg API directly
scan = table.scan()
```
