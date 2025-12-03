# Updating Tables

IceFrame supports appending to and overwriting Iceberg tables.

## Appending Data

Add new rows to an existing table.

```python
import polars as pl

new_data = pl.DataFrame({
    "id": [3, 4],
    "name": ["Charlie", "David"]
})

ice.append_to_table("users", new_data)
```

You can also append using PyArrow Tables or Python dictionaries:

```python
data_dict = {
    "id": [5],
    "name": ["Eve"]
}
ice.append_to_table("users", data_dict)
```

> [!IMPORTANT]
> Ensure data types match the table schema exactly. For example, use `int32` for `int` columns and `int64` for `long` columns.

## Overwriting Data

Replace all data in the table with new data.

```python
# Replaces entire table content
ice.overwrite_table("daily_report", today_data)
```

## Upserts / Merge

Currently, IceFrame supports Append and Overwrite. Full Merge/Upsert functionality (Merge-on-Read) depends on the underlying PyIceberg support for your specific catalog and table version (v2).

For basic updates, you can:
1. Read the table
2. Modify the DataFrame locally
3. Overwrite the table (for small tables)

For large tables, use SQL-based engines (like Spark, Trino, or Dremio) connected to the same catalog for complex merge operations.
