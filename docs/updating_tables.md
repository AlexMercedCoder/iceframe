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

Use PyIceberg's native atomic upsert when matched rows can be replaced wholesale:

```python
result = ice.upsert("users", incoming, join_cols=["id"])
```

`QueryBuilder.merge()` supports column-level update rules, but that fallback
materializes and overwrites the full target table. Reserve it for small tables.
For large rule-based merges, use Spark, Trino, Dremio, or another distributed
engine connected to the catalog.
