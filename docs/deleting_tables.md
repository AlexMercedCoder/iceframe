# Deleting Tables

IceFrame allows you to delete tables and remove specific rows.

## Dropping Tables

Remove a table from the catalog.

```python
ice.drop_table("temp_table")
```

This removes the table metadata from the catalog. The underlying data files may be retained depending on catalog configuration (GC properties).

## Deleting Rows

Delete rows matching a filter expression.

```python
# Delete all users from 'test' region
ice.delete_from_table("users", "region = 'test'")
```

> [!NOTE]
> Row-level deletion requires Iceberg v2 tables and support from the underlying catalog and PyIceberg version.

## Cleaning Up

After deleting tables or rows, you may want to perform [Maintenance](maintenance.md) to clean up old files.
