# Catalog Operations

Manage catalog-level operations beyond basic table CRUD.

## Registering Tables

You can register an existing Iceberg table (metadata.json) into the catalog. This is useful for:
*   Migrating tables between catalogs.
*   Recovering tables from storage.
*   Registering tables created by other engines.

```python
# Register a table using its metadata location
metadata_url = "s3://bucket/warehouse/my_table/metadata/v1.metadata.json"

ice.register_table("my_new_table", metadata_url)
```

> [!NOTE]
> Not all catalogs support table registration. Check your catalog's documentation.
