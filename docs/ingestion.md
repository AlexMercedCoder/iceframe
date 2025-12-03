# Data Ingestion & Bulk Import

IceFrame supports efficient bulk import of existing data files into Iceberg tables without rewriting them.

## Adding Files

If you have existing Parquet, Avro, or ORC files (e.g., from a migration or another job), you can register them directly into the table. This is a metadata-only operation and is extremely fast.

```python
files = [
    "s3://bucket/data/file1.parquet",
    "s3://bucket/data/file2.parquet"
]

# Add files to the table
ice.add_files("my_table", files)
```

### Requirements
*   Files must match the table's schema.
*   Files must be in a location accessible by the catalog/engine.
*   If the table is partitioned, files should ideally align with partitions, though Iceberg can handle unpartitioned files in partitioned tables (they will be scanned to determine partition values if metrics are available).

> [!TIP]
> Use this for migrating large datasets from legacy systems or other table formats.
