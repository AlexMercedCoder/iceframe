# Creating Tables

IceFrame makes it easy to create Apache Iceberg tables with various schema formats.

## Basic Usage

```python
ice.create_table("my_table", schema)
```

## Schema Formats

You can define schemas in several ways:

### Dictionary Schema
Simple key-value pairs of column names and types.

```python
schema = {
    "id": "long",
    "name": "string",
    "price": "double",
    "active": "boolean",
    "created_at": "timestamp",
    "birth_date": "date"
}
ice.create_table("products", schema)
```

Supported types: `string`, `int`, `long`, `float`, `double`, `boolean`, `timestamp`, `date`.

### PyArrow Schema
For more control over types and nullability.

```python
import pyarrow as pa

schema = pa.schema([
    pa.field("id", pa.int64(), nullable=False),
    pa.field("name", pa.string()),
    pa.field("tags", pa.list_(pa.string()))
])
ice.create_table("users", schema)
```

### Polars DataFrame
Infer schema from an existing DataFrame.

```python
import polars as pl

df = pl.DataFrame({"id": [1], "name": ["test"]})
ice.create_table("inferred_table", df)
```

## Namespaces

You can specify namespaces (databases/schemas) in the table name:

```python
# Creates table 'sales' in 'marketing' namespace
ice.create_table("marketing.sales", schema)
```

If the namespace doesn't exist, IceFrame will attempt to create it.

## Advanced Options

### Partitioning
Partition data for better query performance.

```python
# Not yet fully exposed in high-level API, use underlying PyIceberg table object
# or pass partition_spec to create_table (requires PyIceberg PartitionSpec object)
```

### Table Properties
Set table properties like compression codec.

```python
properties = {
    "write.parquet.compression-codec": "zstd"
}
ice.create_table("optimized_table", schema, properties=properties)
```
