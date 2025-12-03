# IceFrame

A DataFrame-like library for working with Apache Iceberg tables using REST catalogs with local execution.

IceFrame provides a simple, intuitive API for creating, reading, updating, and deleting Iceberg tables, as well as performing maintenance operations and exporting data.

## Features

- **DataFrame API**: Familiar interface for working with tables
- **Local Execution**: Uses PyIceberg, PyArrow, and Polars for efficient local processing
- **Catalog Support**: Works with REST catalogs (including Dremio, Tabular, etc.) and supports credential vending
- **CRUD Operations**: Create, Read, Update, Delete tables and data
- **Maintenance**: Expire snapshots, remove orphan files, compact data files
- **Export**: Export data to Parquet, CSV, and JSON

## Installation

```bash
pip install iceframe
```

For cloud storage support:

```bash
pip install "iceframe[aws]"   # AWS S3
pip install "iceframe[gcs]"   # Google Cloud Storage
pip install "iceframe[azure]" # Azure Data Lake Storage
```

## Quick Start

1. Create a `.env` file with your catalog credentials (see `.env.example`):

```env
ICEBERG_CATALOG_URI=https://catalog.dremio.cloud/api/iceberg
ICEBERG_TOKEN=your_token
ICEBERG_WAREHOUSE=your_warehouse
ICEBERG_CATALOG_TYPE=rest
```

2. Use IceFrame in your code:

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env
import polars as pl

# Initialize
config = load_catalog_config_from_env()
ice = IceFrame(config)

# Create a table
schema = {
    "id": "long",
    "name": "string",
    "created_at": "timestamp"
}
ice.create_table("my_table", schema)

# Append data
data = pl.DataFrame({
    "id": [1, 2],
    "name": ["Alice", "Bob"],
    "created_at": [pl.datetime(2024, 1, 1), pl.datetime(2024, 1, 2)]
})
ice.append_to_table("my_table", data)

# Read data
df = ice.read_table("my_table")
print(df)

# Query Builder API
from iceframe.expressions import col
from iceframe.functions import sum

df = (ice.query("my_table")
      .select("name", sum(col("id")).alias("total_id"))
      .group_by("name")
      .execute())
print(df)
```

## Documentation

- [Architecture](architecture.md)
- [Creating Tables](docs/creating_tables.md)
- [Reading Tables](docs/reading_tables.md)
- [Updating Tables](docs/updating_tables.md)
- [Deleting Tables](docs/deleting_tables.md)
- [Query Builder API](docs/query_builder.md)
- [Namespace Management](docs/namespaces.md)
- [Schema Evolution](docs/schema_evolution.md)
- [Partition Management](docs/partitioning.md)
- [Data Quality](docs/data_quality.md)
- [Table Maintenance](docs/maintenance.md)
- [Exporting Data](docs/export.md)
- [CLI Usage](docs/cli.md)
- [Dependencies](docs/dependencies.md)

### Advanced Features
- [Incremental Processing](docs/incremental.md)
- [Table Statistics](docs/statistics.md)
- [JOIN Support](docs/joins.md)
- [Branching & Tagging](docs/branching.md)
- [Async Operations](docs/async.md)
