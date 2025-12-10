# IceFrame (Alpha)

A DataFrame-like library for working with Apache Iceberg tables using REST catalogs with local execution.

IceFrame provides a simple, intuitive API for creating, reading, updating, and deleting Iceberg tables, as well as performing maintenance operations and exporting data.

## Features

- **DataFrame API**: Familiar interface for working with tables
- **Local Execution**: Uses PyIceberg, PyArrow, and Polars for efficient local processing
- **Catalog Support**: Works with REST catalogs (including Dremio, Tabular, etc.) and supports credential vending
- **CRUD Operations**: Create, Read, Update, Delete tables and data
- **Maintenance**: Expire snapshots, remove orphan files, compact data files
- **Export**: Export data to Parquet, CSV, and JSON

## Documentation

### Getting Started
- [Creating Tables](docs/creating_tables.md)
- [Reading Tables](docs/reading_tables.md)
- [Updating Tables](docs/updating_tables.md)
- [Deleting Tables](docs/deleting_tables.md)
- [CLI Usage](docs/cli.md)
- [Dependencies](docs/dependencies.md)
- [Environment Variables](docs/variables.md)

### Data Ingestion
- [Native File Ingestion](docs/ingest_native.md) (CSV, JSON, Parquet, ORC, Avro)
- [Optional File Ingestion](docs/ingest_optional.md) (Excel, Delta, Google Sheets)
- [Advanced File Ingestion](docs/ingest_advanced.md) (SQL, XML, SAS/SPSS)
- [API Ingestion](docs/ingest_api.md)
- [HuggingFace Ingestion](docs/ingest_huggingface.md)
- [HTML Ingestion](docs/ingest_html.md)
- [Clipboard Ingestion](docs/ingest_clipboard.md)
- [Folder Ingestion](docs/ingest_folder.md)
- [Bulk Ingestion](docs/ingestion.md)
- [Incremental Ingestion](docs/recipes/incremental_ingestion.md)

### Querying & Processing
- [Query Builder API](docs/query_builder.md)
- [SQL Support (DataFusion)](docs/datafusion.md)
- [Lazy Reading](docs/lazy_reading.md)
- [Distributed Processing (Ray)](docs/distributed.md)
- [Async Operations](docs/async.md)
- [Notebook Integration](docs/notebooks.md)

### Table Management
- [Namespace Management](docs/namespaces.md)
- [Schema Evolution](docs/schema_evolution.md)
- [Partition Management](docs/partitioning.md)
- [Branching & Tagging](docs/branching.md)
- [Views](docs/views.md) (if exists, or remove)
- [Catalog Operations](docs/catalog_ops.md)

### Maintenance & Quality
- [Table Maintenance](docs/maintenance.md)
- [Native Maintenance](docs/native_maintenance.md)
- [Streaming Auto-Compaction](docs/streaming_compaction.md)
- [Data Quality](docs/data_quality.md)
- [Enhanced Data Quality](docs/data_quality_enhanced.md)
- [Rollback & History](docs/rollback.md)

### Advanced Features
- [Visualization](docs/visualization.md)
- [Incremental Processing](docs/incremental.md)
- [Table Statistics](docs/statistics.md)
- [Scalability Overview](docs/scalability.md)
- [AI Agent](docs/ai_agent.md)
- [MCP Server](docs/mcp.md)
- [Pydantic Integration](docs/pydantic.md)

### Recipes
- [ETL Pipeline](docs/recipes/etl_pipeline.md)
- [SCD Type 2](docs/recipes/scd_type_2.md)
- [Data Quality Gate](docs/recipes/data_quality_gate.md)

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


## Feature Comparison: IceFrame vs PyIceberg

IceFrame builds on top of PyIceberg, adding high-level abstractions and missing features.

| Feature | PyIceberg (Native) | IceFrame (Enhanced) |
| :--- | :--- | :--- |
| **Table CRUD** | Low-level API | Simplified `create_table`, `drop_table` |
| **Data Writing** | Arrow/Pandas integration | Polars integration, Auto-schema inference |
| **Branching** | Basic support (WIP) | `create_branch`, `fast_forward`, WAP Pattern |
| **Compaction** | `rewrite_data_files` (limited) | `bin_pack`, `sort` strategies (Polars-based) |
| **Views** | Catalog-dependent | Unified `ViewManager` abstraction |
| **Maintenance** | `expire_snapshots` | `GarbageCollector`, **Native** `remove_orphan_files` |
| **SQL Support** | None | Fluent Query Builder (`select`, `filter`, `join`) |
| **Ingestion** | `add_files` | `add_files` wrapper + Incremental Ingestion recipes |
| **Rollback** | `manage_snapshots` | `rollback_to_snapshot`, `rollback_to_timestamp` |
| **Async** | None | `AsyncIceFrame` for non-blocking I/O |
