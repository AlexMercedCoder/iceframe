# IceFrame (Alpha)

A DataFrame-like library for working with Apache Iceberg tables using REST catalogs with local execution.

IceFrame provides a simple, intuitive API for creating, reading, updating, and deleting Iceberg tables, as well as performing maintenance operations and exporting data.

> **Upgrading from 0.12?** **0.13.0 fixes silent data loss and silently wrong query results.** `compact_data_files(filter_expr=...)` used to replace the *whole* table with the filtered subset; a compound filter whose operand couldn't be pushed down had that operand silently dropped; null rows passed every data-quality constraint; `expire_snapshots` called a PyIceberg API that doesn't exist; and `create_table(sort_order=["col"])` raised `AttributeError`. All are fixed with regression tests. See [`CHANGELOG.md`](CHANGELOG.md) for the full list and behaviour changes.

## Features

- **DataFrame API**: Familiar interface for working with tables — `read_table`, `to_arrow`, `to_pandas`, `lazy`, `head`, `describe`, `count_rows`, `scan_batches`
- **Local Execution**: Uses PyIceberg, PyArrow, and Polars for efficient local processing
- **Catalog Support**: REST catalogs (Dremio, Tabular, Polaris, …) with credential vending, plus PyIceberg's `sql` (SQLite/Postgres), `memory`, `glue`, `hive`, and `dynamodb` catalogs by pass-through
- **CRUD Operations**: Create, Read, Update, Delete tables and data
- **Native upserts and transactions**: `ice.upsert(...)` on PyIceberg's atomic `Table.upsert`, and `with ice.transaction(tbl) as txn:` for multi-operation atomic commits
- **Predicate, projection and limit pushdown**: the query builder pushes `WHERE`, `SELECT` and `LIMIT` into the Iceberg scan when it is sound to do so, and re-applies anything it cannot push locally so results stay correct
- **Metadata tables**: `ice.inspect(tbl).snapshots() / .files() / .partitions() / .manifests() / .history() / .refs()` as Polars frames
- **Maintenance**: Expire snapshots, remove orphan files (dry-run by default), compact data files (bin-pack, sort, approximate z-order)
- **Data quality**: constraint validation where **nulls fail by default**, wired into `append_to_table(validators=[...])` as a write gate
- **Typed**: ships `py.typed`, so annotations are visible to downstream type checkers
- **Structured logging**: standard `logging` throughout the library; no `print()` to stdout
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
- [Scalable Updates](docs/scalable_updates.md)

### Table Management
- [Namespace Management](docs/namespaces.md)
- [Schema Evolution](docs/schema_evolution.md)
- [Partition Management](docs/partitioning.md)
- [Branching & Tagging](docs/branching.md)
- [Views](docs/views.md)
- [Catalog Operations](docs/catalog_ops.md)
- [Catalog Support Matrix](docs/catalogs.md)
- [Transactions & Upserts](docs/transactions.md)
- [Metadata Tables](docs/metadata_tables.md)

### Maintenance & Quality
- [Table Maintenance](docs/maintenance.md)
- [Native Maintenance](docs/native_maintenance.md)
- [Safe Compaction](docs/compaction.md)
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
from iceframe import IceFrame, col, lit, load_catalog_config_from_env
import polars as pl

# Initialize
config = load_catalog_config_from_env()
ice = IceFrame(config)

# Create an EMPTY table (note: as of 0.12, create_table never writes data
# even if you pass a DataFrame as the schema — use append_to_table afterwards)
schema = {
    "id": "long",
    "name": "string",
    "created_at": "timestamp",
}
ice.create_table("my_table", schema)

# Append data
data = pl.DataFrame({
    "id": [1, 2],
    "name": ["Alice", "Bob"],
    "created_at": [pl.datetime(2024, 1, 1), pl.datetime(2024, 1, 2)],
})
ice.append_to_table("my_table", data)

# Read data
df = ice.read_table("my_table")
print(df)

# Query Builder API — col, lit, IceFrame, QueryBuilder all importable from
# the package root as of 0.12.
from iceframe.functions import sum as ice_sum

df = (ice.query("my_table")
      .select("name", ice_sum(col("id")).alias("total_id"))
      .group_by("name")
      .execute())
print(df)
```

### Filters: two dialects, named explicitly

`filter_expr=` historically accepted both an IceFrame `Expression` (pushed to
Iceberg) and a string (Polars SQL, evaluated locally) — the same parameter name
meaning two languages with opposite performance. As of 0.13.0 both are named:

```python
# Pushed into the Iceberg scan: only matching files are read.
ice.read_table("my_table", filter=col("age") > 30)

# Evaluated locally after the scan, in Polars SQL.
ice.read_table("my_table", filter_sql="age > 30")
```

`filter_expr=` still works and dispatches on type, but the explicit parameters
are preferred.

### Projection and limit pushdown

```python
# `selected_fields=("id",)` and `limit=10` are pushed into the scan, so a
# billion-row table is not materialised just to trim it.
ice.query("events").select("id").limit(10).execute()
```

Pushdown is skipped when it would change the answer — an `ORDER BY`, a join, an
aggregation, or a predicate that can't be fully pushed all force the limit to be
applied after the fact.

### Native upsert and transactions

```python
# Atomic, incremental MERGE — only files containing matching rows are rewritten.
ice.upsert("users", new_rows, join_cols=["id"])

# Schema change + append in a single commit.
with ice.transaction("events") as txn:
    txn.set_properties({"owner": "data-eng"})
    txn.append(arrow_table)
```

### Metadata tables

```python
ice.inspect("events").snapshots()   # Polars DataFrame
ice.inspect("events").files()
ice.inspect("events").partitions()
```

### Maintenance

```python
# Compaction scoped by a filter rewrites ONLY matching rows. Before 0.13.0 this
# replaced the entire table with the matching subset.
ice.compact_data_files("events", filter_expr="event_date >= '2026-01-01'")

# Snapshot expiry, on PyIceberg's native maintenance API. Branch/tag heads are
# always protected.
ice.expire_snapshots("events", older_than_days=30, retain_last=5)

# Orphan-file cleanup is DRY-RUN BY DEFAULT: it deletes files permanently.
candidates = ice.remove_orphan_files("events", older_than_days=7)
ice.remove_orphan_files("events", older_than_days=7, dry_run=False)
```

### Data quality (nulls fail by default)

```python
from iceframe.quality import DataValidator

# A null `age` FAILS "age > 0". Before 0.13.0 it passed, because Polars'
# three-valued logic filtered null rows out of the violations frame.
DataValidator().validate(df, ["age > 0"])
DataValidator().validate(df, ["age > 0"], null_policy="pass")   # opt out

# As a write gate:
ice.append_to_table("users", df, validators=["age > 0"])   # raises ValidationError
```

### Error handling

Every deliberate IceFrame error derives from `IceFrameError`, so infrastructure
failures are distinguishable from bad input. For backwards compatibility the
input-shaped errors are still `ValueError` subclasses:

```python
from iceframe import IceFrameError, CatalogError, ValidationError

try:
    ice.append_to_table("users", df, validators=["age > 0"])
except ValidationError:
    ...          # your data was bad
except CatalogError:
    ...          # the catalog was unreachable
```

## Development

```bash
pip install -e ".[dev]"
pytest                 # runs offline against a local SQLite catalog
pytest --live          # additionally runs the live REST-catalog tests
ruff check iceframe/ tests/
```

The core suite needs **no credentials and no network**: a session-scoped
fixture builds a PyIceberg `sql` (SQLite) catalog with a `file://` warehouse.
Before 0.13.0 the read, write, query-builder, schema and stats tests only ran
against the author's live Dremio catalog, so 61 of 179 tests skipped everywhere
else.


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
| **Async** | None | `AsyncIceFrame` with its own bounded thread pool |
| **Upsert / MERGE** | `Table.upsert` | `ice.upsert(...)` wrapper + `QueryBuilder.merge` for column-level update rules |
| **Transactions** | `Table.transaction()` | `with ice.transaction(tbl) as txn:` |
| **Metadata tables** | `Table.inspect.*` (Arrow) | `ice.inspect(tbl).*` returning Polars frames |
| **Data Quality** | None | `DataValidator` constraints, null-failing by default, usable as a write gate |
| **Agent surface** | None | Read-only MCP server with row/byte caps, plus a multi-provider LLM agent |

### What IceFrame is not

Being straight about the limits:

- **Merge-on-read delete *writes* are not supported.** PyIceberg 0.11 has no public delete-file writer, so `MoRWriter.write_position_deletes` / `write_equality_deletes` raise `UnsupportedOperationError`. Deletes are copy-on-write. Reading tables that already contain delete files works fine.
- **`QueryBuilder.merge` with column-level update rules reads the whole target table** and overwrites it. Use `ice.upsert(...)` (native, incremental, atomic) whenever "replace matched rows wholesale" is what you want.
- **Joins read each joined table in full.** Only the driving table gets pushdown.
- **Z-order is an approximation** — a hierarchical sort, not a bit-interleaved Z-curve. The returned `strategy` says so.
- **Window functions run locally in Polars**, after the scan.
