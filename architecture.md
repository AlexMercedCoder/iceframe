# IceFrame

## Directory Structure

IceFrame is a high-level Python library designed to simplify interactions with Apache Iceberg tables by providing a DataFrame-centric API. It bridges the gap between the low-level `pyiceberg` client and the user-friendly experience of `polars` and `pandas`.

## Core Components

### 1. IceFrame (Core Class)
The main entry point (`iceframe.core.IceFrame`). It initializes the connection to the Iceberg catalog and exposes all functionality through a unified API.
- **Responsibility**: Configuration, catalog management, facade for all operations.

### 2. Table Operations
Handles CRUD operations (`iceframe.operations`).
- **Create**: Supports creating tables from PyArrow schemas, Polars DataFrames, or dicts.
- **Read**: Scans Iceberg tables, applies filters (pushdown), and converts to Polars DataFrames.
- **Write**: Appends or overwrites data using PyIceberg's write support.

### 3. Query Builder
A fluent API for constructing complex queries (`iceframe.query`).
- **Expression System**: Unified expression builder (`iceframe.expressions`) that translates to:
    - **PyIceberg Expressions**: For predicate pushdown to the scan level.
    - **Polars Expressions**: For local processing (aggregations, window functions).
- **Execution Engine**: Orchestrates the scan and post-processing.

### 4. Feature Modules
Modular components for specific capabilities:
- **Namespace Management** (`iceframe.namespace`): Manage schemas/databases.
- **Schema Evolution** (`iceframe.schema`): Add/drop/rename/update columns.
- **Partition Management** (`iceframe.partition`): Manage partition specs.
- **Data Quality** (`iceframe.quality`): Validate data before/after writes.
- **Garbage Collection** (`iceframe.gc`): Snapshot expiration, native orphan file removal.
- **Compaction** (`iceframe.compaction`): Bin-packing and sorting strategies.
- **Export** (`iceframe.export`): Export data to Parquet, CSV, JSON.
- **Incremental Processing** (`iceframe.incremental`): Read only new data, CDC.
- **Table Statistics** (`iceframe.stats`): Metadata and column profiling.
- **Branching** (`iceframe.branching`): Create branches and tag snapshots.
- **Views** (`iceframe.views`): Cross-engine view management.
- **Evolution** (`iceframe.evolution`): Partition spec evolution.
- **Procedures** (`iceframe.procedures`): Stored procedure interface.
- **Rollback** (`iceframe.rollback`): Snapshot rollback and management.
- **Catalog Ops** (`iceframe.catalog_ops`): Catalog-level operations.
- **Async Operations** (`iceframe.async_ops`): Non-blocking operations.
- **AI Agent** (`iceframe.agent`): Natural language interface with LLM integration.
- **Pydantic Integration** (`iceframe.pydantic`): Schema conversion and data validation.
- **Notebook Magics** (`iceframe/magics.py`): IPython magic commands (`%iceframe`, `%%iceql`).
- **Bulk Ingestion** (`iceframe/ingestion.py`): Add existing files to tables.
- **Format Ingestion** (`iceframe/ingest.py`): Read Delta, Lance, Vortex, Excel, etc.

### 5. Scalability Features
- **Query Caching** (`iceframe.cache`): In-memory and disk-based result caching
- **Parallel Operations** (`iceframe.parallel`): Concurrent table operations
- **Connection Pooling** (`iceframe.pool`): Catalog connection pooling
- **Memory Management** (`iceframe.memory`): Chunked reading and memory limits
- **Query Optimization** (`iceframe.optimizer`): Automatic query optimization
- **Monitoring** (`iceframe.monitoring`): Query metrics and observability
- **Streaming** (`iceframe.streaming`): Micro-batch and Kafka streaming
- **Data Skipping** (`iceframe.skipping`): File-level filtering
- **Federation** (`iceframe.federation`): Multi-catalog support

### 6. CLI
A command-line interface (`iceframe.cli`) built with `typer` for quick table inspection and management.

### 6. AI Chat
An interactive AI assistant (`iceframe-chat`) for natural language interaction with Iceberg tables.

## Data Flow

1.  **User Interaction**: User calls `ice.read_table()` or `ice.query()`.
2.  **Catalog Interaction**: `IceFrame` uses `pyiceberg` to load the table metadata.
3.  **Predicate Pushdown**: Filters are translated to PyIceberg expressions and passed to `table.scan()`.
4.  **Data Retrieval**: `pyiceberg` reads data files (Parquet/Avro) matching the filter and returns a PyArrow Table.
5.  **Local Processing**: PyArrow Table is converted to a Polars DataFrame. Additional operations (aggregations, complex filters, joins) are applied locally.
6.  **Result**: A Polars DataFrame is returned to the user.

## Design Principles

- **DataFrame-First**: All data input/output is handled via Polars DataFrames (or PyArrow/Pandas where compatible).
- **Pushdown Optimization**: Maximize predicate pushdown to minimize data transfer.
- **Modularity**: Features are isolated in separate modules to maintain clean code and testability.
- **Developer Experience**: Fluent APIs, type hinting, and comprehensive documentation.
- **Async Support**: Non-blocking operations for high-concurrency scenarios.
