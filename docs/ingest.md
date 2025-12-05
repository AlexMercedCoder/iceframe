# Data Ingestion

IceFrame supports creating Iceberg tables directly from various data sources.

## Installation

You can install the required dependencies for specific formats using optional extras:

```bash
# For Delta Lake
pip install "iceframe[delta]"

# For Lance
pip install "iceframe[lance]"

# For Vortex
pip install "iceframe[vortex]"

# For Excel
pip install "iceframe[excel]"

# For Google Sheets
pip install "iceframe[gsheets]"

# For Hudi
pip install "iceframe[hudi]"
```

## Usage

### Delta Lake

Create an Iceberg table from a Delta Lake table.

```python
ice.create_table_from_delta(
    "my_namespace.from_delta",
    "path/to/delta/table",
    version=None # Optional version
)
```

### Lance

Create an Iceberg table from a Lance dataset.

```python
ice.create_table_from_lance(
    "my_namespace.from_lance",
    "path/to/lance/dataset"
)
```

### Vortex

Create an Iceberg table from a Vortex file.

```python
ice.create_table_from_vortex(
    "my_namespace.from_vortex",
    "path/to/vortex/file.vortex"
)
```

### Excel

Create an Iceberg table from an Excel file.

```python
ice.create_table_from_excel(
    "my_namespace.from_excel",
    "path/to/data.xlsx",
    sheet_name="Sheet1"
)
```

### Google Sheets

Create an Iceberg table from a Google Sheet.

```python
ice.create_table_from_gsheets(
    "my_namespace.from_gsheets",
    "https://docs.google.com/spreadsheets/d/...",
    credentials="path/to/credentials.json"
)
```

### Apache Hudi

Create an Iceberg table from a Hudi table.

```python
ice.create_table_from_hudi(
    "my_namespace.from_hudi",
    "path/to/hudi/table"
)
```

### Standard File Formats

IceFrame also supports standard file formats natively supported by Polars.

#### CSV

```python
ice.create_table_from_csv(
    "my_namespace.from_csv",
    "path/to/data.csv",
    has_header=True
)
```

#### JSON

```python
ice.create_table_from_json(
    "my_namespace.from_json",
    "path/to/data.json"
)
```

#### Parquet

```python
ice.create_table_from_parquet(
    "my_namespace.from_parquet",
    "path/to/data.parquet"
)
```

#### IPC / Arrow

```python
ice.create_table_from_ipc(
    "my_namespace.from_ipc",
    "path/to/data.arrow"
)
```

#### Avro

```python
ice.create_table_from_avro(
    "my_namespace.from_avro",
    "path/to/data.avro"
)
```

## How it Works

These methods perform the following steps:
1. Read the data from the source into a Polars DataFrame.
2. Infer the schema from the DataFrame.
3. Create a new Iceberg table with that schema.
4. Append the data to the newly created table.
