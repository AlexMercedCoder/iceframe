# Native File Ingestion

IceFrame supports ingesting data from various file formats that are natively supported by the underlying engines (Polars, PyArrow) without requiring additional dependencies.

## Supported Formats

- **CSV** (`.csv`)
- **JSON** (`.json`, `.ndjson`)
- **Parquet** (`.parquet`)
- **IPC / Arrow / Feather** (`.ipc`, `.arrow`, `.feather`)
- **Avro** (`.avro`)
- **ORC** (`.orc`)

## Usage

### Creating a Table from a File

You can create a new Iceberg table directly from a file. The schema is inferred from the file content.

```python
# Create from Parquet
ice.create_table_from_parquet("my_namespace.table_from_parquet", "data.parquet")

# Create from CSV
ice.create_table_from_csv("my_namespace.table_from_csv", "data.csv")

# Create from JSON
ice.create_table_from_json("my_namespace.table_from_json", "data.json")

# Create from ORC
ice.create_table_from_orc("my_namespace.table_from_orc", "data.orc")
```

### Inserting Data from a File

You can insert data from a file into an existing table using the `insert_from_file` method. This method automatically detects the file format based on the extension, or you can specify it explicitly.

```python
# Insert from CSV (format inferred)
ice.insert_from_file("my_table", "new_data.csv")

# Insert from JSON with explicit format
ice.insert_from_file("my_table", "new_data.json", format="json")

# Insert into a specific branch
ice.insert_from_file("my_table", "experiment_data.parquet", branch="experiment")
```

### Supported Arguments

All ingestion methods accept `**kwargs` which are passed directly to the underlying Polars read functions (e.g., `pl.read_csv`, `pl.read_parquet`). Refer to the [Polars documentation](https://pola-rs.github.io/polars/py-polars/html/reference/io.html) for available options.

```python
# Read CSV with specific options
ice.create_table_from_csv(
    "my_table", 
    "data.csv", 
    has_header=True, 
    separator=";"
)
```
