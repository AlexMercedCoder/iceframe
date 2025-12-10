# Folder Ingestion

IceFrame allows you to ingest multiple files from a folder into a single table.

## Usage

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

config = load_catalog_config_from_env()
ice = IceFrame(config)

# Read all CSVs from a folder
ice.create_table_from_folder(
    "my_namespace.daily_logs",
    "/path/to/logs",
    pattern="*.csv"
)

# Read all Parquet files
ice.create_table_from_folder(
    "my_namespace.archive",
    "/path/to/archive",
    pattern="*.parquet"
)
```

## Supported Formats

- CSV
- JSON
- Parquet
- Excel

The format is inferred from the file extension.
