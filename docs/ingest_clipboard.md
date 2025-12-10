# Clipboard Ingestion

IceFrame allows you to ingest data directly from your system clipboard. This is useful for ad-hoc analysis when copying data from Excel, Google Sheets, or other applications.

## Usage

1. Copy data to your clipboard (e.g., select cells in Excel and press Ctrl+C).
2. Run the following code:

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

config = load_catalog_config_from_env()
ice = IceFrame(config)

# Create table from clipboard content
ice.create_table_from_clipboard("my_namespace.clipboard_data")
```

## Dependencies

Requires `pyperclip` (and `pandas`).

```bash
pip install "iceframe[clipboard]"
```
