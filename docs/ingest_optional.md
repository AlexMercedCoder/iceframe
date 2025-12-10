# Optional File Ingestion

IceFrame supports additional file formats through optional dependencies. These formats require installing specific extras.

## Supported Formats

| Format | Extension | Install Command |
| :--- | :--- | :--- |
| **Excel** | `.xlsx`, `.xls` | `pip install "iceframe[excel]"` |
| **Delta Lake** | (Directory) | `pip install "iceframe[delta]"` |
| **Lance** | `.lance` | `pip install "iceframe[lance]"` |
| **Vortex** | `.vortex` | `pip install "iceframe[vortex]"` |
| **Google Sheets** | (URL) | `pip install "iceframe[gsheets]"` |
| **Hudi** | (Directory) | `pip install "iceframe[hudi]"` |

## Usage

### Excel

```python
# Create table from Excel
ice.create_table_from_excel(
    "my_table", 
    "data.xlsx", 
    sheet_name="Sales_2024"
)
```

### Delta Lake

```python
# Create table from Delta Lake
ice.create_table_from_delta(
    "my_table", 
    "/path/to/delta/table", 
    version=1
)
```

### Google Sheets

```python
# Create table from Google Sheets
ice.create_table_from_gsheets(
    "my_table", 
    "https://docs.google.com/spreadsheets/d/...",
    credentials="path/to/service_account.json"
)
```

### Generic Insert

You can also use `insert_from_file` with these formats, provided the dependencies are installed.

```python
ice.insert_from_file("my_table", "data.xlsx", format="excel")
ice.insert_from_file("my_table", "/path/to/delta", format="delta")
```
