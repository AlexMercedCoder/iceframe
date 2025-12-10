# API Ingestion

IceFrame allows you to ingest data directly from REST APIs.

## Usage

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

config = load_catalog_config_from_env()
ice = IceFrame(config)

# Read from an API
url = "https://api.example.com/users"
ice.create_table_from_api("my_namespace.users", url)

# With a specific key for the list of records
ice.create_table_from_api(
    "my_namespace.users", 
    "https://api.example.com/response", 
    json_key="data"
)

# With authentication headers
ice.create_table_from_api(
    "my_namespace.secure_data",
    "https://api.example.com/secure",
    headers={"Authorization": "Bearer token"}
)
```

## Dependencies

Requires `requests`.

```bash
pip install "iceframe[api]"
```
