# HTML Ingestion

IceFrame allows you to scrape tables from HTML pages.

## Usage

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

config = load_catalog_config_from_env()
ice = IceFrame(config)

# Read tables from a URL
ice.create_table_from_html(
    "my_namespace.wikipedia_data",
    "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_(nominal)"
)

# Match a specific table
ice.create_table_from_html(
    "my_namespace.specific_table",
    "https://example.com/data",
    match="Population"
)
```

## Dependencies

Requires `lxml`, `html5lib`, and `beautifulsoup4`.

```bash
pip install "iceframe[html]"
```
