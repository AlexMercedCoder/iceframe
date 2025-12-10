# HuggingFace Ingestion

IceFrame allows you to ingest datasets directly from the HuggingFace Hub.

## Usage

```python
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env

config = load_catalog_config_from_env()
ice = IceFrame(config)

# Read a dataset
ice.create_table_from_huggingface(
    "my_namespace.imdb", 
    "imdb", 
    split="train"
)

# With specific configuration
ice.create_table_from_huggingface(
    "my_namespace.glue_mrpc",
    "glue",
    name="mrpc",
    split="validation"
)
```

## Dependencies

Requires `datasets`.

```bash
pip install "iceframe[hf]"
```
