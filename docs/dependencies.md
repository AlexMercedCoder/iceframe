# Dependencies

IceFrame is designed to be lightweight with a core set of dependencies and optional extras for specific features.

## Core Dependencies

- `pyiceberg`: Core Iceberg client
- `polars`: High-performance DataFrame library
- `pyarrow`: Apache Arrow support
- `python-dotenv`: Environment variable management

## Optional Dependencies

### CLI (`[cli]`)

Required for the command-line interface.

- `typer`: CLI application builder
- `rich`: Terminal formatting

Install with:
```bash
pip install "iceframe[cli]"
```

### Cloud Storage (`[aws]`, `[gcs]`, `[azure]`)

Required for accessing cloud storage backends.

- `s3fs`: AWS S3 support
- `gcsfs`: Google Cloud Storage support
- `adlfs`: Azure Data Lake Storage support

Install with:
```bash
pip install "iceframe[aws]"
# or
pip install "iceframe[aws,gcs]"
```

## Async Support

Async operations use Python's built-in `asyncio` library (no additional dependencies required).

```python
from iceframe.async_ops import AsyncIceFrame
```
