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

### Pydantic (`[pydantic]`)

Required for Pydantic integration.

- `pydantic>=2.0.0`: Data validation and settings management

Install with:
```bash
pip install "iceframe[pydantic]"
```

### Ingestion (`[ingestion]`)

Required for various data ingestion and format support.

- `pdf`: PDF generation (`fpdf2`, `markdown-it-py`)
- `delta`: Delta Lake support (`deltalake`)
- `lance`: Lance support (`pylance`)
- `vortex`: Vortex support (`vortex-data`)
- `excel`: Excel support (`fastexcel`)
- `gsheets`: Google Sheets support (`gspread`)
- `hudi`: Hudi support (`getdaft`)
- `sql`: SQL Database support (`connectorx`, `sqlalchemy`)
- `xml`: XML support (`lxml`)
- `stats`: Statistical file support (`pyreadstat`)

Install with:
```bash
pip install "iceframe[ingestion]"
```

Or individually:
```bash
pip install "iceframe[sql]"
pip install "iceframe[xml]"
pip install "iceframe[stats]"
```

### MCP Server (`[mcp]`)

Required for running the Model Context Protocol server.

- `mcp`: MCP server library

Install with:
```bash
pip install "iceframe[mcp]"
```

### Notebook (`[notebook]`)

Required for Jupyter Notebook integration.

- `ipython>=8.0.0`: Interactive computing
- `ipywidgets>=8.0.0`: Interactive HTML widgets

Install with:
```bash
pip install "iceframe[notebook]"
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

## AI Agent

The AI agent requires LLM provider packages. Install with:

```bash
pip install "iceframe[agent]"
```

This includes:
- `openai>=1.0.0` - For GPT models
- `anthropic>=0.18.0` - For Claude models  
- `google-generativeai>=0.3.0` - For Gemini models
- `rich>=13.0.0` - For CLI formatting

You only need the API key for the LLM provider you want to use:

```bash
# Choose one:
export OPENAI_API_KEY="your-key"
export ANTHROPIC_API_KEY="your-key"
export GOOGLE_API_KEY="your-key"
```

### Scalability Features

```bash
pip install "iceframe[cache,streaming,monitoring]"
```

### SQL Support (`[datafusion]`)

Required for high-performance SQL execution.

- `datafusion>=35.0.0`: Apache DataFusion

```bash
pip install "iceframe[datafusion]"
```

### Distributed Processing (`[distributed]`)

Required for distributed execution.

- `ray>=2.0.0`: Ray

```bash
pip install "iceframe[distributed]"
```

### Visualization (`[viz]`)

Required for generating charts.

- `altair>=5.0.0`: Declarative statistical visualization

```bash
pip install "iceframe[viz]"
```
