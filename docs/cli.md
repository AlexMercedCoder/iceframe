# Command Line Interface (CLI)

IceFrame provides a CLI for managing Iceberg tables directly from the terminal.

## Installation

The CLI requires the `cli` optional dependency:

```bash
pip install "iceframe[cli]"
```

## Configuration

The CLI uses environment variables for configuration. You can set them in your shell or in a `.env` file.

```bash
export ICEBERG_CATALOG_URI="https://catalog.example.com"
export ICEBERG_CATALOG_TOKEN="your_token"
export ICEBERG_WAREHOUSE="s3://bucket/warehouse"
```

## Commands

### List Tables

List tables in a namespace (default is "default").

```bash
iceframe list
iceframe list --namespace marketing
```

### Describe Table

Show table schema and partition spec.

```bash
iceframe describe my_table
```

### Head Table

Show the first N rows of a table.

```bash
iceframe head my_table --n 10
```
