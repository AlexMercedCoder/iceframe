# MCP Server Integration

IceFrame includes a Model Context Protocol (MCP) server that exposes its capabilities to AI assistants and IDEs (like Claude Desktop, Cursor, etc.) via stdio.

## Installation

Install IceFrame with the `mcp` extra:

```bash
pip install "iceframe[mcp]"
```

## Configuration

The MCP server requires the same environment variables as the IceFrame library to connect to your Iceberg catalog.

- `ICEBERG_CATALOG_URI` (Required)
- `ICEBERG_CATALOG_TYPE` (Default: `rest`)
- `ICEBERG_WAREHOUSE`
- `ICEBERG_TOKEN`
- `ICEBERG_CREDENTIAL`
- `ICEBERG_OAUTH2_SERVER_URI`

## Usage

### Getting Configuration

To get the JSON configuration for your MCP client (e.g., for `claude_desktop_config.json`), run:

```bash
iceframe mcp config
```

This will output a JSON object like:

```json
{
  "mcpServers": {
    "iceframe": {
      "command": "/path/to/python",
      "args": [
        "-m",
        "iceframe.cli",
        "mcp",
        "start"
      ],
      "env": {
        "ICEBERG_CATALOG_URI": "..."
      }
    }
  }
}
```

Copy this configuration into your client's settings file.

### Starting the Server

The server is typically started automatically by the MCP client using the command specified in the configuration. However, you can start it manually for testing:

```bash
iceframe mcp start
```

## Available Tools

The MCP server exposes the following tools to the AI assistant:

- `list_tables(namespace)`: List tables in a namespace.
- `describe_table(table_name)`: Get schema and metadata for a table.
- `get_table_stats(table_name)`: Get table statistics.
- `execute_query(table_name, query, limit)`: Execute a query (filter expression) on a table.
- `generate_code(operation)`: Generate Python code for complex operations.
- `generate_sql(description)`: Generate SQL query templates.
- `list_documentation()`: List available documentation files.
- `read_documentation(page)`: Read the content of a documentation file.
