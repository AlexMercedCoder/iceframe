# MCP Server Integration

IceFrame includes a Model Context Protocol (MCP) server that exposes read-only
Iceberg capabilities to compatible AI assistants and IDEs over stdio. IceFrame
0.14 uses the official Python SDK 2.1 series and its `MCPServer` API. The SDK
serves the 2026-07-28 protocol while retaining compatibility with legacy
initialize-era MCP clients.

## Installation

Install IceFrame with the `mcp` extra:

```bash
pip install "iceframe[mcp]"
```

This installs `mcp>=2.1.1,<3`. Python 3.10 or newer is required. Older MCP SDK
releases that expose only `mcp.server.fastmcp.FastMCP` are not supported by
IceFrame 0.14.

## Configuration

The MCP server requires the same environment variables as the IceFrame library to connect to your Iceberg catalog.

- `ICEBERG_CATALOG_URI` (Required)
- `ICEBERG_CATALOG_TYPE` (Default: `rest`)
- `ICEBERG_WAREHOUSE`
- `ICEBERG_TOKEN`
- `ICEBERG_CREDENTIAL`
- `ICEBERG_OAUTH2_SERVER_URI`

The server is read-only by default. `ICEFRAME_MCP_READ_ONLY=0` is reserved for
future mutating tools; none ship today. Query responses are capped by
`ICEFRAME_MCP_MAX_ROWS` (default 1,000) and `ICEFRAME_MCP_MAX_BYTES` (default 5
MiB), measured from serialized JSON.

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

Copy this configuration into your client's settings file. Clients should
launch the command as a stdio server; no HTTP port is opened.

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
- `get_schema(table_name)`: Get structured columns, partitions, sorting, and a
  row-count estimate for query planning.
- `execute_query(table_name, query, limit)`: Execute a query (filter expression) on a table.
- `generate_code(operation)`: Generate Python code for complex operations.
- `generate_sql(description)`: Generate SQL query templates.
- `list_documentation()`: List available documentation files.
- `read_documentation(page)`: Read the content of a documentation file.

Documentation pages are restricted to packaged Markdown files below the
IceFrame docs directory; absolute paths and `..` traversal are rejected.
