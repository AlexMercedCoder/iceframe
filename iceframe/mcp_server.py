"""
MCP Server for IceFrame.

Exposes IceFrame capabilities as an MCP server over stdio.

**Safety model.** Every tool here is read-only. The server holds one catalog
connection for its lifetime (it used to build a fresh ``IceFrame`` — and
therefore a fresh auth handshake — on *every* tool call), and query results are
capped in both rows and bytes so a model can't pull a billion-row table into
its context.

Set ``ICEFRAME_MCP_READ_ONLY=0`` to allow future mutating tools; it defaults to
``1`` (read-only) and no mutating tool ships today.
"""

import logging
import os
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from iceframe.core import IceFrame
from iceframe.exceptions import ValidationError

logger = logging.getLogger(__name__)

# Initialize FastMCP server
mcp = FastMCP("iceframe-mcp")

#: Hard caps on what a single tool call can return. Without these an agent can
#: exhaust its context (and the server's memory) with one query.
MAX_ROWS = int(os.environ.get("ICEFRAME_MCP_MAX_ROWS", "1000"))
MAX_BYTES = int(os.environ.get("ICEFRAME_MCP_MAX_BYTES", str(5 * 1024 * 1024)))

#: Cached connection; see the module docstring.
_ICEFRAME: Optional[IceFrame] = None


def is_read_only() -> bool:
    """Whether the server refuses mutating operations (default: yes)."""
    return os.environ.get("ICEFRAME_MCP_READ_ONLY", "1").lower() not in ("0", "false", "no")


def require_write_access(operation: str) -> None:
    """Raise unless the server was explicitly started in read-write mode."""
    if is_read_only():
        raise ValidationError(
            f"{operation!r} is a mutating operation and this MCP server is running "
            "read-only. Set ICEFRAME_MCP_READ_ONLY=0 to allow writes."
        )


def reset_iceframe() -> None:
    """Drop the cached connection (used by tests and after config changes)."""
    global _ICEFRAME
    _ICEFRAME = None


def get_iceframe() -> IceFrame:
    """
    Return the shared IceFrame, connecting on first use.

    Reconnecting per tool call meant a full catalog auth handshake for every
    single request an agent made.
    """
    global _ICEFRAME
    if _ICEFRAME is not None:
        return _ICEFRAME

    catalog_config = {
        "uri": os.environ.get("ICEBERG_CATALOG_URI"),
        "type": os.environ.get("ICEBERG_CATALOG_TYPE", "rest"),
        "warehouse": os.environ.get("ICEBERG_WAREHOUSE"),
        "token": os.environ.get("ICEBERG_TOKEN"),
        "credential": os.environ.get("ICEBERG_CREDENTIAL"),
        "oauth2-server-uri": os.environ.get("ICEBERG_OAUTH2_SERVER_URI"),
    }

    # Filter out None values
    catalog_config = {k: v for k, v in catalog_config.items() if v is not None}

    if "uri" not in catalog_config:
        raise ValidationError("ICEBERG_CATALOG_URI environment variable is required")

    _ICEFRAME = IceFrame(catalog_config)
    return _ICEFRAME

@mcp.tool()
def list_tables(namespace: str = "default") -> List[str]:
    """
    List all tables in a namespace.

    Args:
        namespace: Namespace to list tables from (default: 'default')
    """
    ice = get_iceframe()
    return ice.list_tables(namespace)

@mcp.tool()
def describe_table(table_name: str) -> Dict[str, Any]:
    """
    Get schema and metadata for a table.

    Args:
        table_name: Name of the table to describe
    """
    ice = get_iceframe()
    table = ice.get_table(table_name)
    schema = table.schema()
    return {
        "columns": [
            {
                "name": f.name,
                "type": str(f.field_type),
                "required": f.required
            }
            for f in schema.fields
        ],
        "partition_spec": str(table.spec()),
        "properties": table.properties
    }

@mcp.tool()
def get_table_stats(table_name: str) -> Dict[str, Any]:
    """
    Get statistics for a table.

    Args:
        table_name: Name of the table
    """
    ice = get_iceframe()
    return ice.stats(table_name)

@mcp.tool()
def get_schema(table_name: str) -> Dict[str, Any]:
    """
    Get a structured schema for query planning: column names, types,
    nullability, partition columns and sort columns.

    Use this before execute_query so filters and projections reference real
    columns instead of guesses.

    Args:
        table_name: Name of the table
    """
    ice = get_iceframe()
    table = ice.get_table(table_name)
    schema = table.schema()
    spec = table.spec()
    order = table.sort_order()

    partition_cols = [schema.find_field(f.source_id).name for f in spec.fields]
    sort_cols = [schema.find_field(f.source_id).name for f in (order.fields if order else [])]

    return {
        "table": table_name,
        "columns": [
            {
                "name": f.name,
                "type": str(f.field_type),
                "nullable": not f.required,
                "is_partition_column": f.name in partition_cols,
            }
            for f in schema.fields
        ],
        "partition_columns": partition_cols,
        "sort_columns": sort_cols,
        "row_count_estimate": ice.count_rows(table_name),
    }


@mcp.tool()
def execute_query(
    table_name: str,
    query: Optional[str] = None,
    limit: int = 10,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Execute a read-only query on a table and return results.

    Results are capped at ICEFRAME_MCP_MAX_ROWS rows and ICEFRAME_MCP_MAX_BYTES
    bytes; the response says so when truncation happened.

    Args:
        table_name: Name of the table to query
        query: Optional filter expression (e.g., 'age > 30')
        limit: Maximum number of rows to return
        columns: Optional column projection
    """
    ice = get_iceframe()

    effective_limit = max(1, min(int(limit), MAX_ROWS))
    df = ice.read_table(
        table_name, filter_expr=query, limit=effective_limit, columns=columns
    )

    truncated_rows = effective_limit < limit
    truncated_bytes = False

    # Byte cap: shrink until the payload fits.
    while df.height > 1 and df.estimated_size() > MAX_BYTES:
        df = df.head(max(1, df.height // 2))
        truncated_bytes = True

    return {
        "rows": df.height,
        "columns": df.columns,
        "data": df.to_dicts(),
        "truncated": truncated_rows or truncated_bytes,
        "limits": {"max_rows": MAX_ROWS, "max_bytes": MAX_BYTES},
        "read_only": is_read_only(),
    }

@mcp.tool()
def generate_code(operation: str) -> str:
    """
    Generate Python code for a complex operation.

    Args:
        operation: Description of the operation to generate code for
    """
    return f"""# Generated code for: {operation}
from iceframe import IceFrame
import os

config = {{
    "uri": os.environ.get("ICEBERG_CATALOG_URI"),
    "type": "rest",
    # Add other config...
}}

ice = IceFrame(config)

# TODO: Implement {operation}
"""

@mcp.tool()
def generate_sql(description: str) -> str:
    """
    Generate a SQL query template based on a description.

    Args:
        description: Description of the query to generate
    """
    return f"""-- Generated SQL for: {description}
-- TODO: Refine this query based on your specific table schema
SELECT *
FROM my_table
WHERE ...
-- Add filters and aggregations as needed
"""

@mcp.tool()
def list_documentation() -> List[str]:
    """
    List available documentation files.
    """
    # Try to find docs folder relative to package or CWD
    possible_paths = [
        os.path.join(os.getcwd(), "docs"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")
    ]

    docs_path = None
    for path in possible_paths:
        if os.path.exists(path) and os.path.isdir(path):
            docs_path = path
            break

    if not docs_path:
        return ["Error: Documentation directory not found."]

    files = []
    for f in os.listdir(docs_path):
        if f.endswith(".md"):
            files.append(f)

    return sorted(files)

@mcp.tool()
def read_documentation(page: str) -> str:
    """
    Read the content of a documentation file.

    Args:
        page: Name of the documentation file (e.g., 'ingest.md')
    """
    # Try to find docs folder
    possible_paths = [
        os.path.join(os.getcwd(), "docs"),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "docs")
    ]

    docs_path = None
    for path in possible_paths:
        if os.path.exists(path) and os.path.isdir(path):
            docs_path = path
            break

    if not docs_path:
        return "Error: Documentation directory not found."

    file_path = os.path.join(docs_path, page)

    if not os.path.exists(file_path):
        return f"Error: File '{page}' not found in documentation."

    try:
        with open(file_path) as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {e}"

def start():
    """Start the MCP server."""
    mcp.run()
