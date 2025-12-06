"""
MCP Server for IceFrame.

Exposes IceFrame capabilities as an MCP server over stdio.
"""

import os
import json
from typing import List, Dict, Any, Optional
from mcp.server.fastmcp import FastMCP
from iceframe.core import IceFrame

# Initialize FastMCP server
mcp = FastMCP("iceframe-mcp")

def get_iceframe() -> IceFrame:
    """Initialize IceFrame from environment variables."""
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
        raise ValueError("ICEBERG_CATALOG_URI environment variable is required")
        
    return IceFrame(catalog_config)

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
def execute_query(table_name: str, query: Optional[str] = None, limit: int = 10) -> Dict[str, Any]:
    """
    Execute a query on a table and return results.
    
    Args:
        table_name: Name of the table to query
        query: Optional filter expression (e.g., 'age > 30')
        limit: Maximum number of rows to return
    """
    ice = get_iceframe()
    df = ice.read_table(table_name, filter_expr=query, limit=limit)
    return {
        "rows": df.height,
        "columns": df.columns,
        "data": df.to_dicts()
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
        with open(file_path, "r") as f:
            return f.read()
    except Exception as e:
        return f"Error reading file: {e}"

def start():
    """Start the MCP server."""
    mcp.run()
