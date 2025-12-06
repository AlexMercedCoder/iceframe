"""
Command Line Interface for IceFrame.
"""

import os
import typer
from typing import Optional
from rich.console import Console
from rich.table import Table
from dotenv import load_dotenv

from iceframe.core import IceFrame

app = typer.Typer(help="IceFrame CLI - Manage Iceberg tables from the command line.")
console = Console()

def get_ice_frame() -> IceFrame:
    """Initialize IceFrame from environment variables"""
    load_dotenv()
    
    # Check for required env vars
    uri = os.getenv("ICEBERG_CATALOG_URI")
    if not uri:
        console.print("[red]Error: ICEBERG_CATALOG_URI environment variable not set.[/red]")
        raise typer.Exit(code=1)
        
    config = {
        "uri": uri,
        "type": os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
    }
    
    # Add optional config
    if token := os.getenv("ICEBERG_CATALOG_TOKEN"):
        config["token"] = token
    if warehouse := os.getenv("ICEBERG_WAREHOUSE"):
        config["warehouse"] = warehouse
    if oauth_uri := os.getenv("ICEBERG_OAUTH2_SERVER_URI"):
        config["oauth2-server-uri"] = oauth_uri
        
    # Add any other ICEBERG_ header configs
    for key, value in os.environ.items():
        if key.startswith("ICEBERG_HEADER_"):
            header_key = key.replace("ICEBERG_HEADER_", "header.").replace("_", "-")
            config[header_key] = value
            
    try:
        return IceFrame(config)
    except Exception as e:
        console.print(f"[red]Error initializing IceFrame: {e}[/red]")
        raise typer.Exit(code=1)

@app.command()
def list(namespace: str = typer.Option("default", help="Namespace to list tables from")):
    """List tables in a namespace."""
    ice = get_ice_frame()
    try:
        tables = ice.list_tables(namespace)
        if not tables:
            console.print(f"No tables found in namespace '{namespace}'.")
            return
            
        table = Table(title=f"Tables in '{namespace}'")
        table.add_column("Table Name", style="cyan")
        
        for t in tables:
            table.add_row(t)
            
        console.print(table)
    except Exception as e:
        console.print(f"[red]Error listing tables: {e}[/red]")

@app.command()
def describe(table_name: str):
    """Show table schema and properties."""
    ice = get_ice_frame()
    try:
        table = ice.get_table(table_name)
        
        # Schema
        console.print(f"\n[bold]Schema for {table_name}:[/bold]")
        schema_table = Table(show_header=True, header_style="bold magenta")
        schema_table.add_column("ID", style="dim")
        schema_table.add_column("Name", style="cyan")
        schema_table.add_column("Type", style="green")
        schema_table.add_column("Required")
        
        for field in table.schema().fields:
            schema_table.add_row(
                str(field.field_id),
                field.name,
                str(field.field_type),
                "Yes" if field.required else "No"
            )
        console.print(schema_table)
        
        # Partition Spec
        if table.spec().fields:
            console.print(f"\n[bold]Partition Spec:[/bold]")
            part_table = Table(show_header=True)
            part_table.add_column("Field ID")
            part_table.add_column("Name")
            part_table.add_column("Transform")
            part_table.add_column("Source ID")
            
            for field in table.spec().fields:
                part_table.add_row(
                    str(field.field_id),
                    field.name,
                    str(field.transform),
                    str(field.source_id)
                )
            console.print(part_table)
            
    except Exception as e:
        console.print(f"[red]Error describing table: {e}[/red]")

@app.command()
def head(table_name: str, n: int = typer.Option(5, help="Number of rows to show")):
    """Show first N rows of a table."""
    ice = get_ice_frame()
    try:
        df = ice.read_table(table_name, limit=n)
        console.print(f"\n[bold]First {n} rows of {table_name}:[/bold]")
        console.print(df)
    except Exception as e:
        console.print(f"[red]Error reading table: {e}[/red]")

# MCP Command Group
mcp_app = typer.Typer(help="Manage MCP Server")
app.add_typer(mcp_app, name="mcp")

@mcp_app.command("start")
def start_mcp():
    """Start the MCP server over stdio."""
    try:
        from iceframe.mcp_server import start
        start()
    except ImportError:
        console.print("[red]MCP dependencies not installed. Run: pip install 'iceframe[mcp]'[/red]")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[red]Error starting MCP server: {e}[/red]")
        raise typer.Exit(code=1)

@mcp_app.command("config")
def config_mcp():
    """Print MCP configuration for clients."""
    import sys
    import json
    
    # Get python executable path
    python_path = sys.executable
    
    config = {
        "mcpServers": {
            "iceframe": {
                "command": python_path,
                "args": ["-m", "iceframe.cli", "mcp", "start"],
                "env": {
                    "ICEBERG_CATALOG_URI": os.getenv("ICEBERG_CATALOG_URI", ""),
                    "ICEBERG_CATALOG_TYPE": os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
                    "ICEBERG_WAREHOUSE": os.getenv("ICEBERG_WAREHOUSE", ""),
                    "ICEBERG_TOKEN": os.getenv("ICEBERG_TOKEN", ""),
                    "ICEBERG_CREDENTIAL": os.getenv("ICEBERG_CREDENTIAL", ""),
                    "ICEBERG_OAUTH2_SERVER_URI": os.getenv("ICEBERG_OAUTH2_SERVER_URI", "")
                }
            }
        }
    }
    
    # Filter out empty env vars
    env = config["mcpServers"]["iceframe"]["env"]
    config["mcpServers"]["iceframe"]["env"] = {k: v for k, v in env.items() if v}
    
    print(json.dumps(config, indent=2))

if __name__ == "__main__":
    app()
