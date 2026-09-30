"""
Command Line Interface for IceFrame.
"""

import os
from typing import Any

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from iceframe.core import IceFrame
from iceframe.utils import load_catalog_config_from_env

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

    config = load_catalog_config_from_env()

    try:
        return IceFrame(config)
    except Exception as e:
        console.print(f"[red]Error initializing IceFrame: {e}[/red]")
        raise typer.Exit(code=1) from e


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
        raise typer.Exit(code=1) from e


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
                "Yes" if field.required else "No",
            )
        console.print(schema_table)

        # Partition Spec
        if table.spec().fields:
            console.print("\n[bold]Partition Spec:[/bold]")
            part_table = Table(show_header=True)
            part_table.add_column("Field ID")
            part_table.add_column("Name")
            part_table.add_column("Transform")
            part_table.add_column("Source ID")

            for partition_field in table.spec().fields:
                part_table.add_row(
                    str(partition_field.field_id),
                    partition_field.name,
                    str(partition_field.transform),
                    str(partition_field.source_id),
                )
            console.print(part_table)

    except Exception as e:
        console.print(f"[red]Error describing table: {e}[/red]")
        raise typer.Exit(code=1) from e


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
        raise typer.Exit(code=1) from e


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
        raise typer.Exit(code=1) from None
    except Exception as e:
        console.print(f"[red]Error starting MCP server: {e}[/red]")
        raise typer.Exit(code=1) from e


@mcp_app.command("config")
def config_mcp():
    """Print MCP configuration for clients."""
    import json
    import sys

    # Get python executable path
    python_path = sys.executable

    config: dict[str, Any] = {
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
                    "ICEBERG_OAUTH2_SERVER_URI": os.getenv("ICEBERG_OAUTH2_SERVER_URI", ""),
                },
            }
        }
    }

    # Filter out empty env vars
    env = config["mcpServers"]["iceframe"]["env"]
    config["mcpServers"]["iceframe"]["env"] = {k: v for k, v in env.items() if v}

    print(json.dumps(config, indent=2))


if __name__ == "__main__":
    app()
