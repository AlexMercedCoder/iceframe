"""
Utility functions for IceFrame library
"""

from typing import Dict, Any, Optional
import os
from dotenv import load_dotenv


def load_catalog_config_from_env() -> Dict[str, Any]:
    """
    Load catalog configuration from environment variables.
    
    Returns:
        Dict containing catalog configuration
    """
    load_dotenv()
    
    config = {
        "uri": os.getenv("ICEBERG_CATALOG_URI"),
        "type": os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
        "warehouse": os.getenv("ICEBERG_WAREHOUSE"),
    }
    
    # Add token if present
    token = os.getenv("ICEBERG_TOKEN")
    if token:
        config["token"] = token
    
    # Add OAuth2 server URI if present
    oauth2_uri = os.getenv("ICEBERG_OAUTH2_SERVER_URI")
    if oauth2_uri:
        config["oauth2-server-uri"] = oauth2_uri
    
    # Add credential vending if enabled
    credential_vending = os.getenv("ICEBERG_CREDENTIAL_VENDING")
    if credential_vending:
        config["header.X-Iceberg-Access-Delegation"] = credential_vending
    
    return config


def validate_catalog_config(config: Dict[str, Any]) -> None:
    """
    Validate catalog configuration.
    
    Args:
        config: Catalog configuration dictionary
        
    Raises:
        ValueError: If required configuration is missing
    """
    required_fields = ["uri", "type"]
    
    for field in required_fields:
        if field not in config or not config[field]:
            raise ValueError(f"Missing required catalog configuration: {field}")
    
    # For REST catalogs, we need either a token or OAuth2 configuration
    if config["type"] == "rest":
        if "token" not in config and "oauth2-server-uri" not in config:
            raise ValueError(
                "REST catalog requires either 'token' or 'oauth2-server-uri' configuration"
            )


def normalize_table_identifier(table_name: str) -> tuple:
    """
    Normalize table identifier to (namespace, table_name) tuple.
    
    Args:
        table_name: Table name, can be 'table' or 'namespace.table'
        
    Returns:
        Tuple of (namespace, table_name)
    """
    parts = table_name.split(".")
    
    if len(parts) == 1:
        # No namespace specified, use default
        return ("default", parts[0])
    elif len(parts) == 2:
        return (parts[0], parts[1])
    else:
        # Multiple dots, treat all but last as namespace
        namespace = ".".join(parts[:-1])
        table = parts[-1]
        return (namespace, table)


def format_table_identifier(namespace: str, table_name: str) -> str:
    """
    Format namespace and table name into full identifier.
    
    Args:
        namespace: Table namespace
        table_name: Table name
        
    Returns:
        Full table identifier
    """
    return f"{namespace}.{table_name}"
