"""
Utility functions for IceFrame library
"""

import os
from typing import Any, Dict

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
        ValueError: If required configuration is missing.

    Notes:
        REST catalogs without an explicit ``token`` or ``oauth2-server-uri``
        get a single ``warnings.warn`` instead of a hard error — unauthenticated
        local REST setups and SigV4-signed catalogs are both legitimate and
        would have been rejected by the older strict check. Auth failures
        will surface naturally at connect time.
    """
    import warnings

    required_fields = ["uri", "type"]
    for field in required_fields:
        if field not in config or not config[field]:
            raise ValueError(f"Missing required catalog configuration: {field}")

    if config["type"] == "rest":
        has_auth = any(k in config for k in ("token", "oauth2-server-uri", "credential"))
        has_signer = any(k.startswith("rest.sigv4") or k == "rest.signing-name" for k in config)
        if not has_auth and not has_signer:
            warnings.warn(
                "REST catalog has no 'token', 'oauth2-server-uri', 'credential', "
                "or SigV4 signing config — connecting unauthenticated.",
                UserWarning,
                stacklevel=2,
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


def safe_summary_dict(summary: Any) -> Dict[str, Any]:
    """
    Safely convert a PyIceberg Summary object to a plain dict.

    PyIceberg's Summary class extends Mapping but does not implement
    __iter__, so calling dict(summary) raises:
        AttributeError: 'tuple' object has no attribute 'lower'

    This utility uses Pydantic's model_dump() when available, falling
    back to manual construction from the operation and additional_properties.

    Args:
        summary: A PyIceberg Summary object (or any Mapping)

    Returns:
        A plain dict with all summary key-value pairs
    """
    if summary is None:
        return {}

    # Try Pydantic v2 model_dump first (most reliable)
    if hasattr(summary, "model_dump"):
        try:
            return summary.model_dump()
        except Exception:
            pass

    # Fallback: manually reconstruct from known attributes
    if hasattr(summary, "operation") and hasattr(summary, "additional_properties"):
        result: Dict[str, Any] = {"operation": str(summary.operation.value)}
        result.update(summary.additional_properties)
        return result

    # Last resort: it might be a normal dict-like object
    try:
        return dict(summary)
    except Exception:
        return {}
