"""
IceFrame - A DataFrame-like library for Apache Iceberg tables

IceFrame provides a simple, intuitive API for working with Apache Iceberg tables
using REST or SQL catalogs with local execution.
"""

#: Single source of truth for the package version. ``pyproject.toml`` reads it
#: from here via ``[tool.setuptools.dynamic]``, so the two can never drift.
__version__ = "0.13.0"

from iceframe.core import IceFrame
from iceframe.exceptions import (
    CatalogError,
    CompactionError,
    IceFrameError,
    MaintenanceError,
    SchemaError,
    TableNotFoundError,
    UnsupportedOperationError,
    ValidationError,
)
from iceframe.expressions import Expression, col, lit
from iceframe.metadata import MetadataInspector
from iceframe.query import QueryBuilder
from iceframe.utils import load_catalog_config_from_env

__all__ = [
    "IceFrame",
    "QueryBuilder",
    "Expression",
    "MetadataInspector",
    "col",
    "lit",
    "load_catalog_config_from_env",
    # Exception hierarchy
    "IceFrameError",
    "CatalogError",
    "TableNotFoundError",
    "SchemaError",
    "ValidationError",
    "CompactionError",
    "MaintenanceError",
    "UnsupportedOperationError",
    "__version__",
]
