"""
IceFrame - A DataFrame-like library for Apache Iceberg tables

IceFrame provides a simple, intuitive API for working with Apache Iceberg tables
using REST or SQL catalogs with local execution.
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__ = _pkg_version("iceframe")
except PackageNotFoundError:  # editable / source checkout without metadata installed
    __version__ = "0.0.0+unknown"

from iceframe.core import IceFrame
from iceframe.expressions import col, lit, Expression
from iceframe.query import QueryBuilder
from iceframe.utils import load_catalog_config_from_env

__all__ = [
    "IceFrame",
    "QueryBuilder",
    "Expression",
    "col",
    "lit",
    "load_catalog_config_from_env",
    "__version__",
]
