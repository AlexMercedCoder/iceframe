"""
Iceberg Views management.
"""

from typing import Any

import polars as pl
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchViewError
from pyiceberg.schema import Schema

from iceframe.exceptions import CatalogError, UnsupportedOperationError, ValidationError
from iceframe.utils import normalize_table_identifier


def _to_view_schema(schema: Any) -> Schema | pa.Schema:
    """Accept a PyIceberg, PyArrow or Polars schema for a view definition."""
    if isinstance(schema, (Schema, pa.Schema)):
        return schema
    if isinstance(schema, (pl.Schema, dict)):
        return pl.DataFrame(schema=schema).to_arrow().schema
    raise ValidationError(
        f"Unsupported view schema type {type(schema).__name__}; use a pyarrow.Schema, "
        "PyIceberg Schema or Polars schema"
    )


class ViewManager:
    """
    Manage Iceberg Views.

    Note: View support in PyIceberg is evolving. This class provides a high-level
    interface that uses the underlying catalog's view capabilities if available.
    """

    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    def create_view(
        self,
        view_name: str,
        sql: str,
        schema: Any = None,
        properties: dict[str, str] | None = None,
        replace: bool = False,
        dialect: str = "spark",
    ) -> Any:
        """
        Create a view.

        The Iceberg view spec stores the schema of the view's result alongside
        its SQL, and PyIceberg does not parse SQL to derive it, so ``schema``
        is required.

        Args:
            view_name: Name of the view ("namespace.view")
            sql: SQL query for the view
            schema: Result schema, as a PyIceberg ``Schema``, a ``pyarrow.Schema``
                or a Polars schema (``pl.Schema`` or a ``{name: dtype}`` dict)
            properties: View properties
            replace: Whether to replace if exists
            dialect: SQL dialect recorded in the view representation

        Returns:
            Created View object
        """
        try:
            from pyiceberg.view.metadata import ViewVersion
        except ImportError as e:
            raise UnsupportedOperationError(
                "Creating views needs pyiceberg>=0.12; the installed PyIceberg can only "
                "list, load and drop them"
            ) from e

        if schema is None:
            raise ValidationError(
                f"create_view('{view_name}') needs the view's result schema: pass "
                "schema= as a pyarrow.Schema, PyIceberg Schema or Polars schema"
            )

        namespace, view = normalize_table_identifier(view_name)
        full_name = f"{namespace}.{view}"

        if replace:
            try:
                self.catalog.drop_view(full_name)
            except NoSuchViewError:
                pass
            except NotImplementedError as e:
                raise UnsupportedOperationError(f"This catalog does not support views: {e}") from e

        view_schema = _to_view_schema(schema)

        # Built from the spec's wire names: PyIceberg's models are keyed by alias.
        view_version = ViewVersion.model_validate(
            {
                "version-id": 1,
                "schema-id": 0,
                "representations": [{"type": "sql", "sql": sql, "dialect": dialect}],
                "default-namespace": namespace.split("."),
            }
        )

        try:
            return self.catalog.create_view(
                identifier=full_name,
                schema=view_schema,
                view_version=view_version,
                properties=properties or {},
            )
        except NotImplementedError as e:
            raise UnsupportedOperationError(f"This catalog does not support views: {e}") from e
        except Exception as e:
            raise CatalogError(f"Failed to create view {view_name}: {e}") from e

    def drop_view(self, view_name: str) -> None:
        """Drop a view"""
        namespace, view = normalize_table_identifier(view_name)
        full_name = f"{namespace}.{view}"

        if not hasattr(self.catalog, "drop_view"):
            raise NotImplementedError("This catalog does not support dropping views")

        self.catalog.drop_view(full_name)

    def list_views(self, namespace: str = "default") -> list[str]:
        """List views in a namespace"""
        if not hasattr(self.catalog, "list_views"):
            # Fallback: list_tables might include views in some catalogs, or not supported
            return []

        try:
            views = self.catalog.list_views(namespace)
            return [str(v) for v in views]
        except Exception:
            return []

    def get_view(self, view_name: str) -> Any:
        """Get a view object"""
        namespace, view = normalize_table_identifier(view_name)
        full_name = f"{namespace}.{view}"

        if not hasattr(self.catalog, "load_view"):
            raise NotImplementedError("This catalog does not support loading views")

        return self.catalog.load_view(full_name)
