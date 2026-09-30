"""
DataFusion integration for IceFrame.
"""

import re
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds

from iceframe.utils import from_arrow_dataframe, normalize_table_identifier

try:
    import datafusion
    import datafusion.catalog

    DATAFUSION_AVAILABLE = True
except ImportError:
    DATAFUSION_AVAILABLE = False


_TABLE_REFERENCE = re.compile(
    r"\b(?:from|join)\s+((?:\"[^\"]+\"|[A-Za-z_][\w]*)(?:\.(?:\"[^\"]+\"|[A-Za-z_][\w]*))*)",
    re.IGNORECASE,
)


def referenced_tables(sql: str) -> list[str]:
    """Names that follow FROM or JOIN in ``sql``, unquoted, in first-seen order."""
    names: list[str] = []
    for match in _TABLE_REFERENCE.finditer(sql):
        name = match.group(1).replace('"', "")
        if name not in names:
            names.append(name)
    return names


class DataFusionManager:
    """
    Manage DataFusion context and execution.

    Iceberg tables are registered under a DataFusion schema named after their
    namespace, so ``SELECT * FROM analytics.events`` resolves the same way it
    does in IceFrame. Each table is also reachable by its bare name (``events``)
    unless another registered table already claimed that name.
    """

    def __init__(self, ice_frame):
        """
        Initialize DataFusion manager.

        Args:
            ice_frame: IceFrame instance
        """
        if not DATAFUSION_AVAILABLE:
            raise ImportError(
                "datafusion is required. Install with 'pip install iceframe[datafusion]'"
            )

        self.ice_frame = ice_frame
        self.ctx = datafusion.SessionContext()
        self._schemas: dict[str, Any] = {}

    def register_table(self, table_name: str, alias: str | None = None) -> None:
        """
        Register an Iceberg table with DataFusion.

        The table is scanned into memory once and exposed to DataFusion as an
        Arrow dataset; DataFusion does not read the Iceberg files itself.

        Args:
            table_name: Name of the Iceberg table ("namespace.table")
            alias: Register under this single name instead of the
                namespace-qualified and bare names
        """
        batch_reader = self.ice_frame._operations.scan_batches(table_name)
        dataset = ds.dataset(pa.Table.from_batches(batch_reader))

        if alias:
            self.ctx.register_table(alias, dataset)
            return

        namespace, table = normalize_table_identifier(table_name)
        schema = self._schemas.get(namespace)
        if schema is None:
            schema = datafusion.catalog.Schema.memory_schema()
            self.ctx.catalog().register_schema(namespace, schema)
            self._schemas[namespace] = schema
        schema.register_table(table, dataset)

        if not self.ctx.table_exist(table):
            self.ctx.register_table(table, dataset)

    def query(self, sql: str) -> pl.DataFrame:
        """
        Execute SQL query using DataFusion.

        Args:
            sql: SQL query string

        Returns:
            Polars DataFrame result
        """
        df_result = self.ctx.sql(sql)
        # Convert DataFusion DataFrame to PyArrow Table then Polars
        return from_arrow_dataframe(df_result.to_arrow_table())
