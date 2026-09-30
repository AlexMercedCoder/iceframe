"""
Catalog federation for multi-catalog support.
"""

from typing import TYPE_CHECKING, Any

import polars as pl

if TYPE_CHECKING:
    from iceframe.core import IceFrame


class CatalogFederation:
    """
    Manage multiple catalogs and enable cross-catalog queries.
    """

    def __init__(self):
        self.catalogs: dict[str, IceFrame] = {}

    def add_catalog(self, name: str, catalog_config: dict[str, Any]):
        """
        Add a catalog to the federation.

        Args:
            name: Catalog name
            catalog_config: Catalog configuration
        """
        from iceframe.core import IceFrame

        self.catalogs[name] = IceFrame(catalog_config)

    def list_catalogs(self) -> list[str]:
        """List all registered catalogs"""
        return list(self.catalogs.keys())

    def get_catalog(self, name: str) -> "IceFrame":
        """
        Get a catalog by name.

        Args:
            name: Catalog name

        Returns:
            IceFrame instance
        """
        if name not in self.catalogs:
            raise ValueError(f"Catalog '{name}' not found")
        return self.catalogs[name]

    def read_table(self, catalog_name: str, table_name: str, **kwargs) -> pl.DataFrame:
        """
        Read table from a specific catalog.

        Args:
            catalog_name: Name of the catalog
            table_name: Name of the table
            **kwargs: Additional arguments for read_table

        Returns:
            Polars DataFrame
        """
        catalog = self.get_catalog(catalog_name)
        return catalog.read_table(table_name, **kwargs)

    def union_tables(
        self,
        table_specs: list[tuple[str, str]],
    ) -> pl.DataFrame:
        """
        Union tables from multiple catalogs.

        Args:
            table_specs: List of (catalog_name, table_name) tuples

        Returns:
            Combined DataFrame
        """
        dfs: list[pl.DataFrame] = []
        for catalog_name, table_name in table_specs:
            df = self.read_table(catalog_name, table_name)
            dfs.append(df)

        if not dfs:
            return pl.DataFrame()

        # Union all DataFrames
        result = dfs[0]
        for df in dfs[1:]:
            result = pl.concat([result, df])

        return result
