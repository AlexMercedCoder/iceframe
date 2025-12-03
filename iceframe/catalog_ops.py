"""
Catalog operations.
"""

from typing import Any
from pyiceberg.catalog import Catalog

from iceframe.utils import normalize_table_identifier

class CatalogOperations:
    """
    Manage catalog-level operations.
    """
    
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        
    def register_table(self, table_name: str, metadata_location: str) -> Any:
        """
        Register an existing table into the catalog.
        
        Args:
            table_name: Name of the table to create
            metadata_location: Location of the metadata.json file
            
        Returns:
            Registered Table object
        """
        namespace, table = normalize_table_identifier(table_name)
        full_name = f"{namespace}.{table}"
        
        try:
            if hasattr(self.catalog, "register_table"):
                return self.catalog.register_table(full_name, metadata_location)
            else:
                raise NotImplementedError("Register table not supported by this catalog/client")
        except AttributeError:
            raise NotImplementedError("Register table not supported by this catalog/client")
