"""
Iceberg Views management.
"""

from typing import Dict, Any, Optional, List
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from iceframe.utils import normalize_table_identifier

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
        properties: Optional[Dict[str, str]] = None,
        replace: bool = False
    ) -> Any:
        """
        Create a view.
        
        Args:
            view_name: Name of the view
            sql: SQL query for the view
            properties: View properties
            replace: Whether to replace if exists
            
        Returns:
            Created View object
        """
        namespace, view = normalize_table_identifier(view_name)
        full_name = f"{namespace}.{view}"
        
        # Check if catalog supports views
        if not hasattr(self.catalog, "create_view"):
            raise NotImplementedError("This catalog does not support creating views via PyIceberg")
            
        try:
            if replace:
                # PyIceberg might not have create_or_replace_view yet
                try:
                    self.catalog.drop_view(full_name)
                except Exception:
                    pass
            
            # Note: PyIceberg create_view signature might vary
            # Assuming standard (identifier, sql, properties)
            return self.catalog.create_view(
                identifier=full_name,
                sql=sql,
                properties=properties or {}
            )
        except Exception as e:
            raise RuntimeError(f"Failed to create view {view_name}: {e}")
            
    def drop_view(self, view_name: str) -> None:
        """Drop a view"""
        namespace, view = normalize_table_identifier(view_name)
        full_name = f"{namespace}.{view}"
        
        if not hasattr(self.catalog, "drop_view"):
            raise NotImplementedError("This catalog does not support dropping views")
            
        self.catalog.drop_view(full_name)
        
    def list_views(self, namespace: str = "default") -> List[str]:
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
