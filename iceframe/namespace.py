"""
Namespace management for IceFrame.
"""

from typing import Dict, List, Optional, Any
from pyiceberg.catalog import Catalog

class NamespaceManager:
    """
    Manages namespaces in the Iceberg catalog.
    """
    
    def __init__(self, catalog: Catalog):
        self.catalog = catalog
        
    def create_namespace(self, name: str, properties: Optional[Dict[str, str]] = None) -> None:
        """
        Create a new namespace.
        
        Args:
            name: Name of the namespace (e.g., "marketing" or "marketing.campaigns")
            properties: Optional dictionary of properties for the namespace
        """
        self.catalog.create_namespace(name, properties or {})
        
    def drop_namespace(self, name: str) -> None:
        """
        Drop a namespace.
        
        Args:
            name: Name of the namespace to drop
        """
        self.catalog.drop_namespace(name)
        
    def list_namespaces(self, parent: Optional[str] = None) -> List[tuple]:
        """
        List namespaces.
        
        Args:
            parent: Optional parent namespace to list children of
            
        Returns:
            List of namespace identifiers (tuples)
        """
        return self.catalog.list_namespaces(parent or ())
        
    def load_namespace_properties(self, name: str) -> Dict[str, str]:
        """
        Load properties for a namespace.
        
        Args:
            name: Name of the namespace
            
        Returns:
            Dictionary of properties
        """
        return self.catalog.load_namespace_properties(name)
        
    def update_namespace_properties(self, name: str, removals: Optional[set] = None, updates: Optional[Dict[str, str]] = None) -> None:
        """
        Update namespace properties.
        
        Args:
            name: Name of the namespace
            removals: Set of property keys to remove
            updates: Dictionary of properties to add or update
        """
        self.catalog.update_namespace_properties(name, removals or set(), updates or {})
