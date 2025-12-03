"""
Unit tests for Namespace Management
"""

import pytest
from iceframe.namespace import NamespaceManager

def test_create_drop_namespace(ice_frame):
    """Test creating and dropping a namespace"""
    ns_name = "test_ns"
    
    # Ensure it doesn't exist
    try:
        ice_frame.drop_namespace(ns_name)
    except:
        pass
        
    # Create
    ice_frame.create_namespace(ns_name, {"owner": "test"})
    
    # List to verify
    namespaces = ice_frame.list_namespaces()
    # Note: list_namespaces returns list of tuples, e.g. [('default',), ('test_ns',)]
    # Depending on catalog, it might be nested.
    
    # Check if our namespace is in the list
    # Normalize to tuples if needed, but PyIceberg returns tuples
    assert any(ns == (ns_name,) or ns == ns_name for ns in namespaces)
    
    # Check properties
    props = ice_frame.namespaces.load_namespace_properties(ns_name)
    assert props.get("owner") == "test"
    
    # Drop
    ice_frame.drop_namespace(ns_name)
    
    # Verify dropped
    namespaces_after = ice_frame.list_namespaces()
    assert not any(ns == (ns_name,) or ns == ns_name for ns in namespaces_after)

def test_nested_namespace(ice_frame):
    """Test creating nested namespaces if supported"""
    # Note: Not all catalogs support nested namespaces, but REST usually does
    parent = "parent_ns"
    child = "parent_ns.child_ns"
    
    try:
        ice_frame.create_namespace(parent)
        ice_frame.create_namespace(child)
        
        namespaces = ice_frame.list_namespaces(parent)
        # Should find child
        assert any("child_ns" in str(ns) for ns in namespaces)
        
    finally:
        try:
            ice_frame.drop_namespace(child)
        except:
            pass
        try:
            ice_frame.drop_namespace(parent)
        except:
            pass
