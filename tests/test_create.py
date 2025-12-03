"""
Unit tests for table creation functionality
"""

import pytest
import pyarrow as pa
import polars as pl


def test_create_table_with_pyarrow_schema(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test creating a table with PyArrow schema"""
    cleanup_table(test_table_name)
    
    # Create table
    table = ice_frame.create_table(test_table_name, sample_schema)
    
    # Verify table was created
    assert ice_frame.table_exists(test_table_name)
    assert table is not None


def test_create_table_with_dict_schema(ice_frame, cleanup_table):
    """Test creating a table with dictionary schema"""
    table_name = f"test_dict_schema_{int(pytest.importorskip('time').time() * 1000)}"
    cleanup_table(table_name)
    
    schema_dict = {
        "id": "long",
        "name": "string",
        "value": "double",
    }
    
    # Create table
    table = ice_frame.create_table(table_name, schema_dict)
    
    # Verify table was created
    assert ice_frame.table_exists(table_name)


def test_create_table_with_namespace(ice_frame, sample_schema, cleanup_table):
    """Test creating a table with explicit namespace"""
    import time
    table_name = f"default.test_ns_{int(time.time() * 1000)}"
    cleanup_table(table_name)
    
    # Create table
    table = ice_frame.create_table(table_name, sample_schema)
    
    # Verify table was created
    assert ice_frame.table_exists(table_name)


def test_table_exists_false(ice_frame):
    """Test table_exists returns False for non-existent table"""
    assert not ice_frame.table_exists("nonexistent_table_12345")


def test_list_tables(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test listing tables in namespace"""
    cleanup_table(test_table_name)
    
    # Create a test table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # List tables
    tables = ice_frame.list_tables("default")
    
    # Verify our table is in the list
    assert isinstance(tables, list)
    # Tables are returned as strings representing tuples like "('default', 'table_name')"
    # Extract just the table names for comparison
    found = False
    for table_str in tables:
        # Handle both "('namespace', 'table')" and "namespace.table" formats
        if test_table_name in table_str:
            found = True
            break
    
    assert found, f"Table {test_table_name} not found in {tables}"
