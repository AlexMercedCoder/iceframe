"""
Unit tests for Schema Evolution
"""

import pytest
from iceframe.schema import SchemaEvolution
import polars as pl

def test_add_drop_column(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test adding and dropping columns"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add column
    ice_frame.alter_table(test_table_name).add_column("email", "string")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert "email" in table.schema().column_names
    
    # Drop column
    ice_frame.alter_table(test_table_name).drop_column("email")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert "email" not in table.schema().column_names

def test_rename_column(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test renaming columns"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Rename 'name' to 'full_name'
    ice_frame.alter_table(test_table_name).rename_column("name", "full_name")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    assert "full_name" in table.schema().column_names
    assert "name" not in table.schema().column_names

def test_update_column_type(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test updating column type"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Update 'age' (int) to 'long'
    # Note: Iceberg allows int -> long promotion
    ice_frame.alter_table(test_table_name).update_column_type("age", "long")
    
    # Verify
    table = ice_frame.get_table(test_table_name)
    field = table.schema().find_field("age")
    from pyiceberg.types import LongType
    assert isinstance(field.field_type, LongType)
