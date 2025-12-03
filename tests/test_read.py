"""
Unit tests for table reading functionality
"""

import pytest
import polars as pl


def test_read_empty_table(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test reading an empty table"""
    cleanup_table(test_table_name)
    
    # Create empty table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Read table
    df = ice_frame.read_table(test_table_name)
    
    # Verify empty DataFrame
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0


def test_read_table_with_data(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test reading a table with data"""
    cleanup_table(test_table_name)
    
    # Create table and add data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Read table
    df = ice_frame.read_table(test_table_name)
    
    # Verify data
    assert isinstance(df, pl.DataFrame)
    assert len(df) == len(sample_data)
    assert set(df.columns) == set(sample_data.columns)


def test_read_table_with_column_selection(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test reading specific columns"""
    cleanup_table(test_table_name)
    
    # Create table and add data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Read specific columns
    df = ice_frame.read_table(test_table_name, columns=["id", "name"])
    
    # Verify columns
    assert set(df.columns) == {"id", "name"}
    assert len(df) == len(sample_data)


def test_read_table_with_limit(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test reading with row limit"""
    cleanup_table(test_table_name)
    
    # Create table and add data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Read with limit
    df = ice_frame.read_table(test_table_name, limit=3)
    
    # Verify limit
    assert len(df) == 3


def test_get_table(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test getting underlying PyIceberg table object"""
    cleanup_table(test_table_name)
    
    # Create table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Get table object
    table = ice_frame.get_table(test_table_name)
    
    # Verify it's a PyIceberg table
    assert table is not None
    assert hasattr(table, "schema")
