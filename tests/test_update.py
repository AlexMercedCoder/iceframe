"""
Unit tests for table update operations
"""

import pytest
import polars as pl


def test_append_to_table(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test appending data to a table"""
    cleanup_table(test_table_name)
    
    # Create table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Append data
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Read and verify
    df = ice_frame.read_table(test_table_name)
    assert len(df) == len(sample_data)


def test_append_multiple_times(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test appending data multiple times"""
    cleanup_table(test_table_name)
    
    # Create table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Append data twice
    ice_frame.append_to_table(test_table_name, sample_data)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Read and verify
    df = ice_frame.read_table(test_table_name)
    assert len(df) == len(sample_data) * 2


def test_overwrite_table(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test overwriting table data"""
    cleanup_table(test_table_name)
    
    # Create table and add initial data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Create new data
    import datetime
    new_data = pl.DataFrame({
        "id": [100, 200],
        "name": ["New1", "New2"],
        "age": pl.Series([50, 55], dtype=pl.Int32),
        "created_at": [
            datetime.datetime(2024, 2, 1),
            datetime.datetime(2024, 2, 2)
        ],
    }, schema={
        "id": pl.Int64,
        "name": pl.Utf8,
        "age": pl.Int32,
        "created_at": pl.Datetime("us")
    })
    
    # Overwrite
    ice_frame.overwrite_table(test_table_name, new_data)
    
    # Read and verify
    df = ice_frame.read_table(test_table_name)
    assert len(df) == len(new_data)
    assert df["id"].to_list() == [100, 200]


def test_append_with_dict(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test appending data from dictionary"""
    cleanup_table(test_table_name)
    
    # Create table
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Append from dict
    import datetime
    data_dict = {
        "id": [1, 2],
        "name": ["Test1", "Test2"],
        "age": [20, 25],
        "created_at": [
            datetime.datetime(2024, 1, 1),
            datetime.datetime(2024, 1, 2)
        ],
    }
    
    # Convert to Polars DataFrame with explicit types to ensure matching schema
    df_dict = pl.DataFrame(data_dict).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    
    ice_frame.append_to_table(test_table_name, df_dict)
    
    # Read and verify
    df = ice_frame.read_table(test_table_name)
    assert len(df) == 2
