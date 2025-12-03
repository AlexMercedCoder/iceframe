"""
Unit tests for export functionality
"""

import pytest
import os
import tempfile
import polars as pl


def test_export_to_parquet(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test exporting table to Parquet"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Export to parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        output_path = f.name
    
    try:
        ice_frame.to_parquet(test_table_name, output_path)
        
        # Verify file exists
        assert os.path.exists(output_path)
        
        # Read back and verify
        df = pl.read_parquet(output_path)
        assert len(df) == len(sample_data)
    finally:
        # Cleanup
        if os.path.exists(output_path):
            os.remove(output_path)


def test_export_to_csv(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test exporting table to CSV"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Export to CSV
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
        output_path = f.name
    
    try:
        ice_frame.to_csv(test_table_name, output_path)
        
        # Verify file exists
        assert os.path.exists(output_path)
        
        # Read back and verify
        df = pl.read_csv(output_path)
        assert len(df) == len(sample_data)
    finally:
        # Cleanup
        if os.path.exists(output_path):
            os.remove(output_path)


def test_export_to_json(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test exporting table to JSON"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Export to JSON
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
        output_path = f.name
    
    try:
        ice_frame.to_json(test_table_name, output_path)
        
        # Verify file exists
        assert os.path.exists(output_path)
        
        # Read back and verify
        df = pl.read_json(output_path)
        assert len(df) == len(sample_data)
    finally:
        # Cleanup
        if os.path.exists(output_path):
            os.remove(output_path)


def test_export_with_column_selection(ice_frame, test_table_name, sample_schema, sample_data, cleanup_table):
    """Test exporting specific columns"""
    cleanup_table(test_table_name)
    
    # Create table with data
    ice_frame.create_table(test_table_name, sample_schema)
    ice_frame.append_to_table(test_table_name, sample_data)
    
    # Export specific columns to parquet
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        output_path = f.name
    
    try:
        ice_frame.to_parquet(test_table_name, output_path, columns=["id", "name"])
        
        # Read back and verify columns
        df = pl.read_parquet(output_path)
        assert set(df.columns) == {"id", "name"}
        assert len(df) == len(sample_data)
    finally:
        # Cleanup
        if os.path.exists(output_path):
            os.remove(output_path)
