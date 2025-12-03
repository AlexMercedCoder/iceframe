"""
Unit tests for JOIN support
"""

import pytest
import polars as pl
import datetime

def test_inner_join(ice_frame, sample_schema, cleanup_table):
    """Test inner join between two tables"""
    table1 = "test_join_users"
    table2 = "test_join_orders"
    
    cleanup_table(table1)
    cleanup_table(table2)
    
    # Create users table
    ice_frame.create_table(table1, sample_schema)
    users_data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(table1, users_data)
    
    # Create orders table
    orders_schema = {
        "order_id": "long",
        "id": "long",  # user_id
        "amount": "double"
    }
    ice_frame.create_table(table2, orders_schema)
    orders_data = pl.DataFrame({
        "order_id": [101, 102, 103],
        "id": [1, 1, 2],  # user_id
        "amount": [100.0, 200.0, 150.0]
    })
    ice_frame.append_to_table(table2, orders_data)
    
    # Perform inner join
    result = (ice_frame.query(table1)
              .join(table2, on="id", how="inner")
              .select("name", "order_id", "amount")
              .execute())
    
    assert result.height == 3
    assert "name" in result.columns
    assert "order_id" in result.columns
    assert "amount" in result.columns
    
    # Cleanup
    cleanup_table(table1)
    cleanup_table(table2)

def test_left_join(ice_frame, sample_schema, cleanup_table):
    """Test left join"""
    table1 = "test_join_users_left"
    table2 = "test_join_orders_left"
    
    cleanup_table(table1)
    cleanup_table(table2)
    
    # Create users table
    ice_frame.create_table(table1, sample_schema)
    users_data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
        "age": [25, 30, 35],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(table1, users_data)
    
    # Create orders table (only for user 1)
    orders_schema = {
        "order_id": "long",
        "id": "long",
        "amount": "double"
    }
    ice_frame.create_table(table2, orders_schema)
    orders_data = pl.DataFrame({
        "order_id": [101],
        "id": [1],
        "amount": [100.0]
    })
    ice_frame.append_to_table(table2, orders_data)
    
    # Perform left join
    result = (ice_frame.query(table1)
              .join(table2, on="id", how="left")
              .execute())
    
    # Should have all 3 users
    assert result.height == 3
    
    # Cleanup
    cleanup_table(table1)
    cleanup_table(table2)
