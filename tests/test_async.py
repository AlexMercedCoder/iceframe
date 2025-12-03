"""
Unit tests for Async Support
"""

import pytest
import polars as pl
import datetime
import asyncio

@pytest.mark.asyncio
async def test_async_read_table(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test async table read"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
    data = pl.DataFrame({
        "id": [1, 2],
        "name": ["A", "B"],
        "age": [20, 30],
        "created_at": [datetime.datetime.now()] * 2
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Read asynchronously
    from iceframe.async_ops import AsyncIceFrame
    async_ice = AsyncIceFrame(ice_frame.catalog_config)
    
    result = await async_ice.read_table_async(test_table_name)
    assert result.height == 2

@pytest.mark.asyncio
async def test_async_query(ice_frame, test_table_name, sample_schema, cleanup_table):
    """Test async query execution"""
    cleanup_table(test_table_name)
    ice_frame.create_table(test_table_name, sample_schema)
    
    # Add data
    data = pl.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "age": [20, 30, 40],
        "created_at": [datetime.datetime.now()] * 3
    }).with_columns([
        pl.col("age").cast(pl.Int32),
        pl.col("created_at").cast(pl.Datetime("us"))
    ])
    ice_frame.append_to_table(test_table_name, data)
    
    # Query asynchronously
    from iceframe.async_ops import AsyncIceFrame
    from iceframe.expressions import Column
    async_ice = AsyncIceFrame(ice_frame.catalog_config)
    
    query = await async_ice.query_async(test_table_name)
    result = await query.filter(Column("age") > 25).execute_async()
    
    assert result.height == 2
