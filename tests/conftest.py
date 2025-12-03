"""
Pytest fixtures for IceFrame tests
"""

import pytest
import os
import polars as pl
import pyarrow as pa
from iceframe import IceFrame
from iceframe.utils import load_catalog_config_from_env


@pytest.fixture(scope="session")
def catalog_config():
    """Load catalog configuration from environment"""
    config = load_catalog_config_from_env()
    return config


@pytest.fixture(scope="session")
def ice_frame(catalog_config):
    """Create IceFrame instance for testing"""
    return IceFrame(catalog_config)


@pytest.fixture
def test_table_name():
    """Generate unique test table name"""
    import time
    return f"test_table_{int(time.time() * 1000)}"


@pytest.fixture
def sample_schema():
    """Sample PyArrow schema for testing"""
    return pa.schema([
        pa.field("id", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("age", pa.int32()),
        pa.field("created_at", pa.timestamp("us")),
    ])


@pytest.fixture
def sample_data():
    """Sample data for testing"""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": pl.Series([25, 30, 35, 40, 45], dtype=pl.Int32),
        "created_at": pl.datetime_range(
            start=pl.datetime(2024, 1, 1),
            end=pl.datetime(2024, 1, 5),
            interval="1d",
            eager=True,
        ),
    })


@pytest.fixture
def cleanup_table(ice_frame):
    """Fixture to cleanup test tables after tests"""
    tables_to_cleanup = []
    
    def register_table(table_name):
        tables_to_cleanup.append(table_name)
    
    yield register_table
    
    # Cleanup after test
    for table_name in tables_to_cleanup:
        try:
            if ice_frame.table_exists(table_name):
                ice_frame.drop_table(table_name)
        except Exception as e:
            print(f"Failed to cleanup table {table_name}: {e}")
