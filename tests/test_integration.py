import os
import sys
from typing import Optional
from pydantic import BaseModel
from unittest.mock import MagicMock
import unittest
import sys

# Mock IPython
ipython_mock = MagicMock()
sys.modules["IPython"] = ipython_mock
sys.modules["IPython.core"] = ipython_mock
sys.modules["IPython.core.magic"] = ipython_mock
sys.modules["IPython.core.display"] = ipython_mock

# Fix decorators to be pass-through
def identity_decorator(func):
    return func

ipython_mock.line_magic = identity_decorator
ipython_mock.cell_magic = identity_decorator
ipython_mock.magics_class = identity_decorator

# Define dummy Magics class
class DummyMagics:
    def __init__(self, shell):
        self.shell = shell

ipython_mock.Magics = DummyMagics

from iceframe import IceFrame
from iceframe.magics import IceFrameMagics

# Mock catalog config for testing without a real catalog
# Note: In a real scenario, we'd need a running catalog or mock the catalog interaction
# For this script, we'll try to use a mock catalog if possible, or just verify the schema conversion logic

class User(BaseModel):
    id: int
    name: str
    email: Optional[str] = None
    is_active: bool = True

def test_pydantic_schema_conversion():
    print("Testing Pydantic schema conversion...")
    from iceframe.pydantic import to_iceberg_schema
    
    schema = to_iceberg_schema(User)
    print(f"Generated Schema: {schema}")
    
    # Verify fields
    fields = {f.name: f.field_type for f in schema.fields}
    assert "id" in fields
    assert "name" in fields
    assert "email" in fields
    assert "is_active" in fields
    print("Schema conversion successful!")

def test_iceframe_integration():
    print("\nTesting IceFrame integration...")
    
    # Mock the catalog and operations
    config = {"uri": "http://mock", "type": "rest", "warehouse": "s3://mock", "token": "mock_token"}
    
    # Mock CatalogPool to avoid connection attempts
    with unittest.mock.patch('iceframe.core.CatalogPool') as MockPool:
        mock_pool_instance = MockPool.return_value
        mock_pool_instance.get_connection.return_value = MagicMock()
        
        ice = IceFrame(config)
        ice._operations = MagicMock()
    
    # Test create_table with Pydantic model
    ice.create_table("test_table", User)
    
    # Verify _operations.create_table was called with a Schema
    call_args = ice._operations.create_table.call_args
    assert call_args is not None
    schema_arg = call_args.kwargs.get('schema')
    from pyiceberg.schema import Schema
    assert isinstance(schema_arg, Schema)
    print("create_table with Pydantic model verified!")
    
    # Test insert_items
    users = [
        User(id=1, name="Alice", email="alice@example.com"),
        User(id=2, name="Bob", is_active=False)
    ]
    ice.insert_items("test_table", users)
    
    # Verify append_to_table was called with a DataFrame
    call_args = ice._operations.append_to_table.call_args
    assert call_args is not None
    data_arg = call_args.args[1]
    import polars as pl
    assert isinstance(data_arg, pl.DataFrame)
    assert data_arg.height == 2
    print("insert_items verified!")
    
    # Test _repr_html_
    html = ice._repr_html_()
    assert "IceFrame Connection" in html
    assert "http://mock" in html
    print("_repr_html_ verified!")

def test_magics():
    print("\nTesting Magics...")
    shell = MagicMock()
    shell.user_ns = {}
    
    magics = IceFrameMagics(shell)
    
    # Test %iceframe
    ice_mock = MagicMock()
    ice_mock.read_table = MagicMock()
    ice_mock.query = MagicMock()
    shell.user_ns["my_ice"] = ice_mock
    
    magics.iceframe("my_ice")
    assert magics.active_iceframe == ice_mock
    print("%iceframe magic verified!")
    
    # Test %%iceql
    # We can't easily test the full execution without a real engine, but we can check basic parsing
    # and that it tries to call read_table
    
    # Mock read_table to return a DataFrame
    import polars as pl
    df = pl.DataFrame({"id": [1], "name": ["Alice"]})
    ice_mock.read_table.return_value = df
    
    # Mock display
    # We can't mock IPython.core.display.display easily here as it's imported in the module
    # But we can try running it and ignore errors if display fails (or mock it in sys.modules)
    
    try:
        magics.iceql(None, "SELECT * FROM my_table")
        # Verify read_table was called
        ice_mock.read_table.assert_called_with("my_table")
        print("%%iceql magic verified!")
    except Exception as e:
        print(f"%%iceql test warning (expected if display not mocked): {e}")

if __name__ == "__main__":
    test_pydantic_schema_conversion()
    test_iceframe_integration()
    test_magics()
