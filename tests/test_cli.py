"""
Unit tests for CLI
"""

import pytest
from typer.testing import CliRunner
from iceframe.cli import app
from unittest.mock import patch, MagicMock

runner = CliRunner()

@pytest.fixture
def mock_ice_frame():
    with patch("iceframe.cli.IceFrame") as MockIceFrame:
        mock_instance = MockIceFrame.return_value
        yield mock_instance

@pytest.fixture
def mock_env():
    with patch.dict("os.environ", {"ICEBERG_CATALOG_URI": "http://mock-catalog"}):
        yield

def test_list_tables(mock_ice_frame, mock_env):
    """Test list command"""
    mock_ice_frame.list_tables.return_value = ["table1", "table2"]
    
    result = runner.invoke(app, ["list", "--namespace", "default"])
    
    assert result.exit_code == 0
    assert "table1" in result.stdout
    assert "table2" in result.stdout
    mock_ice_frame.list_tables.assert_called_with("default")

def test_describe_table(mock_ice_frame, mock_env):
    """Test describe command"""
    mock_table = MagicMock()
    mock_field = MagicMock()
    mock_field.field_id = 1
    mock_field.name = "id"
    mock_field.field_type = "int"
    mock_field.required = True
    
    mock_table.schema.return_value.fields = [mock_field]
    mock_table.spec.return_value.fields = []
    
    mock_ice_frame.get_table.return_value = mock_table
    
    result = runner.invoke(app, ["describe", "my_table"])
    
    assert result.exit_code == 0
    assert "Schema for my_table" in result.stdout
    assert "id" in result.stdout
    mock_ice_frame.get_table.assert_called_with("my_table")

def test_head_table(mock_ice_frame, mock_env):
    """Test head command"""
    # Mock Polars DataFrame display
    mock_ice_frame.read_table.return_value = "DataFrame Output"
    
    result = runner.invoke(app, ["head", "my_table", "--n", "10"])
    
    assert result.exit_code == 0
    assert "First 10 rows" in result.stdout
    mock_ice_frame.read_table.assert_called_with("my_table", limit=10)

def test_missing_env_var():
    """Test error when env var missing"""
    with patch("iceframe.cli.load_dotenv"):
        with patch.dict("os.environ", {}, clear=True):
            result = runner.invoke(app, ["list"])
            assert result.exit_code == 1
            assert "ICEBERG_CATALOG_URI environment variable not set" in result.stdout
