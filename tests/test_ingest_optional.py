import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from iceframe.ingest import read_delta, read_lance, read_vortex, read_excel, read_gsheets, read_hudi
from iceframe.core import IceFrame

@pytest.fixture
def mock_polars():
    with patch('iceframe.ingest.pl') as mock_pl:
        yield mock_pl

def test_read_delta(mock_polars):
    read_delta("test_delta")
    mock_polars.read_delta.assert_called_once_with("test_delta", version=None)

def test_read_excel(mock_polars):
    read_excel("test.xlsx")
    mock_polars.read_excel.assert_called_once_with("test.xlsx", sheet_name="Sheet1")

@pytest.fixture
def mock_iceframe():
    config = {"uri": "http://mock", "type": "rest", "token": "dummy"}
    with patch('iceframe.core.CatalogPool'), \
         patch('iceframe.core.TableOperations'), \
         patch('iceframe.core.DataExporter'):
        ice = IceFrame(config)
        ice._operations = MagicMock()
        return ice

def test_create_table_from_delta(mock_iceframe):
    with patch('iceframe.ingest.read_delta') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.create_table_from_delta("new_table", "delta_path")
        
        mock_read.assert_called_once_with("delta_path", version=None)
        mock_iceframe._operations.create_table.assert_called_once()
        mock_iceframe._operations.append_to_table.assert_called_once_with("new_table", mock_df, branch=None)

def test_create_table_from_excel(mock_iceframe):
    with patch('iceframe.ingest.read_excel') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.create_table_from_excel("new_table", "data.xlsx")
        
        mock_read.assert_called_once_with("data.xlsx", sheet_name="Sheet1")
        mock_iceframe._operations.create_table.assert_called_once()
        mock_iceframe._operations.append_to_table.assert_called_once_with("new_table", mock_df, branch=None)

def test_insert_from_file_optional(mock_iceframe):
    with patch('iceframe.ingest.read_delta') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.insert_from_file("test_table", "delta_path", format="delta")
        
        mock_read.assert_called_once_with("delta_path")
        mock_iceframe._operations.append_to_table.assert_called_once_with("test_table", mock_df, branch=None)
