import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from iceframe.ingest import read_csv, read_json, read_parquet, read_ipc, read_avro, read_orc
from iceframe.core import IceFrame

@pytest.fixture
def mock_polars():
    with patch('iceframe.ingest.pl') as mock_pl:
        yield mock_pl

def test_read_csv(mock_polars):
    read_csv("test.csv")
    mock_polars.read_csv.assert_called_once_with("test.csv")

def test_read_json(mock_polars):
    read_json("test.json")
    mock_polars.read_json.assert_called_once_with("test.json")

def test_read_parquet(mock_polars):
    read_parquet("test.parquet")
    mock_polars.read_parquet.assert_called_once_with("test.parquet")

def test_read_ipc(mock_polars):
    read_ipc("test.ipc")
    mock_polars.read_ipc.assert_called_once_with("test.ipc")

def test_read_avro(mock_polars):
    read_avro("test.avro")
    mock_polars.read_avro.assert_called_once_with("test.avro")

def test_read_orc(mock_polars):
    read_orc("test.orc")
    mock_polars.read_orc.assert_called_once_with("test.orc")

@pytest.fixture
def mock_iceframe():
    config = {"uri": "http://mock", "type": "rest", "token": "dummy_token"}
    with patch('iceframe.core.CatalogPool'), \
         patch('iceframe.core.TableOperations'), \
         patch('iceframe.core.DataExporter'):
        ice = IceFrame(config)
        ice._operations = MagicMock()
        return ice

def test_insert_from_file_csv(mock_iceframe):
    with patch('iceframe.ingest.read_csv') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.insert_from_file("test_table", "data.csv")
        
        mock_read.assert_called_once_with("data.csv")
        mock_iceframe._operations.append_to_table.assert_called_once_with("test_table", mock_df, branch=None)

def test_insert_from_file_inferred(mock_iceframe):
    with patch('iceframe.ingest.read_parquet') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.insert_from_file("test_table", "data.parquet")
        
        mock_read.assert_called_once_with("data.parquet")
        mock_iceframe._operations.append_to_table.assert_called_once_with("test_table", mock_df, branch=None)

def test_create_table_from_orc(mock_iceframe):
    with patch('iceframe.ingest.read_orc') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.create_table_from_orc("new_table", "data.orc")
        
        mock_read.assert_called_once_with("data.orc")
        mock_iceframe._operations.create_table.assert_called_once()
        mock_iceframe._operations.append_to_table.assert_called_once_with("new_table", mock_df, branch=None)
