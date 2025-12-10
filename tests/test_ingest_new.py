import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from iceframe.ingest import read_sql, read_xml, read_sas, read_spss, read_stata
from iceframe.core import IceFrame

@pytest.fixture
def mock_polars():
    with patch('iceframe.ingest.pl') as mock_pl:
        yield mock_pl

@pytest.fixture
def mock_pandas():
    with patch('iceframe.ingest.pd') as mock_pd:
        yield mock_pd

def test_read_sql(mock_polars):
    read_sql("SELECT * FROM table", "postgres://uri")
    mock_polars.read_database_uri.assert_called_once_with("SELECT * FROM table", "postgres://uri")

def test_read_xml(mock_polars):
    # Patch pandas.read_xml directly
    with patch('pandas.read_xml') as mock_read_xml:
        mock_df_pd = MagicMock()
        mock_read_xml.return_value = mock_df_pd
        
        read_xml("test.xml")
        
        mock_read_xml.assert_called_once_with("test.xml")
        mock_polars.from_pandas.assert_called_once_with(mock_df_pd)

def test_read_sas(mock_polars):
    with patch('pandas.read_sas') as mock_read_sas:
        mock_df_pd = MagicMock()
        mock_read_sas.return_value = mock_df_pd
        
        read_sas("test.sas7bdat")
        
        mock_read_sas.assert_called_once_with("test.sas7bdat", format='sas7bdat')
        mock_polars.from_pandas.assert_called_once_with(mock_df_pd)

@pytest.fixture
def mock_iceframe():
    config = {"uri": "http://mock", "type": "rest", "token": "dummy"}
    with patch('iceframe.core.CatalogPool'), \
         patch('iceframe.core.TableOperations'), \
         patch('iceframe.core.DataExporter'):
        ice = IceFrame(config)
        ice._operations = MagicMock()
        return ice

def test_create_table_from_sql(mock_iceframe):
    with patch('iceframe.ingest.read_sql') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.create_table_from_sql("new_table", "SELECT *", "uri")
        
        mock_read.assert_called_once_with("SELECT *", "uri")
        mock_iceframe._operations.create_table.assert_called_once()
        mock_iceframe._operations.append_to_table.assert_called_once_with("new_table", mock_df, branch=None)

def test_insert_from_file_xml(mock_iceframe):
    with patch('iceframe.ingest.read_xml') as mock_read:
        mock_df = MagicMock()
        mock_read.return_value = mock_df
        
        mock_iceframe.insert_from_file("test_table", "data.xml", format="xml")
        
        mock_read.assert_called_once_with("data.xml")
        mock_iceframe._operations.append_to_table.assert_called_once_with("test_table", mock_df, branch=None)
