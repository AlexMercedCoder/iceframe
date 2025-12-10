import pytest
from unittest.mock import MagicMock, patch
import polars as pl
import pandas as pd
from iceframe.ingest import read_api, read_huggingface, read_html, read_clipboard, read_folder

def test_read_api_success():
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        mock_get.return_value = mock_response
        
        df = read_api("http://example.com/api/users")
        
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 2)
        assert df["name"][0] == "Alice"

def test_read_api_with_key():
    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"users": [{"id": 1, "name": "Alice"}]}
        mock_get.return_value = mock_response
        
        df = read_api("http://example.com/api/users", json_key="users")
        
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (1, 2)

def test_read_huggingface_success():
    import sys
    mock_datasets = MagicMock()
    mock_load = MagicMock()
    mock_datasets.load_dataset = mock_load
    
    # Mock the Arrow table conversion
    mock_ds = MagicMock()
    mock_arrow_table = MagicMock()
    mock_ds.data.table = mock_arrow_table
    mock_load.return_value = mock_ds

    with patch.dict(sys.modules, {'datasets': mock_datasets}):
        with patch('polars.from_arrow') as mock_from_arrow:
            mock_from_arrow.return_value = pl.DataFrame({"text": ["hello"]})
            
            df = read_huggingface("test/dataset")
            
            assert isinstance(df, pl.DataFrame)
            mock_load.assert_called_with("test/dataset", split="train")

def test_read_html_success():
    with patch('pandas.read_html') as mock_read_html:
        mock_read_html.return_value = [pd.DataFrame({"col1": [1, 2]})]
        
        df = read_html("http://example.com")
        
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 1)

def test_read_clipboard_success():
    with patch('pandas.read_clipboard') as mock_read_clipboard:
        mock_read_clipboard.return_value = pd.DataFrame({"col1": [1, 2]})
        
        df = read_clipboard()
        
        assert isinstance(df, pl.DataFrame)
        assert df.shape == (2, 1)

def test_read_folder_success(tmp_path):
    # Create dummy files
    d = tmp_path / "data"
    d.mkdir()
    
    df1 = pl.DataFrame({"a": [1, 2]})
    df2 = pl.DataFrame({"a": [3, 4]})
    
    df1.write_csv(d / "file1.csv")
    df2.write_csv(d / "file2.csv")
    
    df = read_folder(str(d), pattern="*.csv")
    
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (4, 1)
    assert df["a"].sum() == 10
