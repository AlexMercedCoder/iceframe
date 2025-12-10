import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from iceframe.quality import DataValidator

@pytest.fixture
def mock_iceframe():
    ice = MagicMock()
    ice.query_datafusion.return_value = pl.DataFrame({"id": [1, 2]})
    return ice

def test_resolve_dataframe():
    validator = DataValidator()
    df = pl.DataFrame({"id": [1]})
    assert validator._resolve_data(df) is df

def test_resolve_query_builder():
    validator = DataValidator()
    mock_qb = MagicMock()
    mock_qb.execute.return_value = pl.DataFrame({"id": [1]})
    
    assert validator._resolve_data(mock_qb).height == 1
    mock_qb.execute.assert_called_once()

def test_resolve_sql_string(mock_iceframe):
    validator = DataValidator(mock_iceframe)
    
    df = validator._resolve_data("SELECT * FROM table")
    assert df.height == 2
    mock_iceframe.query_datafusion.assert_called_once_with("SELECT * FROM table")

def test_resolve_sql_string_no_iceframe():
    validator = DataValidator()
    with pytest.raises(ValueError, match="IceFrame instance required"):
        validator._resolve_data("SELECT * FROM table")

def test_expect_unique_with_sql(mock_iceframe):
    validator = DataValidator(mock_iceframe)
    
    # Mock query returning unique data
    mock_iceframe.query_datafusion.return_value = pl.DataFrame({"id": [1, 2]})
    assert validator.expect_column_values_to_be_unique("SELECT * FROM table", "id")
