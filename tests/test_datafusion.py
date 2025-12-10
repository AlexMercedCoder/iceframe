import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from iceframe.core import IceFrame
from iceframe.datafusion_ops import DataFusionManager

@pytest.fixture
def mock_iceframe():
    config = {"uri": "http://mock", "type": "rest", "token": "dummy"}
    with patch('iceframe.core.CatalogPool'), \
         patch('iceframe.core.TableOperations'), \
         patch('iceframe.core.DataExporter'):
        ice = IceFrame(config)
        ice._operations = MagicMock()
        return ice

def test_datafusion_manager_init(mock_iceframe):
    with patch('iceframe.datafusion_ops.DATAFUSION_AVAILABLE', True), \
         patch('iceframe.datafusion_ops.datafusion') as mock_df_mod:
        
        dfm = DataFusionManager(mock_iceframe)
        assert dfm.ice_frame == mock_iceframe
        mock_df_mod.SessionContext.assert_called_once()

def test_datafusion_register_table(mock_iceframe):
    with patch('iceframe.datafusion_ops.DATAFUSION_AVAILABLE', True), \
         patch('iceframe.datafusion_ops.datafusion'), \
         patch('iceframe.datafusion_ops.pa') as mock_pa:
        
        dfm = DataFusionManager(mock_iceframe)
        dfm.ctx = MagicMock()
        
        # Mock scan_batches
        mock_reader = MagicMock()
        mock_iceframe._operations.scan_batches.return_value = mock_reader
        
        # Mock Table.from_batches
        mock_arrow_table = MagicMock()
        mock_pa.Table.from_batches.return_value = mock_arrow_table
        
        dfm.register_table("test_table", alias="my_table")
        
        mock_iceframe._operations.scan_batches.assert_called_once_with("test_table")
        mock_pa.Table.from_batches.assert_called_once_with(mock_reader)
        dfm.ctx.register_table.assert_called_once_with("my_table", mock_arrow_table)

def test_datafusion_query(mock_iceframe):
    with patch('iceframe.datafusion_ops.DATAFUSION_AVAILABLE', True), \
         patch('iceframe.datafusion_ops.datafusion'):
        
        dfm = DataFusionManager(mock_iceframe)
        dfm.ctx = MagicMock()
        
        # Mock result
        mock_df_result = MagicMock()
        dfm.ctx.sql.return_value = mock_df_result
        mock_arrow = MagicMock()
        mock_df_result.to_arrow_table.return_value = mock_arrow
        
        with patch('iceframe.datafusion_ops.pl') as mock_pl:
            dfm.query("SELECT * FROM table")
            
            dfm.ctx.sql.assert_called_once_with("SELECT * FROM table")
            mock_df_result.to_arrow_table.assert_called_once()
            mock_pl.from_arrow.assert_called_once_with(mock_arrow)
