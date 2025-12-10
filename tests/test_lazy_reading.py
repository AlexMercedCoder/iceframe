import pytest
from unittest.mock import MagicMock, patch
import polars as pl
import pyarrow as pa
from iceframe.core import IceFrame
from iceframe.memory import MemoryManager

@pytest.fixture
def mock_iceframe():
    config = {"uri": "http://mock", "type": "rest", "token": "dummy"}
    with patch('iceframe.core.CatalogPool'), \
         patch('iceframe.core.TableOperations'), \
         patch('iceframe.core.DataExporter'):
        ice = IceFrame(config)
        ice._operations = MagicMock()
        return ice

def test_scan_batches(mock_iceframe):
    # Mock the table scan and batch reader
    mock_table = MagicMock()
    mock_scan = MagicMock()
    mock_reader = MagicMock()
    
    mock_iceframe._operations.get_table.return_value = mock_table
    mock_table.scan.return_value = mock_scan
    mock_scan.to_arrow_batch_reader.return_value = mock_reader
    
    # Call scan_batches (we need to call the real method on operations, but we mocked operations)
    # So we should test operations.scan_batches directly or unmock operations
    
    # Let's test operations.scan_batches
    from iceframe.operations import TableOperations
    ops = TableOperations(MagicMock())
    ops.get_table = MagicMock(return_value=mock_table)
    
    result = ops.scan_batches("test_table")
    
    mock_table.scan.assert_called_once()
    mock_scan.to_arrow_batch_reader.assert_called_once()
    assert result == mock_reader

def test_read_table_chunked(mock_iceframe):
    # Mock scan_batches to return an iterator of batches
    batch1 = pa.RecordBatch.from_pydict({"a": [1, 2]})
    batch2 = pa.RecordBatch.from_pydict({"a": [3, 4]})
    mock_iceframe._operations.scan_batches.return_value = iter([batch1, batch2])
    
    mem_manager = MemoryManager()
    chunks = list(mem_manager.read_table_chunked(mock_iceframe, "test_table"))
    
    assert len(chunks) == 2
    assert isinstance(chunks[0], pl.DataFrame)
    assert chunks[0].height == 2
    assert chunks[1].height == 2
    mock_iceframe._operations.scan_batches.assert_called_once_with("test_table", columns=None)
