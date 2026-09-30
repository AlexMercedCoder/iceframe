from unittest.mock import MagicMock

import pytest

from iceframe.streaming import StreamingWriter


@pytest.fixture
def mock_iceframe():
    ice = MagicMock()
    return ice


def test_streaming_auto_compaction(mock_iceframe):
    writer = StreamingWriter(mock_iceframe, "test_table", batch_size=1)
    writer.enable_auto_compaction(every_n_flushes=2)

    # Write 1st record -> flush 1
    writer.write({"a": 1})
    assert writer._flushes_since_compact == 1
    mock_iceframe.compact_data_files.assert_not_called()

    # Write 2nd record -> flush 2 -> compaction
    writer.write({"a": 2})
    assert writer._flushes_since_compact == 0
    mock_iceframe.compact_data_files.assert_called_once_with("test_table")

    # Write 3rd record -> flush 1
    writer.write({"a": 3})
    assert writer._flushes_since_compact == 1
    mock_iceframe.compact_data_files.assert_called_once()  # Still called once


def test_streaming_compaction_failure_is_visible(mock_iceframe):
    mock_iceframe.compact_data_files.side_effect = RuntimeError("failed")
    writer = StreamingWriter(mock_iceframe, "test_table", batch_size=1)
    writer.enable_auto_compaction(every_n_flushes=1)
    with pytest.raises(RuntimeError, match="Auto-compaction failed"):
        writer.write({"a": 1})
