import pytest
from unittest.mock import MagicMock, patch
import polars as pl
import pyarrow as pa
from iceframe.visualization import Visualizer

@pytest.fixture
def mock_iceframe():
    ice = MagicMock()
    ice._operations = MagicMock()
    return ice

def test_visualizer_init():
    with patch('iceframe.visualization.VIZ_AVAILABLE', True):
        viz = Visualizer(MagicMock())
        assert viz.ice_frame is not None

def test_visualizer_get_data(mock_iceframe):
    with patch('iceframe.visualization.VIZ_AVAILABLE', True):
        viz = Visualizer(mock_iceframe)
        
        # Mock scan_batches
        batch = pa.RecordBatch.from_pydict({"a": [1, 2], "b": [3, 4]})
        mock_iceframe._operations.scan_batches.return_value = iter([batch])
        
        df = viz._get_data("test_table")
        
        assert isinstance(df, pl.DataFrame)
        assert df.height == 2
        mock_iceframe._operations.scan_batches.assert_called_once()

def test_visualizer_plots(mock_iceframe):
    with patch('iceframe.visualization.VIZ_AVAILABLE', True), \
         patch('iceframe.visualization.alt') as mock_alt:
        
        viz = Visualizer(mock_iceframe)
        
        # Mock _get_data
        viz._get_data = MagicMock(return_value=pl.DataFrame({"a": [1, 2], "b": [3, 4]}))
        
        # Mock Altair chart
        mock_chart = MagicMock()
        mock_alt.Chart.return_value = mock_chart
        mock_chart.mark_bar.return_value = mock_chart
        mock_chart.mark_circle.return_value = mock_chart
        mock_chart.mark_line.return_value = mock_chart
        mock_chart.encode.return_value = mock_chart
        mock_chart.properties.return_value = mock_chart
        
        # Test plot_distribution
        viz.plot_distribution("test_table", "a")
        mock_alt.Chart.assert_called()
        mock_chart.mark_bar.assert_called()
        
        # Test plot_scatter
        viz.plot_scatter("test_table", "a", "b")
        mock_chart.mark_circle.assert_called()
        
        # Test plot_line
        viz.plot_line("test_table", "a", "b")
        mock_chart.mark_line.assert_called()
