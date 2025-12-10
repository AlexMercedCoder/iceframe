import pytest
from unittest.mock import MagicMock, patch
from iceframe.distributed import RayExecutor

def test_ray_executor_init():
    with patch('iceframe.distributed.RAY_AVAILABLE', True), \
         patch('iceframe.distributed.ray') as mock_ray:
        
        mock_ray.is_initialized.return_value = False
        
        executor = RayExecutor()
        mock_ray.init.assert_called_once()

def test_ray_executor_map():
    with patch('iceframe.distributed.RAY_AVAILABLE', True), \
         patch('iceframe.distributed.ray') as mock_ray:
        
        executor = RayExecutor()
        
        # Mock remote function
        mock_remote_func = MagicMock()
        mock_ray.remote.return_value = mock_remote_func
        mock_remote_func.remote.side_effect = lambda x, **k: f"future_{x}"
        
        # Mock ray.get
        mock_ray.get.return_value = [2, 4, 6]
        
        def double(x): return x * 2
        
        results = executor.map(double, [1, 2, 3])
        
        assert results == [2, 4, 6]
        mock_ray.get.assert_called_once()

def test_ray_read_tables_parallel():
    with patch('iceframe.distributed.RAY_AVAILABLE', True), \
         patch('iceframe.distributed.ray') as mock_ray:
        
        executor = RayExecutor()
        
        # Mock remote task
        mock_remote_task = MagicMock()
        mock_ray.remote.return_value = mock_remote_task
        mock_remote_task.remote.return_value = "future"
        
        mock_ray.get.return_value = "dataframe"
        
        results = executor.read_tables_parallel({}, ["table1"])
        
        assert results == {"table1": "dataframe"}
