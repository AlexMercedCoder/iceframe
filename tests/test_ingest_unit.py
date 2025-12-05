import unittest
from unittest.mock import MagicMock, patch
import sys
import polars as pl

# Mock external libraries
sys.modules["deltalake"] = MagicMock()
sys.modules["lance"] = MagicMock()
sys.modules["vortex"] = MagicMock()
sys.modules["fastexcel"] = MagicMock()
sys.modules["gspread"] = MagicMock()
sys.modules["daft"] = MagicMock()

from iceframe.ingest import (
    read_delta, read_lance, read_vortex, read_excel, read_gsheets, read_hudi,
    read_csv, read_json, read_parquet, read_ipc, read_avro
)
from iceframe.core import IceFrame

class TestIngest(unittest.TestCase):
    
    @patch("polars.read_delta")
    def test_read_delta(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_delta("path/to/delta", version=1)
        mock_read.assert_called_with("path/to/delta", version=1)
        self.assertIsInstance(df, pl.DataFrame)

    def test_read_lance(self):
        with patch("lance.dataset") as mock_ds:
            mock_ds.return_value.to_table.return_value = MagicMock() # Arrow table
            with patch("polars.from_arrow") as mock_from_arrow:
                mock_from_arrow.return_value = pl.DataFrame({"a": [1]})
                
                df = read_lance("path/to/lance")
                mock_ds.assert_called_with("path/to/lance")
                self.assertIsInstance(df, pl.DataFrame)

    def test_read_vortex(self):
        with patch("vortex.open") as mock_open:
            mock_scan = mock_open.return_value.scan.return_value
            mock_read_all = mock_scan.read_all.return_value
            mock_read_all.to_arrow.return_value = MagicMock()
            
            with patch("polars.from_arrow") as mock_from_arrow:
                mock_from_arrow.return_value = pl.DataFrame({"a": [1]})
                
                df = read_vortex("path/to/vortex")
                mock_open.assert_called_with("path/to/vortex")
                self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_excel")
    def test_read_excel(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_excel("path/to/excel", sheet_name="Sheet1")
        mock_read.assert_called_with("path/to/excel", sheet_name="Sheet1")
        self.assertIsInstance(df, pl.DataFrame)

    def test_read_gsheets(self):
        with patch("gspread.service_account") as mock_sa:
            mock_gc = mock_sa.return_value
            mock_sh = mock_gc.open_by_url.return_value
            mock_ws = mock_sh.sheet1
            mock_ws.get_all_records.return_value = [{"a": 1}]
            
            df = read_gsheets("http://sheets", credentials="creds.json")
            mock_sa.assert_called_with(filename="creds.json")
            mock_gc.open_by_url.assert_called_with("http://sheets")
            self.assertIsInstance(df, pl.DataFrame)

    def test_read_hudi(self):
        with patch("daft.read_hudi") as mock_read:
            mock_read.return_value.to_arrow.return_value = MagicMock()
            with patch("polars.from_arrow") as mock_from_arrow:
                mock_from_arrow.return_value = pl.DataFrame({"a": [1]})
                
                df = read_hudi("path/to/hudi")
                mock_read.assert_called_with("path/to/hudi")
                self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_csv")
    def test_read_csv(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_csv("path/to/csv")
        mock_read.assert_called_with("path/to/csv")
        self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_json")
    def test_read_json(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_json("path/to/json")
        mock_read.assert_called_with("path/to/json")
        self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_parquet")
    def test_read_parquet(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_parquet("path/to/parquet")
        mock_read.assert_called_with("path/to/parquet")
        self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_ipc")
    def test_read_ipc(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_ipc("path/to/ipc")
        mock_read.assert_called_with("path/to/ipc")
        self.assertIsInstance(df, pl.DataFrame)

    @patch("polars.read_avro")
    def test_read_avro(self, mock_read):
        mock_read.return_value = pl.DataFrame({"a": [1]})
        df = read_avro("path/to/avro")
        mock_read.assert_called_with("path/to/avro")
        self.assertIsInstance(df, pl.DataFrame)

class TestIceFrameIngest(unittest.TestCase):
    
    def setUp(self):
        self.config = {"uri": "http://mock", "type": "rest", "token": "dummy"}
        with patch("iceframe.core.CatalogPool"), \
             patch("iceframe.core.TableOperations"):
            self.ice = IceFrame(self.config)
            self.ice.create_table = MagicMock()
            self.ice.append_to_table = MagicMock()
            
    @patch("iceframe.ingest.read_delta")
    def test_create_from_delta(self, mock_read):
        df = pl.DataFrame({"a": [1]})
        mock_read.return_value = df
        self.ice.create_table.return_value = MagicMock()
        
        self.ice.create_table_from_delta("table", "path")
        
        mock_read.assert_called_with("path", version=None)
        self.ice.create_table.assert_called_with("table", schema=df)
        self.ice.append_to_table.assert_called_with("table", df)

if __name__ == "__main__":
    unittest.main()
