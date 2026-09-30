import os
import unittest
from unittest.mock import MagicMock, patch

from iceframe.mcp_server import (
    describe_table,
    execute_query,
    generate_code,
    generate_sql,
    get_table_stats,
    list_documentation,
    list_tables,
    read_documentation,
)


class TestMCPServer(unittest.TestCase):
    def setUp(self):
        # Mock environment variables
        self.env_patcher = patch.dict(os.environ, {"ICEBERG_CATALOG_URI": "http://mock"})
        self.env_patcher.start()

    def tearDown(self):
        self.env_patcher.stop()

    @patch("iceframe.mcp_server.get_iceframe")
    def test_list_tables(self, mock_get_ice):
        mock_ice = MagicMock()
        mock_ice.list_tables.return_value = ["table1", "table2"]
        mock_get_ice.return_value = mock_ice

        result = list_tables(namespace="test_ns")

        mock_ice.list_tables.assert_called_with("test_ns")
        self.assertEqual(result, ["table1", "table2"])

    @patch("iceframe.mcp_server.get_iceframe")
    def test_describe_table(self, mock_get_ice):
        mock_ice = MagicMock()
        mock_table = MagicMock()
        mock_field = MagicMock()
        mock_field.name = "col1"
        mock_field.field_type = "string"
        mock_field.required = True
        mock_table.schema.return_value.fields = [mock_field]
        mock_table.spec.return_value = "partition_spec"
        mock_table.properties = {"prop": "val"}

        mock_ice.get_table.return_value = mock_table
        mock_get_ice.return_value = mock_ice

        result = describe_table("test_table")

        mock_ice.get_table.assert_called_with("test_table")
        self.assertEqual(result["columns"][0]["name"], "col1")
        self.assertEqual(result["partition_spec"], "partition_spec")

    @patch("iceframe.mcp_server.get_iceframe")
    def test_get_table_stats(self, mock_get_ice):
        mock_ice = MagicMock()
        mock_ice.stats.return_value = {"rows": 100}
        mock_get_ice.return_value = mock_ice

        result = get_table_stats("test_table")

        mock_ice.stats.assert_called_with("test_table")
        self.assertEqual(result, {"rows": 100})

    @patch("iceframe.mcp_server.get_iceframe")
    def test_execute_query(self, mock_get_ice):
        import polars as pl

        mock_ice = MagicMock()
        # A real DataFrame: 0.13.0 caps results by row count AND estimated
        # bytes, so the response has to be a genuine frame.
        mock_ice.read_table.return_value = pl.DataFrame({"col1": ["val"] * 5})
        mock_get_ice.return_value = mock_ice

        result = execute_query("test_table", query="col1 > 0", limit=5)

        mock_ice.read_table.assert_called_with(
            "test_table", filter_sql="col1 > 0", limit=5, columns=None
        )
        self.assertEqual(result["rows"], 5)
        self.assertEqual(result["columns"], ["col1"])
        self.assertFalse(result["truncated"])
        self.assertTrue(result["read_only"])

    def test_generate_code(self):
        result = generate_code("create table")
        self.assertIn("# Generated code for: create table", result)
        self.assertIn("from iceframe import IceFrame", result)

    def test_generate_sql(self):
        result = generate_sql("select all users")
        self.assertIn("-- Generated SQL for: select all users", result)
        self.assertIn("SELECT *", result)

    def test_list_documentation(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as root:
            docs = Path(root)
            (docs / "doc1.md").write_text("content")
            (docs / "doc2.txt").write_text("ignored")
            with patch("iceframe.mcp_server._documentation_roots", return_value=[docs]):
                self.assertEqual(list_documentation(), ["doc1.md"])

    def test_read_documentation(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as root:
            docs = Path(root)
            (docs / "doc1.md").write_text("content")
            with patch("iceframe.mcp_server._documentation_roots", return_value=[docs]):
                self.assertEqual(read_documentation("doc1.md"), "content")


if __name__ == "__main__":
    unittest.main()
