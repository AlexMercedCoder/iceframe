import unittest
from unittest.mock import MagicMock, patch
import os

# Create a mock for FastMCP that acts as an identity decorator
mock_mcp_instance = MagicMock()
def identity_decorator():
    def wrapper(func):
        return func
    return wrapper
mock_mcp_instance.tool.side_effect = identity_decorator

# Patch FastMCP to return our mock instance
with patch("mcp.server.fastmcp.FastMCP", return_value=mock_mcp_instance):
    from iceframe.mcp_server import (
        list_tables, describe_table, get_table_stats, execute_query, generate_code,
        list_documentation, read_documentation, generate_sql
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
        mock_ice = MagicMock()
        mock_df = MagicMock()
        mock_df.height = 10
        mock_df.columns = ["col1"]
        mock_df.to_dicts.return_value = [{"col1": "val"}]
        mock_ice.read_table.return_value = mock_df
        mock_get_ice.return_value = mock_ice
        
        result = execute_query("test_table", query="col1 > 0", limit=5)
        
        mock_ice.read_table.assert_called_with("test_table", filter_expr="col1 > 0", limit=5)
        self.assertEqual(result["rows"], 10)

    def test_generate_code(self):
        result = generate_code("create table")
        self.assertIn("# Generated code for: create table", result)
        self.assertIn("from iceframe import IceFrame", result)

    def test_generate_sql(self):
        result = generate_sql("select all users")
        self.assertIn("-- Generated SQL for: select all users", result)
        self.assertIn("SELECT *", result)

    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.listdir")
    @patch("os.getcwd")
    def test_list_documentation(self, mock_getcwd, mock_listdir, mock_isdir, mock_exists):
        mock_getcwd.return_value = "/mock/cwd"
        mock_exists.return_value = True
        mock_isdir.return_value = True
        mock_listdir.return_value = ["doc1.md", "doc2.txt"]
        
        result = list_documentation()
        
        self.assertEqual(result, ["doc1.md"])

    @patch("builtins.open", new_callable=unittest.mock.mock_open, read_data="content")
    @patch("os.path.exists")
    @patch("os.path.isdir")
    @patch("os.getcwd")
    def test_read_documentation(self, mock_getcwd, mock_isdir, mock_exists, mock_open):
        mock_getcwd.return_value = "/mock/cwd"
        mock_exists.return_value = True
        mock_isdir.return_value = True
        
        result = read_documentation("doc1.md")
        
        self.assertEqual(result, "content")
        mock_open.assert_called()

if __name__ == "__main__":
    unittest.main()
