"""
Tool definitions for IceFrame AI Agent.
"""

from typing import List, Dict, Any
import json

def get_tool_definitions() -> List[Dict[str, Any]]:
    """
    Get tool definitions for LLM function calling.
    
    Returns:
        List of tool definitions in OpenAI format
    """
    return [
        {
            "type": "function",
            "function": {
                "name": "list_tables",
                "description": "List all tables in a namespace",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "namespace": {
                            "type": "string",
                            "description": "Namespace to list tables from (default: 'default')"
                        }
                    }
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "describe_table",
                "description": "Get schema and metadata for a table",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the table to describe"
                        }
                    },
                    "required": ["table_name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "get_table_stats",
                "description": "Get statistics for a table",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the table"
                        }
                    },
                    "required": ["table_name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "execute_query",
                "description": "Execute a query on a table and return results",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "table_name": {
                            "type": "string",
                            "description": "Name of the table to query"
                        },
                        "filter_condition": {
                            "type": "string",
                            "description": "Optional filter condition (e.g., 'age > 30')"
                        },
                        "columns": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Columns to select (optional)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of rows to return"
                        }
                    },
                    "required": ["table_name"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "generate_code",
                "description": "Generate Python code for a complex operation",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "operation": {
                            "type": "string",
                            "description": "Description of the operation to generate code for"
                        }
                    },
                    "required": ["operation"]
                }
            }
        }
    ]
