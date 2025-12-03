"""
IceFrame AI Agent - Natural language interface for Iceberg tables.
"""

from typing import List, Dict, Any, Optional
import json
from iceframe.core import IceFrame
from iceframe.agent.llm_base import BaseLLM, create_llm, LLMConfig
from iceframe.agent.tools import get_tool_definitions
from iceframe.expressions import Column

class IceFrameAgent:
    """
    AI Agent for natural language interaction with IceFrame.
    """
    
    def __init__(self, ice_frame: IceFrame, llm: Optional[BaseLLM] = None):
        """
        Initialize agent.
        
        Args:
            ice_frame: IceFrame instance
            llm: Optional LLM instance (auto-detects if None)
        """
        self.ice_frame = ice_frame
        self.llm = llm or create_llm()
        self.conversation_history: List[Dict[str, str]] = []
        self.tools = get_tool_definitions()
        
        # System prompt
        self.system_prompt = """You are an AI assistant for IceFrame, a Python library for Apache Iceberg tables.

Your role is to help users:
1. Explore their Iceberg tables (list tables, describe schemas, get statistics)
2. Query data using natural language
3. Generate Python code for complex operations
4. Optimize queries and suggest best practices
5. Troubleshoot issues

When users ask questions:
- Use the available tools to get information
- Provide clear, concise answers
- Generate executable Python code when appropriate
- Suggest optimizations (partitioning, column pruning, etc.)

Available tools:
- list_tables: List tables in a namespace
- describe_table: Get table schema and metadata
- get_table_stats: Get table statistics
- execute_query: Run queries on tables
- generate_code: Generate Python code for operations

Be helpful, accurate, and educational."""
        
    def chat(self, user_message: str) -> str:
        """
        Send a message to the agent.
        
        Args:
            user_message: User's message
            
        Returns:
            Agent's response
        """
        # Add user message to history
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })
        
        # Prepare messages with system prompt
        messages = [{"role": "system", "content": self.system_prompt}] + self.conversation_history
        
        # Get LLM response
        response = self.llm.chat(messages, tools=self.tools)
        
        # Handle tool calls
        if "tool_calls" in response:
            tool_results = []
            for tool_call in response["tool_calls"]:
                result = self._execute_tool(tool_call["name"], json.loads(tool_call["arguments"]))
                tool_results.append({
                    "tool_call_id": tool_call["id"],
                    "name": tool_call["name"],
                    "result": result
                })
            
            # Add tool results to conversation
            self.conversation_history.append({
                "role": "assistant",
                "content": response.get("content", ""),
                "tool_calls": response["tool_calls"]
            })
            
            for tr in tool_results:
                self.conversation_history.append({
                    "role": "tool",
                    "content": str(tr["result"]),
                    "tool_call_id": tr["tool_call_id"]
                })
            
            # Get final response
            messages = [{"role": "system", "content": self.system_prompt}] + self.conversation_history
            final_response = self.llm.chat(messages)
            assistant_message = final_response["content"]
        else:
            assistant_message = response["content"]
        
        # Add assistant response to history
        self.conversation_history.append({
            "role": "assistant",
            "content": assistant_message
        })
        
        return assistant_message
        
    def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Execute a tool and return the result"""
        try:
            if tool_name == "list_tables":
                namespace = arguments.get("namespace", "default")
                tables = self.ice_frame.list_tables(namespace)
                return {"tables": tables}
                
            elif tool_name == "describe_table":
                table_name = arguments["table_name"]
                table = self.ice_frame.get_table(table_name)
                schema = table.schema()
                return {
                    "columns": [
                        {
                            "name": f.name,
                            "type": str(f.field_type),
                            "required": f.required
                        }
                        for f in schema.fields
                    ],
                    "partition_spec": str(table.spec())
                }
                
            elif tool_name == "get_table_stats":
                table_name = arguments["table_name"]
                stats = self.ice_frame.stats(table_name)
                return stats
                
            elif tool_name == "execute_query":
                table_name = arguments["table_name"]
                limit = arguments.get("limit", 10)
                columns = arguments.get("columns")
                
                # Simple query execution
                df = self.ice_frame.read_table(table_name, limit=limit, columns=columns)
                
                # Return sample of data
                return {
                    "rows": df.height,
                    "columns": df.columns,
                    "sample": df.head(min(5, df.height)).to_dicts()
                }
                
            elif tool_name == "generate_code":
                operation = arguments["operation"]
                # Generate code template
                code = f"""# Generated code for: {operation}
from iceframe import IceFrame

ice = IceFrame(config)

# TODO: Implement {operation}
"""
                return {"code": code}
                
            else:
                return {"error": f"Unknown tool: {tool_name}"}
                
        except Exception as e:
            return {"error": str(e)}
            
    def reset_conversation(self):
        """Clear conversation history"""
        self.conversation_history = []
