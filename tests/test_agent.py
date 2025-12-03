"""
Unit tests for AI Agent
"""

import pytest
from unittest.mock import Mock, MagicMock
from iceframe.agent.core import IceFrameAgent
from iceframe.agent.llm_base import LLMConfig

@pytest.fixture
def mock_ice_frame():
    """Mock IceFrame instance"""
    mock = Mock()
    mock.list_tables.return_value = ["users", "orders"]
    
    # Create mock table with proper spec
    mock_table = Mock()
    mock_table.schema.return_value = Mock(
        fields=[
            Mock(name="id", field_type="long", required=True),
            Mock(name="name", field_type="string", required=False)
        ]
    )
    mock_table.spec.return_value = "PartitionSpec()"
    
    mock.get_table.return_value = mock_table
    mock.stats.return_value = {
        "table_name": "users",
        "schema": {"fields": 2}
    }
    return mock

@pytest.fixture
def mock_llm():
    """Mock LLM"""
    llm = Mock()
    llm.config = LLMConfig(provider="openai", model="gpt-4")
    llm.chat.return_value = {"content": "This is a test response"}
    return llm

def test_agent_initialization(mock_ice_frame, mock_llm):
    """Test agent initialization"""
    agent = IceFrameAgent(mock_ice_frame, llm=mock_llm)
    assert agent.ice_frame == mock_ice_frame
    assert agent.llm == mock_llm
    assert len(agent.conversation_history) == 0

def test_agent_chat(mock_ice_frame, mock_llm):
    """Test basic chat"""
    agent = IceFrameAgent(mock_ice_frame, llm=mock_llm)
    response = agent.chat("Hello")
    
    assert response == "This is a test response"
    assert len(agent.conversation_history) == 2  # user + assistant

def test_agent_tool_execution(mock_ice_frame, mock_llm):
    """Test tool execution"""
    agent = IceFrameAgent(mock_ice_frame, llm=mock_llm)
    
    # Test list_tables
    result = agent._execute_tool("list_tables", {"namespace": "default"})
    assert "tables" in result
    assert result["tables"] == ["users", "orders"]
    
    # Test describe_table
    result = agent._execute_tool("describe_table", {"table_name": "users"})
    assert "columns" in result
    assert len(result["columns"]) == 2

def test_agent_reset_conversation(mock_ice_frame, mock_llm):
    """Test conversation reset"""
    agent = IceFrameAgent(mock_ice_frame, llm=mock_llm)
    agent.chat("Hello")
    assert len(agent.conversation_history) > 0
    
    agent.reset_conversation()
    assert len(agent.conversation_history) == 0
