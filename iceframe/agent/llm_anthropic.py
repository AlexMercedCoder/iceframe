"""
Anthropic Claude LLM provider for IceFrame AI Agent.
"""

from typing import List, Dict, Any, Optional
import json
from iceframe.agent.llm_base import BaseLLM, LLMConfig

class AnthropicLLM(BaseLLM):
    """Anthropic Claude provider"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=config.api_key)
        except ImportError:
            raise ImportError("anthropic package required. Install with: pip install 'iceframe[agent]'")
            
    def chat(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Send chat messages to Anthropic"""
        # Anthropic expects system message separately
        system_msg = None
        user_messages = []
        
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                user_messages.append(msg)
        
        kwargs = {
            "model": self.config.model,
            "messages": user_messages,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature
        }
        
        if system_msg:
            kwargs["system"] = system_msg
            
        if tools:
            kwargs["tools"] = tools
            
        response = self.client.messages.create(**kwargs)
        
        result = {"content": ""}
        
        for block in response.content:
            if block.type == "text":
                result["content"] += block.text
            elif block.type == "tool_use":
                if "tool_calls" not in result:
                    result["tool_calls"] = []
                result["tool_calls"].append({
                    "id": block.id,
                    "name": block.name,
                    "arguments": json.dumps(block.input)
                })
                
        return result
        
    def stream_chat(self, messages: List[Dict[str, str]]):
        """Stream chat responses from Anthropic"""
        system_msg = None
        user_messages = []
        
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                user_messages.append(msg)
        
        kwargs = {
            "model": self.config.model,
            "messages": user_messages,
            "max_tokens": self.config.max_tokens,
            "temperature": self.config.temperature,
            "stream": True
        }
        
        if system_msg:
            kwargs["system"] = system_msg
            
        with self.client.messages.stream(**kwargs) as stream:
            for text in stream.text_stream:
                yield text
