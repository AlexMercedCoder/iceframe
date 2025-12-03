"""
Google Gemini LLM provider for IceFrame AI Agent.
"""

from typing import List, Dict, Any, Optional
import json
from iceframe.agent.llm_base import BaseLLM, LLMConfig

class GeminiLLM(BaseLLM):
    """Google Gemini provider"""
    
    def __init__(self, config: LLMConfig):
        super().__init__(config)
        try:
            import google.generativeai as genai
            genai.configure(api_key=config.api_key)
            self.model = genai.GenerativeModel(config.model)
            self.genai = genai
        except ImportError:
            raise ImportError("google-generativeai package required. Install with: pip install 'iceframe[agent]'")
            
    def chat(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Send chat messages to Gemini"""
        # Convert messages to Gemini format
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] in ["user", "system"] else "model"
            gemini_messages.append({
                "role": role,
                "parts": [msg["content"]]
            })
        
        # Gemini tools format is different
        generation_config = {
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_tokens
        }
        
        kwargs = {"contents": gemini_messages, "generation_config": generation_config}
        
        if tools:
            # Convert tools to Gemini function declarations
            gemini_tools = []
            for tool in tools:
                if tool["type"] == "function":
                    func = tool["function"]
                    gemini_tools.append(
                        self.genai.protos.Tool(
                            function_declarations=[
                                self.genai.protos.FunctionDeclaration(
                                    name=func["name"],
                                    description=func.get("description", ""),
                                    parameters=func.get("parameters", {})
                                )
                            ]
                        )
                    )
            if gemini_tools:
                kwargs["tools"] = gemini_tools
        
        response = self.model.generate_content(**kwargs)
        
        result = {"content": ""}
        
        if response.text:
            result["content"] = response.text
            
        # Check for function calls
        if hasattr(response, "candidates") and response.candidates:
            for part in response.candidates[0].content.parts:
                if hasattr(part, "function_call") and part.function_call:
                    if "tool_calls" not in result:
                        result["tool_calls"] = []
                    result["tool_calls"].append({
                        "id": f"call_{len(result.get('tool_calls', []))}",
                        "name": part.function_call.name,
                        "arguments": json.dumps(dict(part.function_call.args))
                    })
                    
        return result
        
    def stream_chat(self, messages: List[Dict[str, str]]):
        """Stream chat responses from Gemini"""
        gemini_messages = []
        for msg in messages:
            role = "user" if msg["role"] in ["user", "system"] else "model"
            gemini_messages.append({
                "role": role,
                "parts": [msg["content"]]
            })
        
        generation_config = {
            "temperature": self.config.temperature,
            "max_output_tokens": self.config.max_tokens
        }
        
        response = self.model.generate_content(
            contents=gemini_messages,
            generation_config=generation_config,
            stream=True
        )
        
        for chunk in response:
            if chunk.text:
                yield chunk.text
