"""
Base LLM abstraction for IceFrame AI Agent.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import os

@dataclass
class LLMConfig:
    """Configuration for LLM providers"""
    provider: str  # "openai", "anthropic", "gemini"
    model: str
    api_key: Optional[str] = None
    temperature: float = 0.7
    max_tokens: int = 2000

class BaseLLM(ABC):
    """Abstract base class for LLM providers"""
    
    def __init__(self, config: LLMConfig):
        self.config = config
        
    @abstractmethod
    def chat(self, messages: List[Dict[str, str]], tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Send chat messages to LLM.
        
        Args:
            messages: List of message dicts with 'role' and 'content'
            tools: Optional list of tool definitions for function calling
            
        Returns:
            Response dict with 'content' and optional 'tool_calls'
        """
        pass
        
    @abstractmethod
    def stream_chat(self, messages: List[Dict[str, str]]):
        """Stream chat responses"""
        pass

def detect_llm_from_env() -> LLMConfig:
    """
    Auto-detect LLM provider from environment variables.
    
    Priority: ICEFRAME_LLM_PROVIDER > API keys detection
    
    Environment variables:
        ICEFRAME_LLM_PROVIDER: "openai", "anthropic", or "gemini"
        ICEFRAME_LLM_MODEL: Model name (optional, uses defaults)
        OPENAI_API_KEY: OpenAI API key
        ANTHROPIC_API_KEY: Anthropic API key
        GOOGLE_API_KEY or GEMINI_API_KEY: Google Gemini API key
    """
    provider = os.getenv("ICEFRAME_LLM_PROVIDER", "").lower()
    model = os.getenv("ICEFRAME_LLM_MODEL")
    
    # If provider explicitly set
    if provider in ["openai", "anthropic", "gemini"]:
        if provider == "openai":
            api_key = os.getenv("OPENAI_API_KEY")
            default_model = "gpt-4"
        elif provider == "anthropic":
            api_key = os.getenv("ANTHROPIC_API_KEY")
            default_model = "claude-3-5-sonnet-20241022"
        else:  # gemini
            api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
            default_model = "gemini-2.0-flash-exp"
            
        return LLMConfig(
            provider=provider,
            model=model or default_model,
            api_key=api_key
        )
    
    # Auto-detect from API keys
    if os.getenv("OPENAI_API_KEY"):
        return LLMConfig(
            provider="openai",
            model=model or "gpt-4",
            api_key=os.getenv("OPENAI_API_KEY")
        )
    elif os.getenv("ANTHROPIC_API_KEY"):
        return LLMConfig(
            provider="anthropic",
            model=model or "claude-3-5-sonnet-20241022",
            api_key=os.getenv("ANTHROPIC_API_KEY")
        )
    elif os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY"):
        return LLMConfig(
            provider="gemini",
            model=model or "gemini-2.0-flash-exp",
            api_key=os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
        )
    else:
        raise ValueError(
            "No LLM API key found. Set one of: OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY"
        )

def create_llm(config: Optional[LLMConfig] = None) -> BaseLLM:
    """
    Create LLM instance from config or auto-detect.
    
    Args:
        config: Optional LLMConfig, if None will auto-detect
        
    Returns:
        BaseLLM instance
    """
    if config is None:
        config = detect_llm_from_env()
        
    if config.provider == "openai":
        from iceframe.agent.llm_openai import OpenAILLM
        return OpenAILLM(config)
    elif config.provider == "anthropic":
        from iceframe.agent.llm_anthropic import AnthropicLLM
        return AnthropicLLM(config)
    elif config.provider == "gemini":
        from iceframe.agent.llm_gemini import GeminiLLM
        return GeminiLLM(config)
    else:
        raise ValueError(f"Unknown LLM provider: {config.provider}")
