"""Agent package for IceFrame AI capabilities"""

from iceframe.agent.llm_base import BaseLLM, LLMConfig, create_llm, detect_llm_from_env

__all__ = ["BaseLLM", "LLMConfig", "create_llm", "detect_llm_from_env"]
