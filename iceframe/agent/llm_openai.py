"""
OpenAI LLM provider for IceFrame AI Agent.
"""

from collections.abc import Iterator
from typing import Any

from iceframe.agent.llm_base import BaseLLM, LLMConfig


class OpenAILLM(BaseLLM):
    """OpenAI GPT provider"""

    def __init__(self, config: LLMConfig):
        super().__init__(config)
        self.client: Any
        try:
            from openai import OpenAI

            self.client = OpenAI(api_key=config.api_key)
        except ImportError:
            raise ImportError(
                "openai package required. Install with: pip install 'iceframe[agent]'"
            ) from None

    def chat(
        self, messages: list[dict[str, str]], tools: list[dict] | None = None
    ) -> dict[str, Any]:
        """Send chat messages to OpenAI"""
        kwargs: dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
        }

        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        response = self.client.chat.completions.create(**kwargs)
        message = response.choices[0].message

        result = {"content": message.content or ""}

        if hasattr(message, "tool_calls") and message.tool_calls:
            result["tool_calls"] = [
                {"id": tc.id, "name": tc.function.name, "arguments": tc.function.arguments}
                for tc in message.tool_calls
            ]

        return result

    def stream_chat(self, messages: list[dict[str, str]]) -> Iterator[str]:
        """Stream chat responses from OpenAI"""
        stream = self.client.chat.completions.create(
            model=self.config.model,
            messages=messages,
            temperature=self.config.temperature,
            max_tokens=self.config.max_tokens,
            stream=True,
        )

        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
