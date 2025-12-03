# AI Agent

IceFrame includes an AI agent that provides a natural language interface for interacting with your Iceberg tables.

## Features

- **Natural Language Queries**: Ask questions in plain English
- **Schema Discovery**: Explore tables and understand data structures
- **Code Generation**: Get Python code for complex operations
- **Query Optimization**: Receive suggestions for better performance
- **Multiple LLM Support**: Works with OpenAI, Anthropic Claude, or Google Gemini

## Setup

### 1. Install Dependencies

```bash
pip install "iceframe[agent]"
```

This installs: `openai`, `anthropic`, `google-generativeai`, and `rich`.

### 2. Configure LLM

Set one of these environment variables:

```bash
# OpenAI (GPT-4, GPT-3.5-turbo)
export OPENAI_API_KEY="your-key"

# Anthropic (Claude)
export ANTHROPIC_API_KEY="your-key"

# Google Gemini
export GOOGLE_API_KEY="your-key"
```

Optional: Specify provider and model explicitly:

```bash
export ICEFRAME_LLM_PROVIDER="openai"  # or "anthropic" or "gemini"
export ICEFRAME_LLM_MODEL="gpt-4"      # or specific model
```

## Python API

```python
from iceframe import IceFrame
from iceframe.agent.core import IceFrameAgent

# Initialize
ice = IceFrame(config)
agent = IceFrameAgent(ice)

# Chat
response = agent.chat("What tables do I have?")
print(response)

response = agent.chat("Show me users older than 30")
print(response)

# Reset conversation
agent.reset_conversation()
```

## CLI Chat Interface

Start interactive chat:

```bash
iceframe-chat
```

Example session:

```
You: What tables do I have?
Assistant: You have the following tables:
- users
- orders
- products

You: Describe the users table
Assistant: The users table has the following schema:
- id (long, required)
- name (string)
- age (int)
- created_at (timestamp)

You: Show me users older than 30
Assistant: Here's a query to get users older than 30:

```python
from iceframe.expressions import Column

result = (ice.query("users")
    .filter(Column("age") > 30)
    .execute())
```

I found 15 users matching this criteria.
```

## Commands

- `exit` or `quit`: End session
- `clear` or `reset`: Clear conversation history

## Capabilities

### Schema Discovery

```
You: What columns are in the orders table?
Assistant: The orders table contains:
- order_id (long)
- user_id (long)
- amount (double)
- status (string)
- created_at (timestamp)
```

### Natural Language Queries

```
You: Find all orders over $100
Assistant: [Executes query and shows results]
```

### Code Generation

```
You: How do I join users with orders?
Assistant: Here's how to join users with orders:

```python
result = (ice.query("users")
    .join("orders", on="user_id", how="inner")
    .select("name", "order_id", "amount")
    .execute())
```
```

### Query Optimization

```
You: How can I make my queries faster?
Assistant: Based on your query patterns, I recommend:
1. Partition the orders table by date
2. Use column pruning to select only needed columns
3. Apply filters early to reduce data scanned
```

## Supported LLMs

| Provider | Models | Environment Variable |
|----------|--------|---------------------|
| OpenAI | GPT-4, GPT-3.5-turbo | `OPENAI_API_KEY` |
| Anthropic | Claude 3.5 Sonnet, Claude 3 Opus | `ANTHROPIC_API_KEY` |
| Google | Gemini 2.0 Flash, Gemini Pro | `GOOGLE_API_KEY` or `GEMINI_API_KEY` |

## Extending with Custom LLMs

Create a custom LLM provider:

```python
from iceframe.agent.llm_base import BaseLLM, LLMConfig

class CustomLLM(BaseLLM):
    def chat(self, messages, tools=None):
        # Your implementation
        pass
    
    def stream_chat(self, messages):
        # Your implementation
        pass

# Use it
agent = IceFrameAgent(ice, llm=CustomLLM(config))
```
