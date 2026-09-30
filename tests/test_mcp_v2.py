"""Contract tests against the MCP Python SDK v2 high-level server."""

import pytest
from mcp import Client
from mcp.server import MCPServer

from iceframe.mcp_server import mcp


def test_server_uses_mcp_v2_api():
    assert isinstance(mcp, MCPServer)


@pytest.mark.asyncio
async def test_mcp_v2_discovers_registered_tools():
    async with Client(mcp) as client:
        result = await client.list_tools()

    names = {tool.name for tool in result.tools}
    assert {"get_schema", "execute_query", "read_documentation"} <= names


@pytest.mark.asyncio
async def test_mcp_v2_server_remains_legacy_client_compatible():
    # SDK v2 intentionally serves the 2026-07-28 protocol and the prior
    # initialize-era protocol from one server. Exercise the compatibility path
    # through the real legacy initialization and discovery exchange. Individual
    # handlers are exercised in the MCP unit suite; SDK 2.1's in-process direct
    # dispatcher can otherwise deadlock when a legacy call follows a modern
    # session in the same event loop.
    async with Client(mcp, mode="legacy") as client:
        result = await client.list_tools()

    names = {tool.name for tool in result.tools}
    assert {"generate_sql", "execute_query"} <= names
