"""sdp-meta Model Context Protocol (MCP) server.

Exposes a curated subset of sdp-meta operations as MCP tools so an MCP-capable
client (Claude Code, Cursor, Claude Desktop) can drive sdp-meta workflows.

Entrypoint: :func:`databricks.labs.sdp_meta.mcp.server.run_stdio`.

Requires the ``mcp`` extra::

    pip install 'databricks-labs-sdp-meta[mcp]'
"""
