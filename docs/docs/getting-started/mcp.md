---
id: mcp
title: MCP Server
sidebar_position: 6
---

# MCP Server

The SDP-META MCP (Model Context Protocol) server exposes SDP-META operations as MCP tools so an MCP-capable client — Claude Code, Cursor, Claude Desktop, or any other MCP host — can drive SDP-META scaffolding and inspection. The server runs locally over stdio via `databricks labs sdp-meta mcp` and is shipped as the optional `[mcp]` extra of `databricks-labs-sdp-meta`.

## Prerequisites

- Python 3.10+
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) authenticated against your workspace
- An MCP-capable client (Claude Code, Claude Desktop, Cursor, or similar)

## Authentication

The MCP server uses the default profile from `~/.databrickscfg`. To use a specific profile, set the environment variable before launching your MCP client:

```bash
export DATABRICKS_CONFIG_PROFILE=my-profile
```

Or pass it inline when wiring the server (Claude Code example):

```bash
DATABRICKS_CONFIG_PROFILE=my-profile claude mcp add sdp-meta -- databricks labs sdp-meta mcp
```

## Install

```bash
pip install 'databricks-labs-sdp-meta[mcp]'
databricks labs install sdp-meta
```

## Wire into your MCP client

### Claude Desktop

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sdp-meta": {
      "command": "databricks",
      "args": ["labs", "sdp-meta", "mcp"]
    }
  }
}
```

### Claude Code

```bash
claude mcp add sdp-meta -- databricks labs sdp-meta mcp
```

Or add to `.mcp.json` in your project root using the same `mcpServers` shape.

### Cursor

Use the same `mcpServers` JSON shape as Claude Desktop in your Cursor MCP settings.

## Available tools

| Tool | Description |
|---|---|
| `sdp_meta_bundle_init` | Scaffold a new SDP-META DAB. Pass `quickstart=true` for developer defaults. |
| `sdp_meta_bundle_validate` | Run `databricks bundle validate` plus SDP-META checks against a scaffolded bundle. |
| `sdp_meta_bundle_add_flow` | Append one or more flow entries to a bundle's onboarding file. |
| `sdp_meta_list_templates` | List the names of every packaged onboarding, DQE, and silver-transformation template. |
| `sdp_meta_get_onboarding_template` | Return the raw text of a packaged template by name. |

## MCP resources

The server exposes packaged templates as MCP resources:

```
sdp-meta://templates/<format>/<filename>
```

Examples:
- `sdp-meta://templates/json/cloudfiles-onboarding.template`
- `sdp-meta://templates/yml/eventhub-onboarding.template.yml`

## Troubleshooting

**`ImportError: The mcp extra is not installed`** — install with `pip install 'databricks-labs-sdp-meta[mcp]'`.

**The server exits immediately when launched manually** — expected. The stdio transport waits for an MCP client; launch via your MCP host.

**Tool calls return `returncode != 0`** — the underlying `databricks bundle ...` invocation failed. The captured stdout is in the `output` field of the response.
