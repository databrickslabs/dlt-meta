---
title: "SDP-META MCP Server"
date: 2026-04-29
weight: 10
draft: false
---

### What is the MCP server?

The sdp-meta MCP (Model Context Protocol) server exposes a curated subset of
sdp-meta operations as MCP tools so an MCP-capable client — Claude Code,
Cursor, Claude Desktop, or any other MCP host — can drive sdp-meta scaffolding
and inspection on your behalf.

The server runs locally over stdio. It is launched by the
`databricks labs sdp-meta mcp` command and shipped as the optional `mcp` extra
of `databricks-labs-sdp-meta`.

### Prerequisites

- Python 3.8 +
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html)
- An MCP-capable client (e.g. Claude Code, Claude Desktop, Cursor)

### Install

```commandline
pip install 'databricks-labs-sdp-meta[mcp]'
databricks labs install sdp-meta
```

### Run

```commandline
databricks labs sdp-meta mcp
```

The process reads/writes JSON-RPC framed messages on stdio and blocks until
the client disconnects. Most users do not run this command directly; their
MCP client launches it for them via the configuration below.

### Wire it into Claude Desktop / Claude Code / Cursor

Add an entry to the client's MCP server configuration. The shape varies per
client but the launch command is always the same.

**Claude Desktop** (`claude_desktop_config.json`):

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

**Claude Code** (`.mcp.json` in your project, or via `claude mcp add`):

```commandline
claude mcp add sdp-meta -- databricks labs sdp-meta mcp
```

**Cursor** — same `mcpServers` shape as Claude Desktop in your Cursor settings.

### Tools (v0)

| Tool | Description |
|------|-------------|
| `sdp_meta_bundle_init` | Scaffold a new sdp-meta DAB. Pass `quickstart=true` for developer defaults; otherwise supply `config_file` (the server cannot run interactive prompts). |
| `sdp_meta_bundle_validate` | Run `databricks bundle validate` plus sdp-meta sanity checks against a scaffolded bundle. |
| `sdp_meta_bundle_add_flow` | Append one or more flow entries to a scaffolded bundle's onboarding file. Flows are dicts matching `FlowSpec` in `bundle.py`. |
| `sdp_meta_list_templates` | List the names of every packaged onboarding / DQE / silver-transformation template. |
| `sdp_meta_get_onboarding_template` | Return the raw text of a packaged template by name (use `list_templates` to discover names). |

### Resources

The server exposes packaged templates as MCP resources:

```
sdp-meta://templates/<format>/<filename>
```

For example: `sdp-meta://templates/json/cloudfiles-onboarding.template` or
`sdp-meta://templates/yml/eventhub-onboarding.template.yml`. A client can
fetch any of these as additional context when authoring a configuration.


### Troubleshooting

- `ImportError: The mcp extra is not installed` — install with
  `pip install 'databricks-labs-sdp-meta[mcp]'`.
- The server exits immediately when launched manually — that is expected;
  stdio transport waits for an MCP client. Launch via your MCP host instead.
- Tool calls return `returncode != 0` — the underlying `databricks bundle ...`
  invocation failed; the captured stdout is in the `output` field of the
  response payload.
