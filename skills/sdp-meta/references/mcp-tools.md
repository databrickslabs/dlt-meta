# SDP-META MCP server & tools

SDP-META ships its own **stdio MCP server** so an MCP-capable agent can scaffold
and inspect bundles without a live workspace. It is defined at
`src/databricks/labs/sdp_meta/mcp/server.py` and launched via the Labs CLI:

```bash
databricks labs sdp-meta mcp        # requires the `mcp` extra
pip install "databricks-labs-sdp-meta[mcp]"
```

The v0 server deliberately exposes **scaffolding/inspection only** — no
`onboard`/`deploy` (those need a live workspace and integration testing).

## Tools

| Tool | What it does | Key args |
|------|--------------|----------|
| `sdp_meta_bundle_init` | Scaffold a new sdp-meta DAB. With `quickstart=true` uses developer defaults; otherwise requires `config_file`. | `output_dir`, `quickstart`, `config_file` |
| `sdp_meta_bundle_add_flow` | Append one or more flow entries to a scaffolded bundle's onboarding file. | `bundle_dir`, flow fields / CSV |
| `sdp_meta_bundle_validate` | Run `databricks bundle validate` plus sdp-meta sanity checks against a scaffolded bundle. | `bundle_dir` |
| `sdp_meta_list_templates` | List every packaged onboarding / DQE / silver-transformation template name. | — |
| `sdp_meta_get_onboarding_template` | Return the raw text of a packaged template by name (from `list_templates`). | `name` |

Read-only template assets are also exposed as MCP **resources** under the
`sdp-meta://templates/` URI prefix.

## Filesystem sandbox

stdio MCP has no separate authz layer (trust boundary is "who can spawn the
process"). Every caller-supplied path (`output_dir` / `bundle_dir` /
`config_file` / `onboarding_file`) is resolved and confined to a single root via
`_resolve_within_root`. The root defaults to the process working directory and
can be overridden with `SDP_META_MCP_ROOT`. Paths that escape the root are
rejected before any write.

## Registering the server with an agent (example: Claude Code)

Add an MCP server entry so the agent can call the tools. Example `.mcp.json`:

```json
{
  "mcpServers": {
    "sdp-meta": {
      "command": "databricks",
      "args": ["labs", "sdp-meta", "mcp"],
      "defer_loading": true
    }
  }
}
```

Set `SDP_META_MCP_ROOT` in the server's environment to pin the sandbox root to
your project directory if the launch cwd is not already it.

## Recommended agent flow

1. `sdp_meta_list_templates` → discover available onboarding/DQE/transformation
   templates.
2. `sdp_meta_get_onboarding_template` → pull a template as a starting point.
3. `sdp_meta_bundle_init` (quickstart or with a `config_file`) → scaffold.
4. `sdp_meta_bundle_add_flow` → add tables/flows.
5. `sdp_meta_bundle_validate` → catch layer/topology/placeholder errors.
6. Hand off to the CLI (`databricks bundle deploy` / `sdp-meta deploy`) for the
   live run — see [cli-and-bundles.md](cli-and-bundles.md).
