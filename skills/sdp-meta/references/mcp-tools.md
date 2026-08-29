# SDP-META MCP server & tools

SDP-META ships its own **stdio MCP server** so an MCP-capable agent can scaffold
and inspect local bundles. Template inspection and flow editing are local;
bundle validation invokes the Databricks CLI and can contact a workspace using
the authentication inherited by the server process.

The server is defined at `src/databricks/labs/sdp_meta/mcp_server.py`, requires
Python 3.10+ and MCP SDK 2.x, and is launched through the Labs CLI:

```bash
python -m pip install "databricks-labs-sdp-meta[mcp]"
databricks labs install sdp-meta
databricks labs sdp-meta mcp
```

Install the extra in the Python environment used by the Labs CLI. The server
deliberately exposes **scaffolding/inspection only** — no
`onboard`/`deploy` (those need a live workspace and integration testing).

## Tools

| Tool | What it does | Key args |
|------|--------------|----------|
| `sdp_meta_bundle_init` | Scaffold a new SDP-META DAB. With `quickstart=true` it uses developer defaults; otherwise it requires `config_file`. | `output_dir`, `quickstart`, `config_file`, `overrides`, `profile` |
| `sdp_meta_bundle_add_flow` | Append one or more typed flow entries to a scaffolded bundle's onboarding file. The MCP tool does not accept CSV input. | `flows`, `bundle_dir`, `onboarding_file`, `dry_run` |
| `sdp_meta_bundle_validate` | Run `databricks bundle validate` plus SDP-META sanity checks against a scaffolded bundle. | `bundle_dir`, `target`, `profile` |
| `sdp_meta_list_templates` | List every packaged onboarding / DQE / silver-transformation template name. | — |
| `sdp_meta_get_onboarding_template` | Return the raw text of a packaged template by name (from `list_templates`). | `name` |

Read-only template assets are also exposed as MCP **resources** under the
`sdp-meta://templates/` URI prefix.

### Result semantics

- `sdp_meta_bundle_validate` returns a normal structured result even when
  `returncode` is nonzero. This means the tool ran and found an invalid bundle;
  inspect `output` for the validation failures.
- Invalid arguments, paths outside the filesystem root, unavailable/unknown
  templates, and failed initialization or flow updates return an MCP result
  with `is_error=true`.
- Expected command failures include a JSON payload containing `returncode` and
  captured `output` in the error text.

## Filesystem sandbox

stdio MCP has no separate authz layer (trust boundary is "who can spawn the
process"). Every caller-supplied path (`output_dir` / `bundle_dir` /
`config_file` / `onboarding_file`) is resolved and confined to a single root via
`_resolve_within_root`. `SDP_META_MCP_ROOT` must identify that existing root;
bundle initialization, validation, and flow operations refuse to run when it is
absent. Paths that escape the root are rejected before any write. Packaged
template list/get operations do not require this root.

If packaged examples are unavailable, the server logs the problem and starts
without template resources. Bundle tools remain available; template list/get
calls return an actionable error.

## Registering the server with an agent (example: Claude Code)

Add an MCP server entry so the agent can call the tools. Example `.mcp.json`:

```json
{
  "mcpServers": {
    "sdp-meta": {
      "command": "databricks",
      "args": ["labs", "sdp-meta", "mcp"],
      "env": {
        "SDP_META_MCP_ROOT": "/absolute/path/to/your/project"
      }
    }
  }
}
```

Set `SDP_META_MCP_ROOT` in the server's environment to the project directory
the tools may read and modify. Desktop hosts might not inherit an activated
shell or its `PATH`; in that case, configure the server with the absolute path
to the Python executable that has `databricks-labs-sdp-meta[mcp]` installed and
run `databricks.labs.sdp_meta.mcp_server.run_stdio` directly.

## Recommended agent flow

1. `sdp_meta_bundle_init` (quickstart or with a `config_file`) → scaffold.
2. `sdp_meta_bundle_add_flow` → add typed table/flow definitions.
3. `sdp_meta_bundle_validate` → catch layer/topology/placeholder errors; inspect
   a nonzero `returncode` as a normal negative result.
4. When examples are useful, call `sdp_meta_list_templates`, then
   `sdp_meta_get_onboarding_template` or read the corresponding MCP resource.
   Template content is reference material and is not an input to
   `sdp_meta_bundle_init`.
5. Hand off to the CLI (`databricks bundle deploy` or
   `databricks labs sdp-meta deploy`) for the live run — see
   [cli-and-bundles.md](cli-and-bundles.md).
