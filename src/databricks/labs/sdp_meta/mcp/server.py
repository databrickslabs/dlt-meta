"""sdp-meta MCP server (stdio transport).

The server exposes four scaffolding/inspection tools plus a templates resource
namespace. It deliberately does NOT expose ``onboard``/``deploy`` in v0 — those
require a live workspace and proper integration testing (tracked as a follow-up
under issue #231).

Architecture:
- The lowlevel :class:`mcp.server.Server` is wrapped by :func:`build_server`
  which registers tool/resource handlers against a captured ``SDPMeta`` instance
  (so future tools that need the ``WorkspaceClient`` can reach it).
- :func:`run_stdio` is the entrypoint called by the ``mcp`` CLI command.
- Tool dispatch is split into pure handlers (``_handle_*``) so unit tests can
  call them directly without spinning up the asyncio stdio plumbing.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
from contextlib import redirect_stdout
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Resource, TextContent, Tool
from pydantic import AnyUrl

logger = logging.getLogger("databricks.labs.sdp_meta.mcp")


# ---------------------------------------------------------------------------
# Templates: locate the onboarding/DQE/transformation examples directory.
#
# Resolution order (first match wins):
#   1. ``SDP_META_EXAMPLES_DIR`` environment variable — explicit override for
#      installed/containerised deployments and tests.
#   2. ``importlib.resources`` against ``databricks.labs.sdp_meta._packaged_examples``
#      — populated for true wheel installs once #231 packages templates.
#   3. Repo-tree walk (``parents[5]/examples``) — the developer/source-checkout
#      path. This is the path the unit tests exercise today.
#
# Returns ``None`` if no strategy resolves a directory; callers surface a
# clear error pointing at strategy 1 as the recommended workaround.
# ---------------------------------------------------------------------------

_EXAMPLES_ENV_VAR = "SDP_META_EXAMPLES_DIR"
_PACKAGED_EXAMPLES_MODULE = "databricks.labs.sdp_meta._packaged_examples"


def _locate_examples_dir() -> Optional[Path]:
    """Resolve the examples directory or return ``None`` if not found."""
    env_override = os.environ.get(_EXAMPLES_ENV_VAR)
    if env_override:
        candidate = Path(env_override).expanduser().resolve()
        if candidate.is_dir():
            return candidate
        logger.warning(
            "%s=%r is set but the path does not exist or is not a directory; "
            "falling back to packaged/source-tree resolution.",
            _EXAMPLES_ENV_VAR,
            env_override,
        )

    try:
        from importlib.resources import files

        pkg_root = files(_PACKAGED_EXAMPLES_MODULE)
        # ``files()`` returns a Traversable; convert to a real Path when we can.
        pkg_path = Path(str(pkg_root))
        if pkg_path.is_dir():
            return pkg_path
    except (ModuleNotFoundError, ImportError):
        pass

    repo_candidate = Path(__file__).resolve().parents[5] / "examples"
    if repo_candidate.is_dir():
        return repo_candidate

    return None


def _list_template_files() -> List[Tuple[str, Path]]:
    """Return ``[(logical_name, absolute_path)]`` for every shipped template.

    ``logical_name`` is what callers pass to ``get_onboarding_template`` and
    what appears in the ``sdp-meta://templates/<name>`` resource URI. We use
    ``<format>/<filename>`` (e.g. ``json/cloudfiles-onboarding.template``) so
    the JSON and YAML variants of the same template don't collide.

    Returns an empty list if no examples directory could be resolved (see
    :func:`_locate_examples_dir`); callers should produce an actionable error
    pointing the user at the ``SDP_META_EXAMPLES_DIR`` workaround.
    """
    out: List[Tuple[str, Path]] = []
    examples_dir = _locate_examples_dir()
    if examples_dir is None:
        return out
    for fmt in ("json", "yml"):
        fmt_dir = examples_dir / fmt
        if not fmt_dir.is_dir():
            continue
        for path in sorted(fmt_dir.rglob("*")):
            if not path.is_file():
                continue
            name = f"{fmt}/{path.relative_to(fmt_dir).as_posix()}"
            out.append((name, path))
    return out


# ---------------------------------------------------------------------------
# Tool input schemas (JSON Schema). Kept as plain dicts so we don't pull in
# pydantic just for tool signatures.
# ---------------------------------------------------------------------------

_BUNDLE_INIT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "output_dir": {
            "type": "string",
            "description": "Directory where the bundle will be scaffolded. Created if missing.",
            "default": ".",
        },
        "quickstart": {
            "type": "boolean",
            "description": (
                "Use the developer-friendly quickstart defaults "
                "(cloudFiles + bronze_silver + split + pypi). "
                "When true, no further prompts are answered."
            ),
            "default": True,
        },
        "config_file": {
            "type": "string",
            "description": (
                "Optional path to a JSON config file with pre-answered "
                "template prompts. Ignored when quickstart=true."
            ),
        },
        "profile": {
            "type": "string",
            "description": "Optional Databricks CLI profile to forward to `databricks bundle init`.",
        },
    },
    "additionalProperties": False,
}

_BUNDLE_VALIDATE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "bundle_dir": {
            "type": "string",
            "description": "Path to the scaffolded bundle directory (must contain databricks.yml).",
            "default": ".",
        },
        "target": {
            "type": "string",
            "description": "Optional bundle target (e.g. dev, prod) to forward to validate.",
        },
        "profile": {
            "type": "string",
            "description": "Optional Databricks CLI profile.",
        },
    },
    "required": ["bundle_dir"],
    "additionalProperties": False,
}

_FLOW_SPEC_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "description": "A single flow entry. Field names match FlowSpec in bundle.py.",
    "properties": {
        "source_format": {"type": "string", "default": "cloudFiles"},
        "source_path": {"type": "string"},
        "source_database": {"type": "string"},
        "source_table": {"type": "string"},
        "source_schema_path": {"type": "string"},
        "kafka_bootstrap_servers": {"type": "string"},
        "kafka_topic": {"type": "string"},
        "starting_offsets": {"type": "string", "default": "earliest"},
        "snapshot_format": {"type": "string", "default": "delta"},
        "bronze_table": {"type": "string"},
        "silver_table": {"type": "string"},
        "data_flow_id": {"type": "string", "default": "auto"},
        "data_flow_group": {"type": "string"},
        "source_system": {"type": "string", "default": "auto_added"},
        "cloudfiles_format": {"type": "string", "default": "json"},
    },
    "additionalProperties": False,
}

_BUNDLE_ADD_FLOW_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "bundle_dir": {
            "type": "string",
            "description": "Path to the scaffolded bundle directory.",
            "default": ".",
        },
        "flows": {
            "type": "array",
            "description": "One or more flow entries to append.",
            "items": _FLOW_SPEC_SCHEMA,
            "minItems": 1,
        },
        "onboarding_file": {
            "type": "string",
            "description": (
                "Optional override for the onboarding file name. "
                "Auto-detected from resources/variables.yml when omitted."
            ),
        },
        "dry_run": {
            "type": "boolean",
            "description": "Show the resulting onboarding file without writing it.",
            "default": False,
        },
    },
    "required": ["bundle_dir", "flows"],
    "additionalProperties": False,
}

_GET_TEMPLATE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "description": (
                "Template identifier of the form '<format>/<filename>', e.g. "
                "'json/cloudfiles-onboarding.template' or "
                "'yml/eventhub-onboarding.template.yml'. "
                "Use the list_templates tool to discover names."
            ),
        },
    },
    "required": ["name"],
    "additionalProperties": False,
}

_LIST_TEMPLATES_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


# ---------------------------------------------------------------------------
# Tool handlers (pure functions; tests call these directly)
# ---------------------------------------------------------------------------


def _ok(payload: Any) -> List[TextContent]:
    """Wrap a tool result as the single TextContent block MCP expects."""
    if isinstance(payload, str):
        return [TextContent(type="text", text=payload)]
    return [TextContent(type="text", text=json.dumps(payload, indent=2, default=str))]


def _handle_bundle_init(args: Dict[str, Any]) -> List[TextContent]:
    from databricks.labs.sdp_meta.bundle import (
        BundleInitCommand,
        bundle_init,
        write_quickstart_config_file,
    )

    output_dir = args.get("output_dir", ".")
    quickstart = bool(args.get("quickstart", True))
    profile = args.get("profile")
    config_file = args.get("config_file")

    if quickstart:
        cfg_dir = Path(output_dir).resolve()
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = write_quickstart_config_file(cfg_dir)
        cmd = BundleInitCommand(
            output_dir=output_dir, config_file=str(cfg_path), profile=profile
        )
    else:
        if not config_file:
            raise ValueError(
                "config_file is required when quickstart=false (the MCP server "
                "cannot run interactive prompts)."
            )
        cmd = BundleInitCommand(
            output_dir=output_dir, config_file=config_file, profile=profile
        )

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bundle_init(cmd)
    return _ok({"returncode": rc, "output": buf.getvalue(), "output_dir": str(Path(output_dir).resolve())})


def _handle_bundle_validate(args: Dict[str, Any]) -> List[TextContent]:
    from databricks.labs.sdp_meta.bundle import BundleValidateCommand, bundle_validate

    cmd = BundleValidateCommand(
        bundle_dir=args.get("bundle_dir", "."),
        target=args.get("target"),
        profile=args.get("profile"),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bundle_validate(cmd)
    return _ok({"returncode": rc, "output": buf.getvalue()})


def _handle_bundle_add_flow(args: Dict[str, Any]) -> List[TextContent]:
    from databricks.labs.sdp_meta.bundle import (
        BundleAddFlowCommand,
        FlowSpec,
        bundle_add_flow,
    )

    raw_flows = args.get("flows") or []
    if not raw_flows:
        raise ValueError("`flows` must contain at least one entry.")
    flows = [FlowSpec(**f) for f in raw_flows]
    cmd = BundleAddFlowCommand(
        bundle_dir=args.get("bundle_dir", "."),
        onboarding_file=args.get("onboarding_file"),
        flows=flows,
        dry_run=bool(args.get("dry_run", False)),
    )
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bundle_add_flow(cmd)
    return _ok({"returncode": rc, "output": buf.getvalue(), "flows_added": [asdict(f) for f in flows]})


def _handle_list_templates(args: Dict[str, Any]) -> List[TextContent]:
    del args
    examples_dir = _locate_examples_dir()
    names = [name for name, _ in _list_template_files()]
    return _ok(
        {
            "templates": names,
            "examples_dir": str(examples_dir) if examples_dir else None,
            "hint": (
                None
                if examples_dir
                else (
                    f"No examples directory resolved. Set {_EXAMPLES_ENV_VAR}=<path> "
                    "or run from a source checkout with examples/ at the repo root."
                )
            ),
        }
    )


def _handle_get_onboarding_template(args: Dict[str, Any]) -> List[TextContent]:
    name = args.get("name")
    if not name:
        raise ValueError("`name` is required.")
    matches = [path for n, path in _list_template_files() if n == name]
    if not matches:
        available = [n for n, _ in _list_template_files()]
        if not available:
            raise FileNotFoundError(
                f"Template '{name}' not found and no examples directory could be "
                f"resolved. Set {_EXAMPLES_ENV_VAR}=<path> or run from a source "
                "checkout with examples/ at the repo root."
            )
        raise FileNotFoundError(
            f"Template '{name}' not found. Available: {available}"
        )
    text = matches[0].read_text()
    return _ok({"name": name, "path": str(matches[0]), "content": text})


_DISPATCH = {
    "sdp_meta_bundle_init": (_handle_bundle_init, _BUNDLE_INIT_SCHEMA,
                             "Scaffold a new sdp-meta DAB. With quickstart=true, "
                             "uses developer-friendly defaults; otherwise requires "
                             "config_file."),
    "sdp_meta_bundle_validate": (_handle_bundle_validate, _BUNDLE_VALIDATE_SCHEMA,
                                 "Run `databricks bundle validate` plus sdp-meta "
                                 "sanity checks against a scaffolded bundle."),
    "sdp_meta_bundle_add_flow": (_handle_bundle_add_flow, _BUNDLE_ADD_FLOW_SCHEMA,
                                 "Append one or more flow entries to a scaffolded "
                                 "bundle's onboarding file."),
    "sdp_meta_list_templates": (_handle_list_templates, _LIST_TEMPLATES_SCHEMA,
                                "List the names of every packaged onboarding / DQE "
                                "/ silver-transformation template."),
    "sdp_meta_get_onboarding_template": (_handle_get_onboarding_template, _GET_TEMPLATE_SCHEMA,
                                         "Return the raw text of a packaged template "
                                         "by name (see list_templates)."),
}


def list_tools() -> List[Tool]:
    """Public for tests: build the Tool descriptors served over MCP."""
    return [
        Tool(name=name, description=desc, inputSchema=schema)
        for name, (_, schema, desc) in _DISPATCH.items()
    ]


def call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
    """Public for tests: synchronous tool dispatch."""
    if name not in _DISPATCH:
        raise ValueError(f"Unknown tool: {name}. Available: {list(_DISPATCH)}")
    handler, _schema, _desc = _DISPATCH[name]
    return handler(arguments or {})


# ---------------------------------------------------------------------------
# Resources (read-only template assets)
# ---------------------------------------------------------------------------

_RESOURCE_PREFIX = "sdp-meta://templates/"


def _mime_for_template(name: str) -> str:
    """Pick the mimetype for a template name.

    ``*.template`` files contain handlebars-style ``{{ ... }}`` placeholders
    around JSON or YAML and are therefore *not* valid JSON/YAML on their own
    — clients that try to parse them will fail. Surface those as plain text
    and reserve the structured mimetypes for the literal ``*.json`` /
    ``*.yml`` resources.
    """
    if ".template" in name:
        return "text/plain"
    if name.startswith("json/"):
        return "application/json"
    if name.startswith("yml/"):
        return "text/yaml"
    return "text/plain"


def list_resources() -> List[Resource]:
    out: List[Resource] = []
    examples_dir = _locate_examples_dir()
    for name, path in _list_template_files():
        try:
            rel = (
                path.relative_to(examples_dir)
                if examples_dir is not None
                else path.name
            )
        except ValueError:
            rel = path.name
        out.append(
            Resource(
                uri=AnyUrl(f"{_RESOURCE_PREFIX}{name}"),
                name=name,
                description=f"Packaged sdp-meta template at examples/{rel}",
                mimeType=_mime_for_template(name),
            )
        )
    return out


def read_resource(uri: str) -> str:
    if not uri.startswith(_RESOURCE_PREFIX):
        raise ValueError(
            f"Unsupported resource URI scheme: {uri}. Expected prefix {_RESOURCE_PREFIX}"
        )
    name = uri[len(_RESOURCE_PREFIX):]
    matches = [path for n, path in _list_template_files() if n == name]
    if not matches:
        raise FileNotFoundError(f"Template not found: {name}")
    return matches[0].read_text()


# ---------------------------------------------------------------------------
# Server wiring
# ---------------------------------------------------------------------------


def build_server(_sdp_meta) -> Server:
    """Build (but do not run) the MCP Server with all handlers registered.

    The ``sdp_meta`` argument is captured for forward-compatibility with v1
    tools that will need a live ``WorkspaceClient``; v0 tools don't read it.
    """
    server: Server = Server("sdp-meta")

    @server.list_tools()
    async def _list_tools() -> List[Tool]:
        return list_tools()

    @server.call_tool()
    async def _call_tool(name: str, arguments: Optional[Dict[str, Any]]) -> List[TextContent]:
        return call_tool(name, arguments)

    @server.list_resources()
    async def _list_resources() -> List[Resource]:
        return list_resources()

    @server.read_resource()
    async def _read_resource(uri: AnyUrl) -> str:
        return read_resource(str(uri))

    return server


def run_stdio(sdp_meta) -> None:
    """Run the MCP server over stdio. Blocks until the client disconnects."""
    server = build_server(sdp_meta)

    async def _run() -> None:
        async with stdio_server() as (read, write):
            await server.run(read, write, server.create_initialization_options())

    asyncio.run(_run())
