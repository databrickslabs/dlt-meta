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

Filesystem sandbox:
- stdio MCP has no separate authn/authz layer (the transport's trust boundary
  is "who can spawn the process", running with the invoking user's own
  credentials). To contain the confused-deputy / prompt-injection case, every
  caller-supplied path (``output_dir`` / ``bundle_dir`` / ``config_file`` /
  ``onboarding_file``) is resolved and confined to a single root via
  :func:`_resolve_within_root`.
- The root defaults to the process working directory (for stdio, the directory
  the client launched the server in) and can be overridden with the
  ``SDP_META_MCP_ROOT`` environment variable. Paths that escape the root are
  rejected before any mkdir/write reaches ``bundle.py``.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import os
import re
from contextlib import redirect_stdout
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from databricks.labs.sdp_meta.identifiers import validate_uc_identifier

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


# ---------------------------------------------------------------------------
# Filesystem sandbox for caller-supplied paths.
#
# The stdio MCP server has no separate authn/authz layer — that is inherent
# to the transport (the trust boundary is "who can spawn the process", which
# runs with the invoking user's own credentials). What we CAN contain is the
# confused-deputy / prompt-injection case: an LLM driving the bundle_init /
# bundle_validate / bundle_add_flow tools (fed untrusted onboarding content
# or a hostile tool description) being steered into scaffolding files at
# arbitrary filesystem locations the user happens to be able to write.
#
# Every caller-supplied path (output_dir, bundle_dir, config_file,
# onboarding_file) is therefore resolved and checked to live inside a single
# allowed root before it reaches ``bundle.py`` (which ``.resolve()``s and
# ``mkdir(parents=True)`` + writes with no containment of its own).
#
# Root resolution:
#   * ``SDP_META_MCP_ROOT`` env var when set to an existing directory
#     (explicit override for containerised / multi-project deployments).
#   * otherwise the process working directory — for stdio MCP this is the
#     directory the client launched the server in, i.e. the user's project.
# ---------------------------------------------------------------------------

_MCP_ROOT_ENV_VAR = "SDP_META_MCP_ROOT"


def _mcp_root() -> Path:
    """Return the absolute directory caller-supplied paths must stay within."""
    env_override = os.environ.get(_MCP_ROOT_ENV_VAR)
    if env_override:
        candidate = Path(env_override).expanduser().resolve()
        if candidate.is_dir():
            return candidate
        logger.warning(
            "%s=%r is set but is not an existing directory; falling back to "
            "the process working directory as the MCP filesystem root.",
            _MCP_ROOT_ENV_VAR,
            env_override,
        )
    return Path.cwd().resolve()


def _resolve_within_root(raw_path: str, *, kind: str) -> Path:
    """Resolve ``raw_path`` and confirm it stays inside :func:`_mcp_root`.

    Returns the resolved absolute :class:`Path` on success. Raises
    ``ValueError`` with an actionable message when the path escapes the
    sandbox root — including the ``..``-traversal and absolute-path cases,
    since both collapse to an out-of-root resolved path.

    The boundary check compares against ``root`` plus a trailing separator
    so a sibling directory whose name merely *starts with* the root (e.g.
    root ``/work`` vs ``/work_evil``) cannot bypass the guard.
    """
    root = _mcp_root()
    lexical = Path(raw_path).expanduser()
    if not lexical.is_absolute():
        lexical = root / lexical

    # Belt-and-suspenders symlink guard: walk the (un-resolved) path's existing
    # components and reject if any is a symlink whose real target escapes the
    # root. ``resolve()`` below also collapses existing symlinks, so this is
    # defense-in-depth — it narrows (does not fully close) the TOCTOU window
    # between this check and bundle.py's later mkdir/write. Fully closing it
    # would require fd-based (O_NOFOLLOW) writes down in bundle.py.
    _assert_no_symlink_escape(lexical, root, kind=kind, raw_path=raw_path)

    resolved = lexical.resolve()

    root_str = str(root)
    resolved_str = str(resolved)
    inside = resolved == root or resolved_str.startswith(
        root_str.rstrip(os.sep) + os.sep
    )
    if not inside:
        raise ValueError(
            f"{kind} {raw_path!r} resolves to {resolved_str!r}, which is "
            f"outside the MCP filesystem root {root_str!r}. The sdp-meta MCP "
            f"server only scaffolds/reads files inside that root. Pass a path "
            f"within it, or set {_MCP_ROOT_ENV_VAR} to the project directory "
            f"you intend to work in."
        )
    return resolved


def _assert_no_symlink_escape(
    lexical: Path, root: Path, *, kind: str, raw_path: str
) -> None:
    """Reject a path that traverses a symlink pointing outside ``root``.

    Walks every existing component from ``lexical`` up to the filesystem root
    and, for any that is a symlink, checks that its real target stays inside
    ``root``. Non-existent components (``is_symlink()`` is False) and plain
    directories are skipped. Raises ``ValueError`` on an escaping symlink.
    """
    root_real = os.path.realpath(root)
    root_prefix = root_real.rstrip(os.sep) + os.sep
    node = lexical
    while True:
        if node.is_symlink():
            target_real = os.path.realpath(node)
            if not (
                target_real == root_real or target_real.startswith(root_prefix)
            ):
                raise ValueError(
                    f"{kind} {raw_path!r} traverses symlink {str(node)!r} whose "
                    f"target {target_real!r} escapes the MCP filesystem root "
                    f"{root_real!r}."
                )
        parent = node.parent
        if parent == node:  # reached filesystem root
            break
        node = parent


# Databricks CLI profile names are `.databrickscfg` INI section headers; in
# practice they are short identifiers. We forward the value into a
# ``databricks --profile <x>`` argv (list form, no shell — so this is not a
# shell-injection fix), but an allow-list keeps a hostile value from smuggling
# extra CLI flags (e.g. a leading ``-``) or path/quote characters into the
# subprocess. Empty/None means "default profile" and is allowed by the callers.
# First character must be alphanumeric or underscore so a value like
# ``--target`` can never be mistaken for a CLI flag by the ``databricks`` argv
# parser; dashes and dots are allowed only in interior positions.
_PROFILE_RE = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]*$")
_MAX_PROFILE_LEN = 128


def _validate_profile(profile: Optional[str]) -> Optional[str]:
    """Validate a Databricks CLI profile name; return it unchanged (or None)."""
    if profile is None or profile == "":
        return None
    if not isinstance(profile, str):
        raise ValueError(
            f"profile must be a string, got {type(profile).__name__}: {profile!r}"
        )
    if len(profile) > _MAX_PROFILE_LEN:
        raise ValueError(
            f"profile {profile!r} is {len(profile)} characters; maximum is "
            f"{_MAX_PROFILE_LEN}."
        )
    if not _PROFILE_RE.match(profile):
        raise ValueError(
            f"profile {profile!r} is not a valid Databricks CLI profile name. "
            f"Allowed characters: letters, digits, underscore, dash and dot "
            f"({_PROFILE_RE.pattern})."
        )
    return profile


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
        "overrides": {
            "type": "object",
            "description": (
                "Only used when quickstart=true. Override individual "
                "quickstart answers while keeping every other default, e.g. "
                '{"uc_catalog_name": "acme_prod", "sdp_meta_dependency": '
                '"databricks-labs-sdp-meta==0.1.0"}. Overridable keys: '
                "bundle_name, uc_catalog_name, sdp_meta_schema, "
                "bronze_target_schema, silver_target_schema, layer "
                "(bronze|silver|bronze_silver), pipeline_mode (split|combined), "
                "source_format (cloudFiles|delta|kafka|eventhub|snapshot), "
                "onboarding_file_format (yaml|json), dataflow_group, author, "
                "sdp_meta_dependency. Unknown keys and invalid values are "
                "rejected. Catalog/schema names must be regular SQL "
                "identifiers (letters, digits, underscores)."
            ),
            "additionalProperties": True,
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
    profile = _validate_profile(args.get("profile"))
    config_file = args.get("config_file")
    overrides = args.get("overrides")

    # Sandbox every caller-supplied path to the MCP filesystem root before
    # any mkdir / write reaches bundle.py.
    output_dir_resolved = _resolve_within_root(output_dir, kind="output_dir")

    if quickstart:
        output_dir_resolved.mkdir(parents=True, exist_ok=True)
        # `overrides` (validated inside write_quickstart_config_file) lets the
        # caller change e.g. uc_catalog_name without leaving the quickstart
        # happy-path or hand-authoring a full config_file.
        cfg_path = write_quickstart_config_file(output_dir_resolved, overrides=overrides)
        cmd = BundleInitCommand(
            output_dir=str(output_dir_resolved),
            config_file=str(cfg_path),
            profile=profile,
        )
    else:
        if not config_file:
            raise ValueError(
                "config_file is required when quickstart=false (the MCP server "
                "cannot run interactive prompts)."
            )
        config_file_resolved = _resolve_within_root(
            config_file, kind="config_file"
        )
        cmd = BundleInitCommand(
            output_dir=str(output_dir_resolved),
            config_file=str(config_file_resolved),
            profile=profile,
        )

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = bundle_init(cmd)
    return _ok({"returncode": rc, "output": buf.getvalue(), "output_dir": str(output_dir_resolved)})


def _handle_bundle_validate(args: Dict[str, Any]) -> List[TextContent]:
    from databricks.labs.sdp_meta.bundle import BundleValidateCommand, bundle_validate

    bundle_dir = _resolve_within_root(
        args.get("bundle_dir", "."), kind="bundle_dir"
    )
    cmd = BundleValidateCommand(
        bundle_dir=str(bundle_dir),
        target=args.get("target"),
        profile=_validate_profile(args.get("profile")),
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

    # Defense-in-depth: validate the identifier-bearing flow fields at the MCP
    # boundary. bundle.py already rejects bad `source_format` / `bronze_table`
    # / `silver_table`, but not `source_database` / `source_table` (spliced
    # into `spark.read.table(...)` for delta/snapshot sources) or
    # `data_flow_group` (spliced unquoted into the onboarding row). Reject
    # non-regular-identifier values here so a hostile/confused MCP client
    # can't smuggle them downstream (issue #261). Optional fields are only
    # checked when set; `data_flow_id="auto"` is a sentinel, not an identifier.
    for i, flow in enumerate(flows):
        for field_name in ("source_database", "source_table", "data_flow_group"):
            value = getattr(flow, field_name, None)
            if value:
                validate_uc_identifier(value, kind=f"flows[{i}].{field_name}")
    bundle_dir = _resolve_within_root(
        args.get("bundle_dir", "."), kind="bundle_dir"
    )
    onboarding_file = args.get("onboarding_file")
    onboarding_file_resolved = (
        str(_resolve_within_root(onboarding_file, kind="onboarding_file"))
        if onboarding_file
        else None
    )
    cmd = BundleAddFlowCommand(
        bundle_dir=str(bundle_dir),
        onboarding_file=onboarding_file_resolved,
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
