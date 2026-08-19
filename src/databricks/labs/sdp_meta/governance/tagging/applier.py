"""Reconcile catalog-scoped table and column tags."""

import argparse
import sys
from typing import Dict, Optional, Sequence

import yaml

from databricks.labs.sdp_meta.governance.tagging.backends import (
    make_backend,
    preflight_columns,
    preflight_tables,
    read_actual,
)
from databricks.labs.sdp_meta.governance.tagging.config import (
    expand_desired,
    fail,
    load_tags_file,
)
from databricks.labs.sdp_meta.governance.tagging.planner import plan
from databricks.labs.sdp_meta.governance.tagging.tag_sql_renderer import render_ddl
from databricks.labs.sdp_meta.governance.tagging.state import (
    ensure_state_table,
    persist_pending_plan,
    read_state,
    write_state,
)

MARKERS = {
    "set": "+",
    "unset": "-",
    "conflict": "!",
    "noop": "=",
    "record_external": "~",
    "forget": ".",
    "update_contributors": "~",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tags-file", required=True)
    parser.add_argument(
        "--catalog",
        help="override defaults.catalog for this catalog-scoped file",
    )
    parser.add_argument(
        "--schema",
        help="override defaults.schema for one-part table names",
    )
    parser.add_argument(
        "--source-id",
        help="override the stable source_id declared in the tags file",
    )
    parser.add_argument(
        "--state-table",
        required=True,
        help="fully qualified applier state table",
    )
    parser.add_argument(
        "--warehouse-id",
        help="SQL warehouse ID when no Spark session is active",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--transfer-ownership",
        action="store_true",
        help="resolve reviewed external conflicts by taking ownership",
    )
    return parser


def apply_tags(
    tags_file: str,
    state_table: str,
    *,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    source_id: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    dry_run: bool = False,
    transfer_ownership: bool = False,
    backend=None,
) -> int:
    """Reconcile desired tags and return the documented execution status.

    An explicit backend is useful for embedded execution and testing. When it
    is omitted, an active Spark session is preferred; otherwise warehouse_id
    selects the Databricks SQL backend.
    """
    document = load_tags_file(tags_file)
    if backend is None:
        backend = make_backend(warehouse_id)
    defaults = document.get("defaults") or {}
    catalog = catalog or defaults.get("catalog")
    schema = schema or defaults.get("schema")
    desired, selected_contributors, tables = expand_desired(
        document, catalog, schema, source_id
    )
    if not selected_contributors or not tables:
        fail("tags file contains no tables")

    preflight_tables(backend, tables)
    preflight_columns(backend, desired)
    actual = read_actual(backend, tables)
    state = read_state(backend, state_table, tables)
    actions = plan(
        desired,
        actual,
        state,
        transfer_ownership,
        selected_contributors,
    )
    for index, action in enumerate(actions):
        action.idx = index

    counts = {}
    for action in actions:
        counts[action.kind] = counts.get(action.kind, 0) + 1
        value = f" = {action.value!r}" if action.value is not None else ""
        print(
            f" {MARKERS[action.kind]} [{action.kind:15s}] "
            f"{action.key.label()}{value}   ({action.reason})"
        )
    print(f"\nPlan: {counts}")

    conflicts = [action for action in actions if action.kind == "conflict"]
    if dry_run:
        print("DRY RUN — no DDL executed, no state changed.")
        return 2 if conflicts else 0
    if conflicts:
        print(
            "Conflicts require review; no DDL or state changes were made.",
            file=sys.stderr,
        )
        return 2

    ensure_state_table(backend, state_table)
    persist_pending_plan(backend, state_table, actions)
    run_ok: Dict[int, bool] = {}
    failures = 0
    for statement, statement_actions in render_ddl(actions):
        try:
            backend.sql(statement)
            for action in statement_actions:
                run_ok[action.idx] = True
        except Exception as error:
            failures += 1
            print(f"FAILED: {statement}\n        {error}", file=sys.stderr)

    verified = read_actual(backend, tables)
    for action in actions:
        if (
            action.kind == "set"
            and run_ok.get(action.idx)
            and verified.get(action.key) != action.value
        ):
            run_ok[action.idx] = False
            failures += 1
            print(f"VERIFY FAILED: {action.key.label()}", file=sys.stderr)
        if action.kind == "unset" and run_ok.get(action.idx) and action.key in verified:
            run_ok[action.idx] = False
            failures += 1
            print(
                f"VERIFY FAILED (still present): {action.key.label()}",
                file=sys.stderr,
            )

    write_state(backend, state_table, actions, run_ok)
    print(
        f"\nDone. applied={sum(run_ok.values())} failed={failures} "
        f"conflicts={len(conflicts)} "
        f"external_preserved={counts.get('record_external', 0)}"
    )
    return 3 if failures else 0


def _run(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return apply_tags(
        tags_file=args.tags_file,
        state_table=args.state_table,
        catalog=args.catalog,
        schema=args.schema,
        source_id=args.source_id,
        warehouse_id=args.warehouse_id,
        dry_run=args.dry_run,
        transfer_ownership=args.transfer_ownership,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        return _run(argv)
    except (OSError, ValueError, yaml.YAMLError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
