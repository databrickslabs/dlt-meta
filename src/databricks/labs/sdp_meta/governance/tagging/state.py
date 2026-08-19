"""Delta-backed ownership state for tag reconciliation."""

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from databricks.labs.sdp_meta.governance.tagging.config import check_ident, fail
from databricks.labs.sdp_meta.governance.tagging.models import OWN_SCRIPT, Action, Key
from databricks.labs.sdp_meta.governance.tagging.tag_sql_renderer import escape_literal

STATE_DDL = """CREATE TABLE IF NOT EXISTS {table_name} (
  catalog_name STRING,
  schema_name STRING,
  table_name STRING,
  column_name STRING,
  tag_key STRING,
  last_applied_value STRING,
  ownership STRING,
  contributors STRING,
  status STRING,
  error_message STRING,
  first_observed_at TIMESTAMP,
  last_reconciled_at TIMESTAMP
) USING DELTA"""


def split_fqn(fqn: str, what: str) -> Tuple[str, str, str]:
    parts = fqn.split(".")
    if len(parts) != 3:
        fail(f"{what}: expected catalog.schema.table, got {fqn!r}")
    return tuple(check_ident(part, what) for part in parts)


def state_table_exists(backend, state_table: str) -> bool:
    catalog, schema, table = split_fqn(state_table, "state table")
    rows = backend.sql(
        f"SELECT 1 FROM `{catalog}`.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}' LIMIT 1"
    )
    return bool(rows)


def ensure_state_table(backend, state_table: str) -> None:
    split_fqn(state_table, "state table")
    backend.sql(STATE_DDL.format(table_name=state_table))


def decode_contributors(raw: Optional[str]) -> set:
    if not raw:
        return set()
    try:
        return {tuple(item) for item in json.loads(raw)}
    except (TypeError, ValueError) as error:
        raise ValueError(
            f"invalid contributors JSON in state table: {raw!r}"
        ) from error


def read_state(backend, state_table: str, tables: set) -> Dict[Key, dict]:
    if not tables or not state_table_exists(backend, state_table):
        return {}
    predicates = " OR ".join(
        f"(catalog_name='{catalog}' AND schema_name='{schema}' "
        f"AND table_name='{table}')"
        for catalog, schema, table in sorted(tables)
    )
    output = {}
    rows = backend.sql(
        f"SELECT catalog_name, schema_name, table_name, column_name, tag_key, "
        f"last_applied_value, ownership, contributors, status FROM {state_table} "
        f"WHERE {predicates}"
    )
    for (
        catalog,
        schema,
        table,
        column,
        key,
        value,
        ownership,
        contributors,
        status,
    ) in rows:
        output[Key(catalog, schema, table, column or None, key)] = {
            "last_applied_value": value,
            "ownership": ownership,
            "contributors": decode_contributors(contributors),
            "status": status,
        }
    return output


def _column_predicate(key: Key) -> str:
    if not key.column:
        return "column_name IS NULL"
    return f"column_name = '{escape_literal(key.column)}'"


def _delete_state_row(backend, state_table: str, key: Key) -> None:
    backend.sql(
        f"DELETE FROM {state_table} "
        f"WHERE catalog_name = '{escape_literal(key.catalog)}' "
        f"AND schema_name = '{escape_literal(key.schema)}' "
        f"AND table_name = '{escape_literal(key.table)}' "
        f"AND {_column_predicate(key)} "
        f"AND tag_key = '{escape_literal(key.tag_key)}'"
    )


def _merge_state_rows(backend, state_table: str, rows: List[str]) -> None:
    if not rows:
        return
    backend.sql(
        f"""MERGE INTO {state_table} state USING (VALUES {", ".join(rows)}
        AS incoming(catalog_name, schema_name, table_name, column_name, tag_key,
                    last_applied_value, ownership, contributors, status,
                    error_message, first_observed_at, last_reconciled_at))
        ON state.catalog_name=incoming.catalog_name
           AND state.schema_name=incoming.schema_name
           AND state.table_name=incoming.table_name
           AND state.tag_key=incoming.tag_key
           AND (state.column_name<=>incoming.column_name)
        WHEN MATCHED THEN UPDATE SET
             last_applied_value=incoming.last_applied_value,
             ownership=incoming.ownership,
             contributors=incoming.contributors,
             status=incoming.status,
             error_message=incoming.error_message,
             last_reconciled_at=incoming.last_reconciled_at
        WHEN NOT MATCHED THEN INSERT *"""
    )


def _state_value_row(action: Action, status: str, now: str) -> str:
    key = action.key
    column_sql = "NULL" if not key.column else f"'{escape_literal(key.column)}'"
    contributors = escape_literal(json.dumps(sorted(action.contributors)))
    ownership = action.ownership or OWN_SCRIPT
    value = "" if action.value is None else action.value
    return (
        f"('{escape_literal(key.catalog)}','{escape_literal(key.schema)}',"
        f"'{escape_literal(key.table)}',{column_sql},"
        f"'{escape_literal(key.tag_key)}','{escape_literal(value)}',"
        f"'{escape_literal(ownership)}','{contributors}','{status}',NULL,"
        f"timestamp'{now}',timestamp'{now}')"
    )


def persist_pending_plan(backend, state_table: str, actions: List[Action]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        _state_value_row(action, "pending", now)
        for action in actions
        if action.kind == "set"
    ]
    _merge_state_rows(backend, state_table, rows)


def write_state(
    backend,
    state_table: str,
    actions: List[Action],
    run_ok: Dict[int, bool],
) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for action in actions:
        if action.kind == "conflict":
            continue
        if action.kind == "forget":
            _delete_state_row(backend, state_table, action.key)
            continue
        if action.kind in ("set", "unset") and not run_ok.get(action.idx, False):
            continue
        if action.kind == "unset":
            _delete_state_row(backend, state_table, action.key)
            continue
        rows.append(_state_value_row(action, "applied", now))
    _merge_state_rows(backend, state_table, rows)
