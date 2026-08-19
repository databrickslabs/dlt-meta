"""Render deterministic Unity Catalog tag DDL."""

from typing import Dict, List, Tuple

from databricks.labs.sdp_meta.governance.tagging.models import Action


def escape_literal(value: str) -> str:
    return value.replace("'", "''")


def render_ddl(actions: List[Action]) -> List[Tuple[str, List[Action]]]:
    statements: List[Tuple[str, List[Action]]] = []
    grouped_sets: Dict[Tuple[str, str], List[Action]] = {}
    for action in actions:
        if action.kind == "set":
            grouped_sets.setdefault(
                (action.key.fq_table(), action.key.column), []
            ).append(action)
    for (table, column), grouped_actions in sorted(
        grouped_sets.items(), key=lambda item: (item[0][0], item[0][1] or "")
    ):
        pairs = ", ".join(
            f"'{escape_literal(action.key.tag_key)}' = '{escape_literal(action.value)}'"
            for action in grouped_actions
        )
        if column is None:
            statement = f"ALTER TABLE {table} SET TAGS ({pairs})"
        else:
            statement = (
                f"ALTER TABLE {table} ALTER COLUMN `{column}` SET TAGS ({pairs})"
            )
        statements.append((statement, grouped_actions))
    for action in actions:
        if action.kind != "unset":
            continue
        if action.key.column is None:
            statement = (
                f"UNSET TAG ON TABLE {action.key.fq_table()} `{action.key.tag_key}`"
            )
        else:
            statement = (
                f"UNSET TAG ON COLUMN {action.key.fq_table()}."
                f"`{action.key.column}` `{action.key.tag_key}`"
            )
        statements.append((statement, [action]))
    return statements
