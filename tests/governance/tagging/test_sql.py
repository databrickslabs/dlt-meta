"""Tests for Unity Catalog tag DDL rendering."""

from databricks.labs.sdp_meta.governance.tagging.models import Action, Key
from databricks.labs.sdp_meta.governance.tagging.tag_sql_renderer import render_ddl


def test_table_sets_are_grouped_into_one_statement():
    first = Action(
        "set",
        Key("main", "bronze", "customers", None, "sensitivity"),
        "pii",
    )
    second = Action(
        "set",
        Key("main", "bronze", "customers", None, "data_domain"),
        "customer",
    )
    statements = render_ddl([first, second])
    assert len(statements) == 1
    assert statements[0][0].startswith(
        "ALTER TABLE `main`.`bronze`.`customers` SET TAGS"
    )
    assert statements[0][1] == [first, second]


def test_column_unset_uses_column_ddl():
    action = Action(
        "unset",
        Key("main", "bronze", "customers", "email", "sensitivity"),
    )
    statements = render_ddl([action])
    assert statements == [
        (
            "UNSET TAG ON COLUMN `main`.`bronze`.`customers`.`email` `sensitivity`",
            [action],
        )
    ]


def test_column_sets_are_grouped_separately_from_table_sets():
    table_action = Action(
        "set",
        Key("main", "bronze", "customers", None, "data_domain"),
        "customer",
    )
    first_column_action = Action(
        "set",
        Key("main", "bronze", "customers", "email", "sensitivity"),
        "pii",
    )
    second_column_action = Action(
        "set",
        Key("main", "bronze", "customers", "email", "retention"),
        "30 days",
    )

    statements = render_ddl(
        [first_column_action, table_action, second_column_action]
    )

    assert statements == [
        (
            "ALTER TABLE `main`.`bronze`.`customers` SET TAGS "
            "('data_domain' = 'customer')",
            [table_action],
        ),
        (
            "ALTER TABLE `main`.`bronze`.`customers` ALTER COLUMN `email` "
            "SET TAGS ('sensitivity' = 'pii', 'retention' = '30 days')",
            [first_column_action, second_column_action],
        ),
    ]


def test_table_unset_uses_table_ddl():
    action = Action(
        "unset",
        Key("main", "bronze", "customers", None, "sensitivity"),
    )

    statements = render_ddl([action])

    assert statements == [
        (
            "UNSET TAG ON TABLE `main`.`bronze`.`customers` `sensitivity`",
            [action],
        )
    ]


def test_set_escapes_quotes_in_tag_keys_and_values():
    action = Action(
        "set",
        Key("main", "bronze", "customers", None, "data'owner"),
        "O'Reilly's",
    )

    statements = render_ddl([action])

    assert statements == [
        (
            "ALTER TABLE `main`.`bronze`.`customers` SET TAGS "
            "('data''owner' = 'O''Reilly''s')",
            [action],
        )
    ]


def test_non_ddl_actions_are_not_rendered():
    actions = [
        Action(
            kind,
            Key("main", "bronze", "customers", None, "sensitivity"),
            "pii",
        )
        for kind in (
            "noop",
            "conflict",
            "record_external",
            "forget",
            "update_contributors",
        )
    ]

    assert render_ddl(actions) == []
