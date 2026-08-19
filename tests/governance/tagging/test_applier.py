"""Tests for applier preflight behavior."""

from unittest.mock import Mock, call

import pytest

from databricks.labs.sdp_meta.governance.tagging import applier
from databricks.labs.sdp_meta.governance.tagging.backends import preflight_tables
from databricks.labs.sdp_meta.governance.tagging.config import TaggingError
from databricks.labs.sdp_meta.governance.tagging.models import Action, Key


class FakeBackend:
    def __init__(self, responses=None):
        self.statements = []
        self.responses = list(responses or [])

    def sql(self, statement):
        self.statements.append(statement)
        return self.responses.pop(0) if self.responses else []


def test_table_only_target_is_checked_during_preflight():
    backend = FakeBackend(responses=[[]])
    with pytest.raises(TaggingError):
        preflight_tables(
            backend,
            {("main", "protected", "customers")},
        )


def table_key(tag_key="sensitivity"):
    return Key("main", "protected", "customers", None, tag_key)


def column_key(tag_key="sensitivity"):
    return Key("main", "protected", "customers", "email", tag_key)


def main_args(*extra):
    return [
        "--tags-file",
        "tags.yml",
        "--state-table",
        "main.governance.tag_assignments",
        *extra,
    ]


def configure_main(monkeypatch, actions, verified=None):
    backend = Mock()
    tables = {("main", "protected", "customers")}
    selected = {("target", "main.protected.customers")}
    desired = {
        action.key: action.value
        for action in actions
        if action.kind == "set"
    }
    mocks = {
        "load_tags_file": Mock(return_value={}),
        "make_backend": Mock(return_value=backend),
        "expand_desired": Mock(return_value=(desired, selected, tables)),
        "preflight_tables": Mock(),
        "preflight_columns": Mock(),
        "read_actual": Mock(side_effect=[{}, verified or {}]),
        "read_state": Mock(return_value={}),
        "plan": Mock(return_value=actions),
        "ensure_state_table": Mock(),
        "persist_pending_plan": Mock(),
        "write_state": Mock(),
    }
    for name, mock in mocks.items():
        monkeypatch.setattr(applier, name, mock)
    return backend, tables, mocks


def test_empty_tags_file_stops_before_preflight(monkeypatch):
    backend = Mock()
    preflight = Mock()
    monkeypatch.setattr(applier, "load_tags_file", Mock(return_value={}))
    monkeypatch.setattr(applier, "make_backend", Mock(return_value=backend))
    monkeypatch.setattr(
        applier,
        "expand_desired",
        Mock(return_value=({}, set(), set())),
    )
    monkeypatch.setattr(applier, "preflight_tables", preflight)

    assert applier.main(main_args()) == 1
    preflight.assert_not_called()


def test_source_id_override_is_forwarded_to_config_expansion(monkeypatch):
    _, _, mocks = configure_main(monkeypatch, actions=[])

    assert applier.main(main_args("--source-id", "deployment-a")) == 0

    mocks["expand_desired"].assert_called_once_with(
        {}, None, None, "deployment-a"
    )


def test_public_api_accepts_an_embedded_backend(monkeypatch):
    backend, _, mocks = configure_main(monkeypatch, actions=[])

    result = applier.apply_tags(
        tags_file="tags.yml",
        state_table="main.governance.tag_assignments",
        source_id="notebook-demo",
        dry_run=True,
        backend=backend,
    )

    assert result == 0
    mocks["make_backend"].assert_not_called()
    mocks["expand_desired"].assert_called_once_with(
        {}, None, None, "notebook-demo"
    )


def test_empty_plan_performs_no_ddl_and_writes_empty_result(monkeypatch):
    backend, tables, mocks = configure_main(monkeypatch, actions=[])

    result = applier.main(main_args())

    assert result == 0
    backend.sql.assert_not_called()
    mocks["ensure_state_table"].assert_called_once_with(
        backend, "main.governance.tag_assignments"
    )
    mocks["persist_pending_plan"].assert_called_once_with(
        backend, "main.governance.tag_assignments", []
    )
    assert mocks["read_actual"].call_count == 2
    mocks["write_state"].assert_called_once_with(
        backend, "main.governance.tag_assignments", [], {}
    )
    mocks["preflight_tables"].assert_called_once_with(backend, tables)


def test_dry_run_does_not_create_state_or_execute_ddl(monkeypatch):
    action = Action("set", table_key(), "pii", "R1 absent")
    backend, _, mocks = configure_main(monkeypatch, [action])

    result = applier.main(main_args("--dry-run"))

    assert result == 0
    assert action.idx == 0
    backend.sql.assert_not_called()
    mocks["ensure_state_table"].assert_not_called()
    mocks["persist_pending_plan"].assert_not_called()
    mocks["write_state"].assert_not_called()
    assert mocks["read_actual"].call_count == 1


def test_conflict_stops_before_state_or_ddl(monkeypatch, capsys):
    action = Action("conflict", table_key(), "pii", "externally owned")
    backend, _, mocks = configure_main(monkeypatch, [action])

    result = applier.main(main_args())

    assert result == 2
    backend.sql.assert_not_called()
    mocks["ensure_state_table"].assert_not_called()
    mocks["persist_pending_plan"].assert_not_called()
    mocks["write_state"].assert_not_called()
    assert "Conflicts require review" in capsys.readouterr().err


def test_successful_ddl_is_verified_and_written_to_state(monkeypatch):
    set_action = Action("set", table_key(), "pii", "R1 absent")
    unset_action = Action("unset", column_key(), reason="R5 stale")
    actions = [set_action, unset_action]
    backend, _, mocks = configure_main(
        monkeypatch,
        actions,
        verified={set_action.key: "pii"},
    )

    result = applier.main(main_args())

    assert result == 0
    assert backend.sql.call_args_list == [
        call(
            "ALTER TABLE `main`.`protected`.`customers` "
            "SET TAGS ('sensitivity' = 'pii')"
        ),
        call(
            "UNSET TAG ON COLUMN `main`.`protected`.`customers`.`email` "
            "`sensitivity`"
        ),
    ]
    mocks["write_state"].assert_called_once_with(
        backend,
        "main.governance.tag_assignments",
        actions,
        {0: True, 1: True},
    )


def test_ddl_failure_is_reported_and_written_as_unsuccessful(
    monkeypatch, capsys
):
    action = Action("set", table_key(), "pii", "R1 absent")
    backend, _, mocks = configure_main(monkeypatch, [action])
    backend.sql.side_effect = RuntimeError("permission denied")

    result = applier.main(main_args())

    assert result == 3
    mocks["write_state"].assert_called_once_with(
        backend,
        "main.governance.tag_assignments",
        [action],
        {},
    )
    stderr = capsys.readouterr().err
    assert "FAILED: ALTER TABLE" in stderr
    assert "permission denied" in stderr


def test_verification_failures_mark_set_and_unset_unsuccessful(
    monkeypatch, capsys
):
    set_action = Action("set", table_key(), "pii", "R1 absent")
    unset_action = Action("unset", column_key(), reason="R5 stale")
    actions = [set_action, unset_action]
    backend, _, mocks = configure_main(
        monkeypatch,
        actions,
        verified={
            set_action.key: "restricted",
            unset_action.key: "pii",
        },
    )

    result = applier.main(main_args())

    assert result == 3
    mocks["write_state"].assert_called_once_with(
        backend,
        "main.governance.tag_assignments",
        actions,
        {0: False, 1: False},
    )
    stderr = capsys.readouterr().err
    assert "VERIFY FAILED: main.protected.customers :: sensitivity" in stderr
    assert (
        "VERIFY FAILED (still present): "
        "main.protected.customers.email :: sensitivity"
    ) in stderr
