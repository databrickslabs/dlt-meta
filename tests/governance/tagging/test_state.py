"""Tests for assignment ownership state."""

import pytest

from databricks.labs.sdp_meta.governance.tagging.config import TaggingError
from databricks.labs.sdp_meta.governance.tagging.models import (
    OWN_SCRIPT,
    Action,
    Key,
)
from databricks.labs.sdp_meta.governance.tagging.state import (
    decode_contributors,
    ensure_state_table,
    persist_pending_plan,
    read_state,
    split_fqn,
    state_table_exists,
    write_state,
)


class FakeBackend:
    def __init__(self, responses=None):
        self.statements = []
        self.responses = list(responses or [])

    def sql(self, statement):
        self.statements.append(statement)
        return self.responses.pop(0) if self.responses else []


def key():
    return Key("main", "protected", "customers", None, "sensitivity")


def test_read_state_does_not_create_missing_table():
    backend = FakeBackend(responses=[[]])
    result = read_state(
        backend,
        "main.governance.tag_assignments",
        {("main", "protected", "customers")},
    )
    assert result == {}
    assert len(backend.statements) == 1
    assert "information_schema.tables" in backend.statements[0]
    assert "CREATE TABLE" not in backend.statements[0]


def test_pending_ownership_is_persisted_before_execution():
    backend = FakeBackend()
    action = Action(
        "set",
        key(),
        "pii",
        "R1 absent",
        contributors={("target", "main.protected.customers")},
        ownership=OWN_SCRIPT,
    )
    persist_pending_plan(
        backend,
        "main.governance.tag_assignments",
        [action],
    )
    assert len(backend.statements) == 1
    assert "MERGE INTO" in backend.statements[0]
    assert "'pending'" in backend.statements[0]


def test_split_fqn_validates_three_part_state_table():
    assert split_fqn("main.governance.tag_assignments", "state table") == (
        "main",
        "governance",
        "tag_assignments",
    )

    with pytest.raises(TaggingError, match="expected catalog.schema.table"):
        split_fqn("governance.tag_assignments", "state table")


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        ([], False),
        ([(1,)], True),
    ],
)
def test_state_table_exists_queries_information_schema(rows, expected):
    backend = FakeBackend(responses=[rows])

    assert state_table_exists(backend, "main.governance.tag_assignments") is expected
    assert "`main`.information_schema.tables" in backend.statements[0]
    assert "table_schema = 'governance'" in backend.statements[0]
    assert "table_name = 'tag_assignments'" in backend.statements[0]


def test_ensure_state_table_validates_and_creates_delta_table():
    backend = FakeBackend()

    ensure_state_table(backend, "main.governance.tag_assignments")

    assert len(backend.statements) == 1
    assert backend.statements[0].startswith(
        "CREATE TABLE IF NOT EXISTS main.governance.tag_assignments"
    )
    assert "USING DELTA" in backend.statements[0]


def test_read_state_skips_backend_for_empty_target_set():
    backend = FakeBackend()

    assert read_state(backend, "main.governance.tag_assignments", set()) == {}
    assert backend.statements == []


def test_read_state_decodes_table_and_column_metadata():
    backend = FakeBackend(
        responses=[
            [(1,)],
            [
                (
                    "main",
                    "protected",
                    "customers",
                    None,
                    "domain",
                    "customer",
                    "script",
                    '[["target", "main.protected.customers"]]',
                    "applied",
                ),
                (
                    "main",
                    "protected",
                    "customers",
                    "email",
                    "sensitivity",
                    "pii",
                    "external",
                    None,
                    "pending",
                ),
            ],
        ]
    )

    result = read_state(
        backend,
        "main.governance.tag_assignments",
        {("main", "protected", "customers")},
    )

    assert result == {
        Key("main", "protected", "customers", None, "domain"): {
            "last_applied_value": "customer",
            "ownership": "script",
            "contributors": {("target", "main.protected.customers")},
            "status": "applied",
        },
        Key("main", "protected", "customers", "email", "sensitivity"): {
            "last_applied_value": "pii",
            "ownership": "external",
            "contributors": set(),
            "status": "pending",
        },
    }
    assert "SELECT catalog_name" in backend.statements[1]
    assert "catalog_name='main'" in backend.statements[1]


@pytest.mark.parametrize("raw", [None, ""])
def test_decode_contributors_empty_values(raw):
    assert decode_contributors(raw) == set()


def test_decode_contributors_supports_source_scoped_identity():
    assert decode_contributors(
        '[["target", "file-a", "main.protected.customers"]]'
    ) == {("target", "file-a", "main.protected.customers")}


@pytest.mark.parametrize("raw", ["not-json", "null"])
def test_decode_contributors_rejects_invalid_payload(raw):
    with pytest.raises(ValueError, match="invalid contributors JSON"):
        decode_contributors(raw)


def test_pending_plan_only_merges_set_actions():
    backend = FakeBackend()
    actions = [
        Action("set", key(), "pii", contributors={("rule", "R1")}),
        Action("unset", key()),
        Action("conflict", key()),
    ]

    persist_pending_plan(backend, "main.governance.tag_assignments", actions)

    assert len(backend.statements) == 1
    assert "'pii','script'" in backend.statements[0]
    assert '["rule", "R1"]' in backend.statements[0]
    assert "'pending'" in backend.statements[0]


def test_pending_plan_with_no_sets_does_not_execute_sql():
    backend = FakeBackend()

    persist_pending_plan(
        backend,
        "main.governance.tag_assignments",
        [Action("unset", key()), Action("forget", key())],
    )

    assert backend.statements == []


def test_write_state_applies_successful_transitions_and_deletes_rows():
    backend = FakeBackend()
    column_key = Key("main", "protected", "customers", "email", "sensitivity")
    actions = [
        Action("conflict", key(), idx=0),
        Action("forget", key(), idx=1),
        Action("set", key(), "failed", idx=2),
        Action("unset", column_key, idx=3),
        Action(
            "set",
            column_key,
            "pii",
            idx=4,
            contributors={("rule", "R1")},
            ownership="external",
        ),
    ]

    write_state(
        backend,
        "main.governance.tag_assignments",
        actions,
        {1: True, 2: False, 3: True, 4: True},
    )

    assert len(backend.statements) == 3
    assert "column_name IS NULL" in backend.statements[0]
    assert "DELETE FROM" in backend.statements[1]
    assert "column_name = 'email'" in backend.statements[1]
    assert "MERGE INTO" in backend.statements[2]
    assert "'pii','external'" in backend.statements[2]
    assert "'applied'" in backend.statements[2]
    assert "'failed'" not in backend.statements[2]


def test_write_state_leaves_failed_unset_owned():
    backend = FakeBackend()

    write_state(
        backend,
        "main.governance.tag_assignments",
        [Action("unset", key(), idx=7)],
        {7: False},
    )

    assert backend.statements == []
