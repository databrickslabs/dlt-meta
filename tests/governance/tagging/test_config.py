"""Tests for catalog-scoped tag configuration."""

from pathlib import Path

import pytest

from databricks.labs.sdp_meta.governance.tagging.config import (
    MAX_COLUMN_TAGS_PER_TABLE,
    MAX_TAGS_PER_OBJECT,
    TaggingError,
    expand_desired,
    load_tags_file,
    resolve_target,
)
from databricks.labs.sdp_meta.governance.tagging.models import Key

EXAMPLES = Path(__file__).parents[3] / "demo" / "conf" / "governance"


def contributor(target="main.protected.customers", source_id=None):
    if source_id:
        return ("target", source_id, target)
    return ("target", target)


def test_empty_target_still_selects_table_for_cleanup():
    document = {
        "version": "1",
        "source_id": "file-a",
        "tables": {"customers": {}},
    }
    desired, contributors, tables = expand_desired(document, "main", "protected")
    assert desired == {}
    assert contributors == {contributor(source_id="file-a")}
    assert tables == {("main", "protected", "customers")}


def test_source_id_scopes_contributors_and_cli_override_wins():
    document = {
        "version": "1",
        "source_id": "file-a",
        "tables": {"customers": {"table": {"domain": "retail"}}},
    }

    desired, contributors, _ = expand_desired(
        document, "main", "protected", source_id="deployment-a"
    )

    expected = contributor(source_id="deployment-a")
    assert contributors == {expected}
    assert next(iter(desired.values())).contributors == {expected}


def test_expand_desired_requires_stable_source_id():
    document = {"version": "1", "tables": {"customers": {}}}

    with pytest.raises(TaggingError, match="source_id is required"):
        expand_desired(document, "main", "protected")


@pytest.mark.parametrize("source_id", ["", " spaces ", "bad/source", 123])
def test_load_tags_file_rejects_invalid_source_id(tmp_path, source_id):
    path = tmp_path / "tags.yml"
    path.write_text(
        f'version: "1"\nsource_id: {source_id!r}\ntables: {{}}\n',
        encoding="utf-8",
    )

    with pytest.raises(TaggingError, match="source_id must be"):
        load_tags_file(str(path))


@pytest.mark.parametrize(
    "target,catalog,schema,expected",
    [
        ("customers", "dev", "bronze", ("dev", "bronze", "customers")),
        ("silver.customers", "dev", None, ("dev", "silver", "customers")),
        (
            "finance.reporting.balances",
            "dev",
            "bronze",
            ("finance", "reporting", "balances"),
        ),
    ],
)
def test_target_resolution_supports_defaults_and_explicit_catalogs(
    target, catalog, schema, expected
):
    assert resolve_target(target, catalog, schema) == expected


def test_example_file_resolves_multiple_schemas_in_one_catalog():
    document = load_tags_file(str(EXAMPLES / "tags.template.yml"))
    _, _, tables = expand_desired(document, "dev", "retail_bronze")
    assert ("dev", "retail_bronze", "customers") in tables
    assert ("dev", "retail_quarantine", "customers_quarantine") in tables
    assert ("dev", "reporting", "gl_balances") in tables


def test_onboarding_example_maps_all_physical_targets():
    document = load_tags_file(str(EXAMPLES / "tags.onboarding-example.yml"))
    _, _, tables = expand_desired(
        document,
        document["defaults"]["catalog"],
        document["defaults"]["schema"],
    )
    assert len(tables) == 12


def test_relative_table_requires_catalog_and_schema_defaults():
    with pytest.raises(TaggingError):
        resolve_target("customers", None, None)


def test_file_scope_rejects_another_catalog():
    document = {
        "version": "1",
        "defaults": {"catalog": "main", "schema": "bronze"},
        "tables": {"finance.reporting.customers": {}},
    }
    with pytest.raises(TaggingError):
        expand_desired(document, "main", "bronze", "test-source")


@pytest.mark.parametrize(
    "contents,match",
    [
        ("tables: {}\n", 'expected a mapping with version: "1"'),
        ('version: "1"\ntables: []\n', "expected tables to be a mapping"),
        (
            'version: "1"\ndefaults: [invalid]\ntables: {}\n',
            "defaults must be a mapping",
        ),
        ('version: "1"\nextra: true\ntables: {}\n', "unknown top-level nodes"),
        (
            'version: "1"\ndefaults:\n  warehouse: x\ntables: {}\n',
            "unknown defaults",
        ),
    ],
)
def test_load_tags_file_rejects_invalid_document_shapes(tmp_path, contents, match):
    path = tmp_path / "tags.yml"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(TaggingError, match=match):
        load_tags_file(str(path))


def test_load_tags_file_rejects_duplicate_keys(tmp_path):
    path = tmp_path / "tags.yml"
    path.write_text(
        'version: "1"\ntables:\n  customers: {}\n  customers: {}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate key 'customers'"):
        load_tags_file(str(path))


@pytest.mark.parametrize(
    "target,catalog,schema,match",
    [
        (123, "main", "bronze", "target name must be a string"),
        ("bronze.customers", None, None, "requires --catalog"),
        ("a.b.c.d", "main", "bronze", "expected table, schema.table"),
        ("bad-name", "main", "bronze", "target identifier"),
    ],
)
def test_target_resolution_rejects_invalid_targets(target, catalog, schema, match):
    with pytest.raises(TaggingError):
        resolve_target(target, catalog, schema)


@pytest.mark.parametrize(
    "node,match",
    [
        (["invalid"], "expected a mapping"),
        ({"unexpected": {}}, "unknown nodes"),
        ({"table": ["invalid"]}, "expected a map of tag key"),
        ({"columns": ["invalid"]}, "columns: expected a mapping"),
        ({"columns": {"bad-name": {}}}, "column"),
        ({"table": {"bad.key": "value"}}, "forbidden char"),
        ({"table": {" key": "value"}}, "bad tag key"),
        ({"table": {"key": 1}}, "bad value"),
        ({"table": {"key": " value"}}, "bad value"),
    ],
)
def test_expand_desired_rejects_invalid_assignments(node, match):
    document = {"version": "1", "tables": {"customers": node}}

    with pytest.raises(TaggingError, match=match):
        expand_desired(document, "main", "bronze", "test-source")


def test_none_tag_value_becomes_empty_string_and_reserved_prefix_allows_dot():
    document = {
        "version": "1",
        "tables": {"customers": {"table": {"system.certification": None}}},
    }

    desired, _, _ = expand_desired(document, "main", "bronze", "test-source")

    assert desired[
        Key("main", "bronze", "customers", None, "system.certification")
    ].value == ""


def test_aliases_for_same_target_merge_identical_assignments():
    document = {
        "version": "1",
        "tables": {
            "customers": {"table": {"domain": "retail"}},
            "bronze.customers": {"table": {"domain": "retail"}},
        },
    }

    desired, contributors, tables = expand_desired(
        document, "main", "bronze", "test-source"
    )

    assignment = desired[Key("main", "bronze", "customers", None, "domain")]
    assert assignment.value == "retail"
    assert assignment.contributors == {
        contributor("main.bronze.customers", "test-source")
    }
    assert contributors == {contributor("main.bronze.customers", "test-source")}
    assert tables == {("main", "bronze", "customers")}


def test_aliases_for_same_target_reject_conflicting_assignments():
    document = {
        "version": "1",
        "tables": {
            "customers": {"table": {"domain": "retail"}},
            "bronze.customers": {"table": {"domain": "finance"}},
        },
    }

    with pytest.raises(TaggingError):
        expand_desired(document, "main", "bronze", "test-source")


def test_table_tag_limit_is_enforced():
    tags = {f"tag_{index}": "value" for index in range(MAX_TAGS_PER_OBJECT + 1)}
    document = {"version": "1", "tables": {"customers": {"table": tags}}}

    with pytest.raises(TaggingError):
        expand_desired(document, "main", "bronze", "test-source")


def test_column_tag_limit_is_enforced_across_columns():
    columns = {
        f"column_{index}": {
            f"tag_{tag_index}": "value"
            for tag_index in range(MAX_TAGS_PER_OBJECT)
        }
        for index in range(MAX_COLUMN_TAGS_PER_TABLE // MAX_TAGS_PER_OBJECT + 1)
    }
    document = {"version": "1", "tables": {"customers": {"columns": columns}}}

    with pytest.raises(TaggingError):
        expand_desired(document, "main", "bronze", "test-source")


@pytest.mark.parametrize(
    "catalog,schema",
    [
        (None, "bronze"),
        ("bad-name", "bronze"),
        ("main", "bad-name"),
    ],
)
def test_expand_desired_rejects_missing_or_invalid_defaults(catalog, schema):
    document = {"version": "1", "tables": {}}

    with pytest.raises(TaggingError):
        expand_desired(document, catalog, schema, "test-source")
