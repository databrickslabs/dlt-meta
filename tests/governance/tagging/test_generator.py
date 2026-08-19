"""Tests for generating tag skeletons from onboarding targets."""

import json

import pytest
import yaml

from databricks.labs.sdp_meta.governance.tagging.config import expand_desired
from databricks.labs.sdp_meta.governance.tagging.generator import (
    convert,
    load_onboarding,
    main,
    output_name,
    resolve_onboarding_target,
)


def onboarding_row():
    return {
        "data_flow_id": "customers-100",
        "bronze_catalog_prod": "main",
        "bronze_database_prod": "retail_bronze",
        "bronze_table": "customers",
        "bronze_catalog_quarantine_prod": "main",
        "bronze_database_quarantine_prod": "retail_quarantine",
        "bronze_quarantine_table": "customers_quarantine",
        "silver_catalog_prod": "main",
        "silver_database_prod": "main.retail_silver",
        "silver_table": "customers",
    }


def test_generates_entries_for_every_onboarding_target():
    rendered, targets = convert(
        [onboarding_row()],
        "prod",
        default_catalog="main",
        default_schema="retail_bronze",
    )
    assert yaml.safe_load(rendered) == {
        "version": "1",
        "source_id": "main-prod-tags",
        "defaults": {
            "catalog": "main",
            "schema": "retail_bronze",
        },
        "tables": {},
    }
    assert targets == [
        "customers",
        "retail_quarantine.customers_quarantine",
        "retail_silver.customers",
    ]
    assert "#   customers:" in rendered
    assert "customers: {}" not in rendered


def test_generated_skeleton_cannot_select_owned_tags_for_cleanup():
    rendered, _ = convert(
        [onboarding_row()],
        "prod",
        default_catalog="main",
        default_schema="retail_bronze",
    )

    desired, contributors, tables = expand_desired(
        yaml.safe_load(rendered), "main", "retail_bronze"
    )

    assert desired == {}
    assert contributors == set()
    assert tables == set()


def test_does_not_read_tag_attributes():
    row = onboarding_row()
    row["bronze_tags"] = {"table": {"must_not": "be_copied"}}
    rendered, _ = convert([row], "prod", "main", "retail_bronze")
    assert "must_not" not in rendered


def test_catalog_selection_generates_one_catalog_file():
    row = onboarding_row()
    row["silver_catalog_prod"] = "finance"
    row["silver_database_prod"] = "reporting"
    rendered, targets = convert([row], "prod", "finance")
    result = yaml.safe_load(rendered)
    assert result["defaults"] == {"catalog": "finance"}
    assert result["tables"] == {}
    assert targets == ["reporting.customers"]


def test_multiple_catalogs_require_selection():
    row = onboarding_row()
    row["silver_catalog_prod"] = "finance"
    row["silver_database_prod"] = "reporting"
    with pytest.raises(ValueError, match="run once per catalog"):
        convert([row], "prod")


def test_rejects_unresolved_placeholders():
    row = onboarding_row()
    row["bronze_catalog_prod"] = "{uc_catalog_name}"
    with pytest.raises(ValueError, match="unresolved placeholder"):
        convert([row], "prod", "main", "retail_bronze")


def test_duplicate_targets_are_deduplicated():
    first = onboarding_row()
    second = onboarding_row()
    second["data_flow_id"] = "customers-200"
    _, targets = convert(
        [first, second],
        "prod",
        "main",
        "retail_bronze",
    )
    assert len(targets) == 3


def test_missing_optional_targets_are_skipped():
    row = onboarding_row()
    row.pop("bronze_quarantine_table")
    row.pop("silver_table")
    rendered, targets = convert([row], "prod", "main", "retail_bronze")
    assert yaml.safe_load(rendered)["tables"] == {}
    assert targets == ["customers"]


def test_existing_curated_output_is_not_overwritten(tmp_path):
    source = tmp_path / "onboarding.yml"
    output = tmp_path / "tags.yml"
    source.write_text(yaml.safe_dump([onboarding_row()]), encoding="utf-8")
    output.write_text("curated: true\n", encoding="utf-8")

    exit_code = main(
        [
            "--input",
            str(source),
            "--output",
            str(output),
            "--environment",
            "prod",
            "--catalog",
            "main",
        ]
    )

    assert exit_code == 1
    assert output.read_text(encoding="utf-8") == "curated: true\n"


@pytest.mark.parametrize("suffix", [".yml", ".yaml"])
def test_load_onboarding_accepts_single_yaml_row(tmp_path, suffix):
    source = tmp_path / f"onboarding{suffix}"
    source.write_text(yaml.safe_dump(onboarding_row()), encoding="utf-8")

    assert load_onboarding(str(source)) == [onboarding_row()]


def test_load_onboarding_accepts_json_row_list(tmp_path):
    source = tmp_path / "onboarding.json"
    source.write_text(json.dumps([onboarding_row()]), encoding="utf-8")

    assert load_onboarding(str(source)) == [onboarding_row()]


@pytest.mark.parametrize(
    "suffix,contents",
    [
        (".yml", "row: 1\nrow: 2\n"),
        (".json", '{"row": 1, "row": 2}'),
    ],
)
def test_load_onboarding_rejects_duplicate_yaml_and_json_keys(
    tmp_path, suffix, contents
):
    source = tmp_path / f"onboarding{suffix}"
    source.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate key 'row'"):
        load_onboarding(str(source))


@pytest.mark.parametrize(
    "contents",
    [
        '"not a row"',
        '[{"data_flow_id": "one"}, "not a row"]',
    ],
)
def test_load_onboarding_rejects_invalid_payload_shape(tmp_path, contents):
    source = tmp_path / "onboarding.json"
    source.write_text(contents, encoding="utf-8")

    with pytest.raises(ValueError, match="row object or a list"):
        load_onboarding(str(source))


def test_resolve_onboarding_target_uses_packed_catalog():
    row = onboarding_row()
    row.pop("silver_catalog_prod")

    assert resolve_onboarding_target(row, "silver", False, "prod") == (
        "main",
        "retail_silver",
        "customers",
    )


def test_resolve_onboarding_target_rejects_conflicting_catalog():
    row = onboarding_row()
    row["silver_catalog_prod"] = "finance"

    with pytest.raises(ValueError, match="conflicts with"):
        resolve_onboarding_target(row, "silver", False, "prod")


@pytest.mark.parametrize(
    "field,value,match",
    [
        ("bronze_table", " ", "bronze_table is required"),
        (
            "bronze_database_prod",
            "main.bronze.extra",
            "must be schema or catalog.schema",
        ),
        (
            "bronze_database_prod",
            "{catalog}.bronze",
            "unresolved placeholder",
        ),
        ("bronze_catalog_prod", None, "bronze_catalog_prod is required"),
    ],
)
def test_resolve_onboarding_target_rejects_invalid_fields(field, value, match):
    row = onboarding_row()
    row[field] = value

    with pytest.raises(ValueError, match=match):
        resolve_onboarding_target(row, "bronze", False, "prod")


@pytest.mark.parametrize(
    "target,default_catalog,default_schema,expected",
    [
        (("main", "bronze", "customers"), "main", "bronze", "customers"),
        (
            ("main", "silver", "customers"),
            "main",
            "bronze",
            "silver.customers",
        ),
        (
            ("finance", "silver", "customers"),
            "main",
            "bronze",
            "finance.silver.customers",
        ),
    ],
)
def test_output_name_uses_shortest_unambiguous_target(
    target, default_catalog, default_schema, expected
):
    assert output_name(target, default_catalog, default_schema) == expected


def test_catalog_selection_rejects_catalog_without_targets():
    with pytest.raises(ValueError, match="no targets in catalog 'finance'"):
        convert([onboarding_row()], "prod", "finance")


def test_empty_onboarding_generates_empty_document_without_defaults():
    rendered, targets = convert([], "prod")
    assert yaml.safe_load(rendered) == {"version": "1", "tables": {}}
    assert targets == []


def test_cli_writes_generated_yaml_and_reports_success(tmp_path, capsys):
    source = tmp_path / "onboarding.json"
    output = tmp_path / "tags.yml"
    source.write_text(json.dumps(onboarding_row()), encoding="utf-8")

    exit_code = main(
        [
            "--input",
            str(source),
            "--output",
            str(output),
            "--environment",
            "prod",
            "--catalog",
            "main",
            "--schema",
            "retail_bronze",
        ]
    )

    assert exit_code == 0
    generated = output.read_text(encoding="utf-8")
    assert yaml.safe_load(generated)["tables"] == {}
    assert "#   customers:" in generated
    assert "Wrote 3 commented table examples" in capsys.readouterr().out


def test_cli_reports_invalid_input_without_creating_output(tmp_path, capsys):
    source = tmp_path / "onboarding.json"
    output = tmp_path / "tags.yml"
    source.write_text("{invalid json", encoding="utf-8")

    exit_code = main(
        [
            "--input",
            str(source),
            "--output",
            str(output),
            "--environment",
            "prod",
        ]
    )

    assert exit_code == 1
    assert not output.exists()
    assert "ERROR:" in capsys.readouterr().err


def test_cli_overwrite_replaces_existing_output(tmp_path):
    source = tmp_path / "onboarding.yml"
    output = tmp_path / "tags.yml"
    source.write_text(yaml.safe_dump(onboarding_row()), encoding="utf-8")
    output.write_text("curated: true\n", encoding="utf-8")

    exit_code = main(
        [
            "--input",
            str(source),
            "--output",
            str(output),
            "--environment",
            "prod",
            "--catalog",
            "main",
            "--overwrite",
        ]
    )

    assert exit_code == 0
    generated = output.read_text(encoding="utf-8")
    assert yaml.safe_load(generated)["tables"] == {}
    assert "#   retail_bronze.customers:" in generated
