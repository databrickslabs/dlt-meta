"""Focused parity tests for canonical Lakeflow Connect validation."""
import copy

from databricks.labs.sdp_meta.lfc.validation import (
    is_valid,
    strict_failures,
    validate,
    validate_strict,
)


def _row():
    return {
        "data_flow_id": "300",
        "data_flow_group": "postgres-orders",
        "ingestion": {
            "source": {
                "type": "POSTGRESQL",
                "catalog": "ordersdb",
                "schema": "public",
                "host_prod": "orders.example.com",
                "secret": "orders-credentials",
                "slot": {
                    "publication": "orders_publication",
                    "slot": "orders_slot",
                },
            },
            "target": {"catalog_prod": "main", "schema": "orders"},
            "gateway": {
                "pipeline_configuration": {"pipelines.gateway.setting": "true"},
                "compute": {
                    "node_type_id": "i3.xlarge",
                    "autoscale": {"min_workers": 1, "max_workers": 4},
                },
            },
            "ingestion_pipeline": {
                "pipeline_configuration": {
                    "pipelines.trigger.interval": "5 minutes"
                }
            },
            "tables": [
                {"name": "customers", "scd": 1},
                {
                    "name": "orders",
                    "configuration": {"scd_type": "SCD_TYPE_2"},
                },
            ],
        },
    }


def test_valid_rows_and_deploy_false_pass():
    row = _row()
    row["ingestion"]["deploy"] = False

    errors, warnings = validate([row], "prod")

    assert errors == []
    assert warnings == []
    assert is_valid(errors, warnings)


def test_unknown_nested_keys_warn_and_strict_mode_fails():
    row = _row()
    row["ingestion"]["tabels"] = []
    row["ingestion"]["source"]["scheam"] = "typo"
    row["ingestion"]["target"]["catlog_prod"] = "typo"
    row["ingestion"]["gateway"]["continous"] = True
    row["ingestion"]["ingestion_pipeline"]["chanell"] = "CURRENT"
    row["ingestion"]["source"]["slot"]["slto"] = "typo"
    row["ingestion"]["gateway"]["compute"]["node_typ_id"] = "typo"
    row["ingestion"]["gateway"]["compute"]["autoscale"]["min_worker"] = 1
    row["ingestion"]["tables"][0]["sdc"] = 1

    errors, warnings = validate([row], "prod")

    assert errors == []
    for typo in (
        "tabels",
        "scheam",
        "catlog_prod",
        "continous",
        "chanell",
        "slto",
        "node_typ_id",
        "min_worker",
        "sdc",
    ):
        assert any(typo in warning for warning in warnings)
    assert strict_failures(errors, warnings) == []
    assert strict_failures(errors, warnings, strict=True) == warnings
    assert not is_valid(errors, warnings, strict=True)
    assert validate_strict([row], "prod") == warnings


def test_placeholders_warn_and_only_fail_strict_validation():
    row = _row()
    row["ingestion"]["source"]["host_prod"] = "REPLACE_ME_HOST"

    errors, warnings = validate(row, "prod")

    assert errors == []
    assert any("REPLACE_ME" in warning for warning in warnings)
    assert is_valid(errors, warnings)
    assert not is_valid(errors, warnings, strict=True)


def test_duplicate_ids_tables_keys_and_destinations_are_errors():
    first = _row()
    first["ingestion"]["tables"].append("customers")
    second = copy.deepcopy(_row())
    second["data_flow_group"] = "postgres_orders"
    second["ingestion"]["target"]["catalog_prod"] = "MAIN"
    second["ingestion"]["target"]["schema"] = " Orders "

    errors, _ = validate([first, second], "prod")

    assert any("duplicate tables" in error for error in errors)
    assert any("duplicate ingestion data_flow_id" in error for error in errors)
    assert any("pipeline key collision" in error for error in errors)
    assert any("destination collision" in error for error in errors)


def test_scd_validation_covers_defaults_table_and_configuration():
    row = _row()
    row["ingestion"]["table_configuration"] = {"scd_type": "SCD_TYPE_9"}
    row["ingestion"]["tables"][0]["scd"] = 3
    row["ingestion"]["tables"][1]["configuration"]["scd_type"] = "TYPE_2"

    errors, _ = validate(row, "prod")

    assert any("table_configuration.scd_type" in error for error in errors)
    assert any(".scd: must be 1 or 2" in error for error in errors)
    assert any("configuration.scd_type" in error for error in errors)


def test_pipeline_configuration_structure_and_credentials_remain_errors():
    row = _row()
    row["ingestion"]["gateway"]["pipeline_configuration"] = {
        "objects": "not allowed",
        "pipelines.workers": 2,
    }
    row["ingestion"]["ingestion_pipeline"]["pipeline_configuration"] = {
        "oauth.client-secret": "not allowed"
    }

    errors, warnings = validate(row, "prod")

    assert warnings == []
    assert any("structural keys are not allowed" in error for error in errors)
    assert any("pass-through values must be strings" in error for error in errors)
    assert any("credential values are not allowed" in error for error in errors)
