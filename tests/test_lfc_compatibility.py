"""Compatibility parity for canonical Lakeflow Connect metadata authoring."""
from datetime import datetime
from pathlib import Path

import pytest

from databricks.labs.sdp_meta.lfc.bundle import (
    build_pipeline_resources,
    bundle_key,
    render_bundle_resources,
)
from databricks.labs.sdp_meta.lfc.onboarding import (
    IngestionValidationError,
    onboard_ingestion_rows,
    parse_ingestion_row,
)
from databricks.labs.sdp_meta.lfc.validation import validate


FIXED_NOW = datetime(2026, 8, 20, 12, 0, 0)
GOLDEN = Path(__file__).parent / "fixtures" / "lfc_compatibility_golden.yml"


def _row(ingestion):
    return {
        "data_flow_id": "301",
        "data_flow_group": "query-exchange.historical",
        "ingestion": ingestion,
    }


def _terse_ingestion():
    return {
        "manage_connection": False,
        "source": {
            "type": "POSTGRESQL",
            "catalog": "query_exchange",
            "connection": "query_exchange_connection",
            "slot": {"publication": "query_pub", "slot": "query_slot"},
        },
        "target": {"catalog_prod": "main", "schema": "query_history"},
        "table_defaults": {"scd_type": "SCD_TYPE_1"},
        "tables": [
            "trades",
            {
                "name": "events",
                "schema": "audit",
                "destination": "events_history",
                "scd": 2,
            },
        ],
        "schedule": {
            "quartz_cron_expression": "0 0/15 * * * ?",
            "timezone_id": "UTC",
        },
    }


def _explicit_ingestion():
    return {
        "manage_connection": False,
        "connection_name": "query_exchange_connection",
        "source": {
            "type": "POSTGRESQL",
            "catalog": "query_exchange",
            "schema": "public",
            "slot": {
                "publication_name": "query_pub",
                "slot_name": "query_slot",
            },
        },
        "target": {"catalog": "main", "schema": "query_history"},
        "table_configuration": {"scd_type": "SCD_TYPE_1"},
        "tables": [
            {"name": "trades"},
            {
                "name": "events",
                "source_schema": "audit",
                "destination_table": "events_history",
                "scd_type": "SCD_TYPE_2",
            },
        ],
        "schedule": {
            "quartz_cron_expression": "0 0/15 * * * ?",
            "timezone_id": "UTC",
        },
    }


def _parse(ingestion):
    return parse_ingestion_row(_row(ingestion), "prod", now=FIXED_NOW)


def test_terse_and_explicit_metadata_have_golden_resource_equivalence():
    for ingestion in (_terse_ingestion(), _explicit_ingestion()):
        assert validate(_row(ingestion), "prod") == ([], [])

    terse = _parse(_terse_ingestion())
    explicit = _parse(_explicit_ingestion())

    assert terse == explicit
    assert terse["manageConnection"] is False
    assert build_pipeline_resources(terse) == build_pipeline_resources(explicit)
    assert render_bundle_resources([terse]) == GOLDEN.read_text()
    assert render_bundle_resources([explicit]) == GOLDEN.read_text()


def test_partial_explicit_slot_alias_does_not_emit_null_values():
    ingestion = _explicit_ingestion()
    ingestion["source"]["slot"] = {"publication_name": "query_pub"}

    spec = _parse(ingestion)
    source_configurations = build_pipeline_resources(spec)["pipelines"][
        "lfc_query_exchange_historical_ingestion"
    ]["ingestion_definition"]["source_configurations"]

    assert source_configurations[0]["catalog"]["postgres"]["slot_config"] == {
        "publication_name": "query_pub"
    }


def test_normalized_destination_collisions_match_validation_and_onboarding():
    first = _row(_terse_ingestion())
    second = _row(_terse_ingestion())
    second["data_flow_id"] = "302"
    second["data_flow_group"] = "other"
    second["ingestion"]["target"] = {
        "catalog_prod": " MAIN ",
        "schema": " Query_History ",
    }

    errors, _ = validate([first, second], "prod")
    assert any("destination collision" in error for error in errors)
    with pytest.raises(IngestionValidationError, match="destination collision"):
        onboard_ingestion_rows([first, second], "prod")


def test_bundle_key_rejects_names_without_identifier_characters():
    with pytest.raises(ValueError, match="letters or digits"):
        bundle_key("---")
