"""Tests for pure Lakeflow Connect ingestion onboarding."""
import json
from datetime import datetime

import pytest

from databricks.labs.sdp_meta.lfc.models import (
    IngestionDataflowSpec,
    IngestionDataflowSpecUtils,
)
from databricks.labs.sdp_meta.lfc.onboarding import (
    IngestionOnboarder,
    IngestionValidationError,
    build_ingestion_registry,
    onboard_ingestion_rows,
    parse_ingestion_row,
    prepare_onboarding_rows,
    resolve_environment_field,
    resolve_ingestion_ref,
    resolve_row_ingestion_ref,
    validate_configuration,
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
                "host_prod": "orders-db.example.com",
                "port": 5432,
                "secret": "company/prod/ordersdb",
                "slot": {
                    "publication": "databricks_publication",
                    "slot": "databricks_slot",
                },
            },
            "target": {"catalog_prod": "main", "schema": "orders"},
            "gateway": {
                "storage_catalog_prod": "main",
                "storage_schema": "lfc_staging",
                "pipeline_configuration": {
                    "pipelines.gateway.mapUuidToString": "true",
                    "pipelines.qbcNumSnapshotSplit": "10",
                },
                "compute": {
                    "node_type_id": "r8i.2xlarge",
                    "autoscale": {"min_workers": 1, "max_workers": 10},
                },
            },
            "ingestion_pipeline": {
                "pipeline_configuration": {
                    "pipelines.trigger.interval": "15 minutes",
                }
            },
            "schedule": {
                "quartz_cron_expression": "0 0/15 * * * ?",
                "timezone_id": "UTC",
            },
            "tables": [
                {"name": "customers", "scd": 1},
                {
                    "name": "payments",
                    "destination": "payments_raw",
                    "scd": 2,
                    "configuration": {"primary_keys": ["payment_id"]},
                },
            ],
        },
    }


def test_parse_ingestion_row_builds_persistence_ready_spec():
    now = datetime(2026, 8, 20, 12, 0, 0)
    result = parse_ingestion_row(
        _row(),
        "prod",
        version="7",
        created_by="tester",
        now=now,
    )

    assert IngestionDataflowSpec(**result).dataFlowId == "300"
    assert result["connectionName"] == "postgres-orders_connection"
    assert result["manageConnection"] is True
    assert result["gatewayDetails"] == {
        "name": "postgres-orders_gateway",
        "storageCatalog": "main",
        "storageSchema": "lfc_staging",
        "continuous": True,
        "channel": "CURRENT",
    }
    assert result["targetDetails"] == {
        "catalog": "main",
        "schema": "orders",
        "name": "postgres-orders_ingestion",
        "channel": "CURRENT",
    }
    assert result["version"] == "7"
    assert result["createDate"] == now
    assert result["updateDate"] == now

    connection = json.loads(result["connectionSpec"])
    assert connection == {
        "host": "orders-db.example.com",
        "port": 5432,
        "secret": "company/prod/ordersdb",
    }
    source_configurations = json.loads(result["sourceConfigurations"])
    slot_config = source_configurations[0]["catalog"]["postgres"]["slot_config"]
    assert slot_config == {
        "publication_name": "databricks_publication",
        "slot_name": "databricks_slot",
    }
    objects = json.loads(result["objects"])
    assert len(objects) == 2
    assert objects[0]["table"]["table_configuration"]["scd_type"] == "SCD_TYPE_1"
    assert objects[1]["table"]["destination_table"] == "payments_raw"
    assert objects[1]["table"]["table_configuration"]["primary_keys"] == [
        "payment_id"
    ]
    assert json.loads(result["gatewayPipelineConfiguration"])[
        "pipelines.qbcNumSnapshotSplit"
    ] == "10"


def test_explicit_connection_name_can_disable_connection_management():
    row = _row()
    row["ingestion"]["manage_connection"] = False
    row["ingestion"]["connection_name"] = "existing_connection"
    del row["ingestion"]["source"]["host_prod"]
    result = parse_ingestion_row(row, "prod")
    assert result["connectionName"] == "existing_connection"
    assert result["manageConnection"] is False


def test_source_connection_alias_is_used_for_existing_connection():
    row = _row()
    row["ingestion"]["manage_connection"] = False
    row["ingestion"]["source"]["connection"] = "orders_connection"
    del row["ingestion"]["source"]["host_prod"]
    result = parse_ingestion_row(row, "prod")
    assert result["connectionName"] == "orders_connection"


def test_unmanaged_connection_requires_an_explicit_name():
    row = _row()
    row["ingestion"]["manage_connection"] = False
    del row["ingestion"]["source"]["host_prod"]

    with pytest.raises(
        IngestionValidationError,
        match="connection_name.*required",
    ):
        parse_ingestion_row(row, "prod")


def test_environment_field_prefers_suffix_and_falls_back():
    values = {"catalog": "fallback", "catalog_prod": "main"}
    assert resolve_environment_field(values, "catalog", "prod") == "main"
    assert resolve_environment_field(values, "catalog", "dev") == "fallback"


def test_duplicate_ingestion_destination_is_rejected():
    first = _row()
    second = _row()
    second["data_flow_id"] = "301"
    second["data_flow_group"] = "another-orders"
    with pytest.raises(IngestionValidationError, match="destination collision"):
        onboard_ingestion_rows([first, second], "prod")


def test_normalized_pipeline_key_collision_is_rejected():
    first = _row()
    second = _row()
    second["data_flow_id"] = "301"
    second["data_flow_group"] = "postgres_orders"
    second["ingestion"]["target"]["schema"] = "other_orders"
    with pytest.raises(IngestionValidationError, match="pipeline key collision"):
        onboard_ingestion_rows([first, second], "prod")


def test_tables_star_expands_to_schema_object():
    row = _row()
    row["ingestion"]["tables"] = "*"
    objects = json.loads(parse_ingestion_row(row, "prod")["objects"])
    assert objects == [{
        "schema": {
            "source_catalog": "ordersdb",
            "source_schema": "public",
            "destination_catalog": "main",
            "destination_schema": "orders",
        }
    }]


def test_table_name_shorthand_uses_same_destination():
    row = _row()
    row["ingestion"]["tables"] = ["customers"]
    table = json.loads(parse_ingestion_row(row, "prod")["objects"])[0]["table"]
    assert table["source_table"] == "customers"
    assert table["destination_table"] == "customers"


def test_non_postgres_source_type_is_rejected_at_onboarding_time():
    row = _row()
    row["ingestion"]["source"]["type"] = "MYSQL"

    with pytest.raises(
        IngestionValidationError, match="only PostgreSQL CDC is supported"
    ):
        parse_ingestion_row(row, "prod")


@pytest.mark.parametrize(
    "source_type", ["POSTGRESQL", "postgresql", "postgres", "POSTGRES-CDC"]
)
def test_postgres_source_type_spellings_are_accepted(source_type):
    row = _row()
    row["ingestion"]["source"]["type"] = source_type

    spec = parse_ingestion_row(row, "prod")

    assert spec["sourceType"] == source_type


def test_validation_aggregates_errors_before_returning_specs():
    row = _row()
    del row["data_flow_id"]
    del row["ingestion"]["source"]["type"]
    del row["ingestion"]["target"]["catalog_prod"]
    row["ingestion"]["tables"] = []
    row["ingestion"]["deploy"] = "yes"

    with pytest.raises(IngestionValidationError) as caught:
        onboard_ingestion_rows([row], "prod")

    message = str(caught.value)
    assert "data_flow_id" in message
    assert "ingestion.source.type" in message
    assert "ingestion.target.catalog" in message
    assert "ingestion.tables" in message
    assert "ingestion.deploy" in message


@pytest.mark.parametrize(
    "configuration, expected",
    [
        ({"pipelines.workers": 2}, "must be strings"),
        ({"password": "unsafe"}, "credential values"),
        ({"db_password": "unsafe"}, "credential values"),
        ({"postgres_api_key": "unsafe"}, "credential values"),
        ({"oauth.client-secret": "unsafe"}, "credential values"),
        ({"objects": "unsafe"}, "structural keys"),
    ],
)
def test_pipeline_configuration_rejects_unsafe_values(
    configuration, expected
):
    errors = validate_configuration(configuration)
    assert any(expected in error for error in errors)


def test_registry_and_ingestion_ref_resolve_existing_source_details_shape():
    spec = parse_ingestion_row(_row(), "prod")
    registry = build_ingestion_registry([spec])
    source_details = resolve_ingestion_ref(
        {"data_flow_id": "300", "table": "customers"},
        registry,
    )
    assert source_details == {
        "catalog": "main",
        "database": "orders",
        "table": "customers",
    }

    consumer = {
        "data_flow_id": "301",
        "ingestion_ref": {"data_flow_id": "300", "table": "customers"},
    }
    resolved = resolve_row_ingestion_ref(consumer, registry)
    assert "ingestion_ref" not in resolved
    assert resolved["sourceDetails"] == source_details
    assert "ingestion_ref" in consumer


def test_registry_rejects_duplicate_data_flow_table_keys():
    spec = parse_ingestion_row(_row(), "prod")
    with pytest.raises(IngestionValidationError, match="duplicate"):
        build_ingestion_registry([spec, spec])


def test_explicit_invalid_scd_type_is_rejected_by_onboarding():
    row = _row()
    row["ingestion"]["tables"][0]["scd_type"] = "SCD_TYPE_9"

    with pytest.raises(IngestionValidationError, match="SCD_TYPE_1"):
        parse_ingestion_row(row, "prod")


def test_prepare_rows_resolves_reference_from_persisted_specs():
    persisted = parse_ingestion_row(_row(), "prod")
    consumer = {
        "data_flow_id": "301",
        "data_flow_group": "silver-orders",
        "ingestion_ref": {"data_flow_id": "300", "table": "customers"},
        "silver_database_prod": "silver",
        "silver_table": "customers",
    }

    prepared = prepare_onboarding_rows(
        [consumer],
        "prod",
        persisted_ingestion_specs=[persisted],
    )

    assert prepared.rows[0]["bronze_database_prod"] == "orders"
    assert prepared.rows[0]["bronze_table"] == "customers"


def test_ingestion_ref_rejects_explicit_bronze_source_fields():
    consumer = {
        "data_flow_id": "301",
        "data_flow_group": "bronze-orders",
        "ingestion_ref": {"data_flow_id": "300", "table": "customers"},
        "source_format": "delta",
        "source_details": {"source_table": "other"},
    }

    with pytest.raises(IngestionValidationError, match="mutually exclusive"):
        prepare_onboarding_rows(
            [consumer],
            "prod",
            persisted_ingestion_specs=[parse_ingestion_row(_row(), "prod")],
        )


def test_missing_ingestion_ref_reports_requested_key():
    registry = build_ingestion_registry([parse_ingestion_row(_row(), "prod")])
    with pytest.raises(IngestionValidationError, match="missing"):
        resolve_ingestion_ref(
            {"data_flow_id": "300", "table": "missing"},
            registry,
        )


def test_onboarder_facade_and_multi_row_validation():
    facade = IngestionOnboarder(
        "prod", version="2", created_by="unit-test"
    )
    result = facade.parse_rows([_row()])
    assert len(result.ingestion_specs) == 1
    assert result.ingestion_specs[0]["version"] == "2"
    assert ("300", "customers") in result.ingestion_registry


def test_dataflow_spec_json_accessors_are_backward_safe():
    spec_dict = parse_ingestion_row(_row(), "prod")
    spec = IngestionDataflowSpecUtils.ingestion_spec_from_row(spec_dict)
    assert isinstance(spec, IngestionDataflowSpec)
    assert len(
        IngestionDataflowSpecUtils.get_ingestion_json(spec, "objects", list)
    ) == 2
    assert (
        IngestionDataflowSpecUtils.parse_json_field(
            None, "optional", dict
        )
        is None
    )
    assert IngestionDataflowSpecUtils.parse_json_field(
        {"already": "parsed"}, "field", dict
    ) == {"already": "parsed"}
    with pytest.raises(ValueError, match="must contain JSON dict"):
        IngestionDataflowSpecUtils.parse_json_field("[]", "field", dict)
    with pytest.raises(ValueError, match="not an ingestion JSON field"):
        IngestionDataflowSpecUtils.get_ingestion_json(spec, "sourceType")
