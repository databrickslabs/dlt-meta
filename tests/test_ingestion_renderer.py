from dataclasses import dataclass

import pytest

from databricks.labs.sdp_meta.lfc.renderer import (
    GATEWAY_ID_PLACEHOLDER,
    IngestionRenderer,
)
from databricks.labs.sdp_meta.lfc.onboarding import parse_ingestion_row


@dataclass
class SpecLike:
    name: str
    connection_name: str
    objects: list
    gateway_storage: dict
    source_type: str = "postgres"


def test_postgres_cdc_golden_render_is_pure_and_separate():
    spec = {
        "name": "orders",
        "connection_name": "postgres-prod",
        "gateway_storage": {
            "catalog": "platform",
            "schema": "connect",
            "name": "orders_gateway",
        },
        "objects": [{"table": {"source_schema": "public", "source_table": "orders"}}],
        "source_configurations": [{"catalog": {"source_catalog": "postgres"}}],
        "gateway_configuration": {"gateway.mode": "cdc"},
        "ingestion_configuration": {"pipelines.channel": "PREVIEW"},
        "gateway_compute": {
            "dbr_version": "15.4.x-scala2.12",
            "cluster_node_type": "i3.xlarge",
            "autoscale": {"min_workers": 1, "max_workers": 3},
            "apply_policy_default_values": True,
        },
        "catalog": "bronze",
        "schema": "raw",
    }

    rendered = IngestionRenderer().render(spec)

    assert rendered.gateway == {
        "name": "orders-gateway",
        "gateway_definition": {
            "connection_name": "postgres-prod",
            "gateway_storage_catalog": "platform",
            "gateway_storage_schema": "connect",
            "gateway_storage_name": "orders_gateway",
        },
        "configuration": {"gateway.mode": "cdc"},
        "continuous": True,
        "channel": "CURRENT",
        "catalog": "platform",
        "schema": "connect",
        "clusters": [
            {
                "label": "default",
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "autoscale": {"min_workers": 1, "max_workers": 3},
                "apply_policy_default_values": True,
            }
        ],
    }
    assert rendered.ingestion == {
        "name": "orders-ingestion",
        "ingestion_definition": {
            "ingestion_gateway_id": GATEWAY_ID_PLACEHOLDER,
            "source_type": "POSTGRESQL",
            "objects": [{"table": {"source_schema": "public", "source_table": "orders"}}],
            "source_configurations": [{"catalog": {"source_catalog": "postgres"}}],
        },
        "configuration": {"pipelines.channel": "PREVIEW"},
        "channel": "CURRENT",
        "catalog": "bronze",
        "schema": "raw",
    }
    assert "ingestion_definition" not in rendered.gateway
    assert "gateway_definition" not in rendered.ingestion
    assert spec["objects"][0]["table"]["source_table"] == "orders"


def test_accepts_protocol_like_object_and_defaults_gateway_continuous():
    rendered = IngestionRenderer().render(
        SpecLike(
            name="customers",
            connection_name="pg",
            objects=[],
            gateway_storage={"catalog": "meta", "schema": "connect"},
        )
    )

    assert rendered.gateway["continuous"] is True
    assert "continuous" not in rendered.ingestion
    assert rendered.ingestion["ingestion_definition"]["source_type"] == "POSTGRESQL"


def test_deploy_false_is_preserved():
    rendered = IngestionRenderer().render(
        {
            "name": "disabled",
            "connection_name": "pg",
            "objects": [],
            "deploy": False,
        }
    )
    assert rendered.deploy is False


def test_renders_persisted_ingestion_dataflowspec():
    persisted = parse_ingestion_row(
        {
            "data_flow_id": "300",
            "data_flow_group": "orders",
            "ingestion": {
                "source": {
                    "type": "POSTGRESQL",
                    "catalog": "orders_foreign",
                    "schema": "public",
                    "connection": "orders_connection",
                },
                "target": {"catalog": "main", "schema": "orders"},
                "tables": ["customers"],
            },
        },
        "prod",
    )

    rendered = IngestionRenderer().render(persisted)

    assert rendered.gateway["gateway_definition"] == {
        "connection_name": "orders_connection",
        "gateway_storage_catalog": "main",
        "gateway_storage_schema": "orders",
    }
    assert rendered.ingestion["ingestion_definition"]["ingestion_gateway_id"] == (
        GATEWAY_ID_PLACEHOLDER
    )
    assert rendered.ingestion["ingestion_definition"]["objects"][0]["table"][
        "source_table"
    ] == "customers"


def test_persisted_false_continuous_values_remain_false():
    persisted = parse_ingestion_row(
        {
            "data_flow_id": "300",
            "data_flow_group": "orders",
            "ingestion": {
                "source": {
                    "type": "POSTGRESQL",
                    "catalog": "orders_foreign",
                    "connection": "orders_connection",
                },
                "target": {"catalog": "main", "schema": "orders"},
                "gateway": {"continuous": False},
                "ingestion_pipeline": {"continuous": False},
                "tables": ["customers"],
            },
        },
        "prod",
    )
    persisted["gatewayDetails"]["continuous"] = "false"
    persisted["targetDetails"]["continuous"] = "false"

    rendered = IngestionRenderer().render(persisted)

    assert rendered.gateway["continuous"] is False
    assert rendered.ingestion["continuous"] is False


def test_configuration_values_must_remain_exact_strings():
    with pytest.raises(TypeError, match="only string"):
        IngestionRenderer().render(
            {
                "name": "bad",
                "connection_name": "pg",
                "objects": [],
                "gateway_configuration": {"pipelines.foo": True},
            }
        )


def test_rejects_non_postgres_source():
    with pytest.raises(ValueError, match="PostgreSQL CDC"):
        IngestionRenderer().render(
            {
                "name": "mysql",
                "connection_name": "mysql",
                "source_type": "MYSQL",
                "objects": [],
            }
        )
