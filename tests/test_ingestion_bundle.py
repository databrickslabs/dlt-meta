import json

from databricks.labs.sdp_meta.lfc.bundle import (
    build_bundle_resources,
    build_pipeline_resources,
    bundle_key,
    planned_files,
    render_bundle_resources,
)
from databricks.labs.sdp_meta.lfc.onboarding import parse_ingestion_row


def _spec(deploy=True):
    return parse_ingestion_row(
        {
            "data_flow_id": "300",
            "data_flow_group": "orders.prod",
            "ingestion": {
                "source": {
                    "type": "POSTGRESQL",
                    "catalog": "orders_foreign",
                    "schema": "public",
                    "connection": "orders_connection",
                    "slot": {"publication": "pub", "slot": "slot"},
                },
                "target": {"catalog": "main", "schema": "orders"},
                "gateway": {
                    "pipeline_configuration": {"gateway.setting": "true"},
                    "compute": {
                        "node_type_id": "r8i.2xlarge",
                        "autoscale": {"min_workers": 1, "max_workers": 10},
                    },
                },
                "ingestion_pipeline": {
                    "pipeline_configuration": {"ingestion.setting": "70"}
                },
                "schedule": {
                    "quartz_cron_expression": "0 0/15 * * * ?",
                    "timezone_id": "UTC",
                },
                "deploy": deploy,
                "tables": [{"name": "customers", "scd": 2}],
            },
        },
        "prod",
    )


def test_builds_native_gateway_ingestion_and_schedule_resources():
    resources = build_pipeline_resources(_spec())
    gateway = resources["pipelines"]["lfc_orders_prod_gateway"]
    ingestion = resources["pipelines"]["lfc_orders_prod_ingestion"]

    assert gateway["gateway_definition"] == {
        "connection_name": "orders_connection",
        "gateway_storage_catalog": "main",
        "gateway_storage_schema": "orders",
    }
    assert gateway["configuration"] == {"gateway.setting": "true"}
    assert gateway["clusters"][0]["node_type_id"] == "r8i.2xlarge"
    assert ingestion["ingestion_definition"]["ingestion_gateway_id"] == (
        "${resources.pipelines.lfc_orders_prod_gateway.id}"
    )
    assert ingestion["ingestion_definition"]["source_configurations"][0][
        "catalog"
    ]["postgres"]["slot_config"] == {
        "publication_name": "pub",
        "slot_name": "slot",
    }
    assert resources["jobs"]["lfc_orders_prod_schedule"]["tasks"][0][
        "pipeline_task"
    ]["pipeline_id"] == "${resources.pipelines.lfc_orders_prod_ingestion.id}"


def test_deploy_false_is_excluded_from_bundle():
    assert build_pipeline_resources(_spec(deploy=False)) == {
        "pipelines": {},
        "jobs": {},
    }
    assert build_bundle_resources([_spec(deploy=False)]) == {
        "resources": {"pipelines": {}}
    }


def test_render_is_deterministic_and_one_file_per_pair(tmp_path):
    first = render_bundle_resources([_spec()])
    second = render_bundle_resources([_spec()])
    assert first == second
    assert first.startswith("# GENERATED")

    plan = planned_files([_spec()], tmp_path)
    path, content = plan["orders_prod"]
    assert path == tmp_path / "orders_prod.ingestion.gen.yml"
    assert content == first


def test_bundle_key_normalizes_like_dab_identifiers():
    assert bundle_key("Query-Exchange.Historical") == "query_exchange_historical"


def test_pipeline_configuration_remains_string_valued():
    resources = build_pipeline_resources(_spec())
    value = resources["pipelines"]["lfc_orders_prod_ingestion"]["configuration"]
    assert json.dumps(value, sort_keys=True) == '{"ingestion.setting": "70"}'
