"""Focused ingestion onboarding parity tests."""
import dataclasses
import json
from pathlib import Path
from unittest.mock import patch

import pytest

from databricks.labs.sdp_meta.lfc.models import IngestionDataflowSpec
from databricks.labs.sdp_meta.lfc.onboarding import (
    IngestionValidationError,
    prepare_onboarding_rows,
)
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
from tests.utils import SDPFrameworkTestCase


def _ingestion_row():
    return {
        "data_flow_id": "ingest-orders",
        "data_flow_group": "orders",
        "ingestion": {
            "manage_connection": False,
            "connection_name": "orders_connection",
            "source": {
                "type": "POSTGRESQL",
                "catalog": "orders_source",
                "schema": "public",
            },
            "target": {"catalog": "main", "schema": "orders_landing"},
            "tables": ["customers"],
        },
    }


def _ref_row(**fields):
    row = {
        "data_flow_id": "consume-orders",
        "data_flow_group": "orders",
        "ingestion_ref": {
            "data_flow_id": "ingest-orders",
            "table": "customers",
        },
    }
    row.update(fields)
    return row


def test_ingestion_ref_routes_all_supported_topologies():
    bronze = _ref_row(
        bronze_database_prod="bronze",
        bronze_table="customers",
    )
    silver = _ref_row(
        data_flow_id="silver-direct",
        silver_database_prod="silver",
        silver_table="customers",
    )
    chain = _ref_row(
        data_flow_id="full-chain",
        bronze_database_prod="bronze",
        bronze_table="customers",
        silver_database_prod="silver",
        silver_table="customers",
    )

    result = prepare_onboarding_rows(
        [_ingestion_row(), bronze, silver, chain], "prod"
    )
    by_id = {row["data_flow_id"]: row for row in result.rows}

    assert "source_details" not in by_id["ingest-orders"]
    assert by_id["consume-orders"]["source_format"] == "delta"
    assert by_id["consume-orders"]["source_details"] == {
        "source_database": "orders_landing",
        "source_table": "customers",
        "source_catalog_prod": "main",
    }
    assert "source_details" not in by_id["silver-direct"]
    assert by_id["silver-direct"]["bronze_database_prod"] == "orders_landing"
    assert by_id["silver-direct"]["bronze_table"] == "customers"
    assert by_id["silver-direct"]["bronze_catalog_prod"] == "main"
    assert by_id["full-chain"]["bronze_database_prod"] == "bronze"
    assert by_id["full-chain"]["source_details"]["source_database"] == (
        "orders_landing"
    )


def test_all_references_fail_before_rows_are_returned():
    missing_a = _ref_row()
    missing_b = _ref_row(data_flow_id="other")
    missing_a["ingestion_ref"]["table"] = "missing-a"
    missing_b["ingestion_ref"]["table"] = "missing-b"

    with pytest.raises(IngestionValidationError) as caught:
        prepare_onboarding_rows(
            [_ingestion_row(), missing_a, missing_b], "prod"
        )

    assert "row[1]" in str(caught.value)
    assert "row[2]" in str(caught.value)


class TestIngestionPersistence(SDPFrameworkTestCase):
    def _params(self, onboarding_path, ingestion_path, overwrite="True"):
        return {
            "onboarding_file_path": str(onboarding_path),
            "database": "ravi_dlt_demo",
            "env": "prod",
            "ingestion_dataflowspec_table": "ingestion_dataflowspec",
            "ingestion_dataflowspec_path": str(ingestion_path),
            "import_author": "integration-test",
            "version": "v1",
            "overwrite": overwrite,
        }

    def test_path_backed_persistence_has_exact_schema_and_merges(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "lfc.json"
        ingestion_path = root / "ingestion"
        onboarding.write_text(json.dumps([_ingestion_row()]))

        first = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, ingestion_path),
            uc_enabled=False,
        )
        first.onboard_ingestion_dataflow_spec()

        persisted = self.spark.read.format("delta").load(str(ingestion_path))
        expected = [
            field.name for field in dataclasses.fields(IngestionDataflowSpec)
        ]
        self.assertEqual(persisted.columns, expected)
        self.assertEqual(persisted.count(), 1)

        changed = _ingestion_row()
        changed["data_flow_group"] = "orders-updated"
        onboarding.write_text(json.dumps([changed]))
        second = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, ingestion_path, overwrite="False"),
            uc_enabled=False,
        )
        second.onboard_ingestion_dataflow_spec()

        merged = self.spark.read.format("delta").load(str(ingestion_path))
        self.assertEqual(merged.count(), 1)
        self.assertEqual(merged.collect()[0]["dataFlowGroup"], "orders-updated")

    def test_path_backed_first_incremental_write_bootstraps_delta_table(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "lfc-incremental.json"
        ingestion_path = root / "ingestion-incremental"
        onboarding.write_text(json.dumps([_ingestion_row()]))

        onboarder = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, ingestion_path, overwrite="False"),
            uc_enabled=False,
        )
        onboarder.onboard_ingestion_dataflow_spec()

        persisted = self.spark.read.format("delta").load(
            str(ingestion_path)
        )
        self.assertEqual(persisted.count(), 1)

    def test_zero_ingestion_rows_is_a_clean_noop(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "legacy.json"
        ingestion_path = root / "not-created"
        onboarding.write_text(json.dumps([{
            "data_flow_id": "legacy",
            "data_flow_group": "legacy",
        }]))

        onboarder = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, ingestion_path),
            uc_enabled=False,
        )
        onboarder.onboard_ingestion_dataflow_spec()

        self.assertFalse(ingestion_path.exists())

    def test_prepared_resolved_json_is_cleaned_up_after_onboarding(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "lfc-cleanup.json"
        onboarding.write_text(json.dumps([_ingestion_row()]))
        onboarder = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, root / "ingestion-cleanup"),
            uc_enabled=False,
        )

        onboarder.onboard_ingestion_dataflow_spec()

        # The reference-resolved sibling file is a working artifact; it must
        # not survive next to the user's onboarding config.
        self.assertFalse(
            (root / "lfc-cleanup_ingestion_resolved.json").exists()
        )

    def test_ingestion_preparation_is_cached_per_onboarding_file(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "lfc-cache.json"
        onboarding.write_text(json.dumps([_ingestion_row()]))
        onboarder = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, root / "ingestion-cache"),
            uc_enabled=False,
        )
        read = onboarder._OnboardDataflowspec__get_onboarding_file_dataframe

        with patch(
            "databricks.labs.sdp_meta.onboard_dataflowspec."
            "prepare_onboarding_rows",
            wraps=prepare_onboarding_rows,
        ) as prepare:
            read(str(onboarding))
            read(str(onboarding))

        self.assertEqual(prepare.call_count, 1)

    def test_legacy_file_skips_ingestion_preparation(self):
        root = Path(self.onboarding_spec_paths)
        onboarding = root / "legacy-no-ingestion.json"
        onboarding.write_text(json.dumps([{
            "data_flow_id": "legacy",
            "data_flow_group": "legacy",
        }]))
        onboarder = OnboardDataflowspec(
            self.spark,
            self._params(onboarding, root / "unused-ingestion"),
            uc_enabled=False,
        )
        read = onboarder._OnboardDataflowspec__get_onboarding_file_dataframe

        with patch(
            "databricks.labs.sdp_meta.onboard_dataflowspec."
            "prepare_onboarding_rows",
            wraps=prepare_onboarding_rows,
        ) as prepare:
            read(str(onboarding))

        prepare.assert_not_called()
