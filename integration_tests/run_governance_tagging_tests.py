"""Run governance tagging integration tests against a Unity Catalog catalog."""

import argparse
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import yaml
from databricks.sdk import WorkspaceClient

from databricks.labs.sdp_meta.governance.tagging import applier
from databricks.labs.sdp_meta.governance.tagging.backends import SdkBackend, read_actual
from databricks.labs.sdp_meta.governance.tagging.models import OWN_EXTERNAL, OWN_SCRIPT, Key
from databricks.labs.sdp_meta.governance.tagging.state import (
    read_state,
    state_table_exists,
)
from databricks.labs.sdp_meta.identifiers import validate_uc_identifier

TABLE_NAME = "customers"
TABLE_TAG = "sdp_meta_it_classification"
COLUMN_TAG = "sdp_meta_it_sensitivity"
EXTERNAL_TAG = "sdp_meta_it_external"
TRANSFER_TAG = "sdp_meta_it_transfer"
DRIFT_TAG = "sdp_meta_it_drift"


def get_workspace_client(profile=None) -> WorkspaceClient:
    """Create a workspace client using the integration-test auth conventions."""
    if os.environ.get("DATABRICKS_APP_PORT"):
        return WorkspaceClient()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient(
        host=input("Databricks Workspace URL: "),
        token=input("Token: "),
    )


class GovernanceTaggingRunner:
    """Own and exercise an isolated Unity Catalog tagging fixture."""

    def __init__(
        self,
        workspace_client: WorkspaceClient,
        warehouse_id: str,
        catalog: str,
        keep_resources: bool = False,
    ):
        run_id = uuid4().hex[:12]
        self.catalog = validate_uc_identifier(catalog, kind="integration test catalog")
        self.schema = validate_uc_identifier(
            f"sdp_meta_tagging_it_{run_id}",
            kind="integration test schema",
        )
        self.table = (self.catalog, self.schema, TABLE_NAME)
        self.table_fqn = f"`{self.catalog}`.`{self.schema}`.`{TABLE_NAME}`"
        self.state_table = f"{self.catalog}.{self.schema}.tag_assignments"
        self.warehouse_id = warehouse_id
        self.keep_resources = keep_resources
        self.backend = SdkBackend.__new__(SdkBackend)
        self.backend.workspace = workspace_client
        self.backend.warehouse_id = warehouse_id
        self._temp_dir = tempfile.TemporaryDirectory(prefix="sdp-meta-tagging-it-")

    def sql(self, statement: str):
        return self.backend.sql(statement)

    def setup(self) -> None:
        print(f"Creating integration-test schema {self.catalog}.{self.schema}")
        self.sql(f"CREATE SCHEMA `{self.catalog}`.`{self.schema}`")
        self.sql(
            f"CREATE TABLE {self.table_fqn} "
            "(customer_id BIGINT, email STRING) USING DELTA"
        )

    def cleanup(self) -> None:
        self._temp_dir.cleanup()
        if self.keep_resources:
            print(
                "Keeping integration-test resources for inspection: "
                f"{self.catalog}.{self.schema}"
            )
            return
        print(f"Dropping integration-test schema {self.catalog}.{self.schema}")
        self.sql(f"DROP SCHEMA IF EXISTS `{self.catalog}`.`{self.schema}` CASCADE")

    def _write_tags(
        self,
        table_tags=None,
        column_tags=None,
        table_name=TABLE_NAME,
    ) -> str:
        node = {}
        if table_tags:
            node["table"] = table_tags
        if column_tags:
            node["columns"] = column_tags
        document = {
            "version": "1",
            "source_id": "governance-integration-test",
            "defaults": {
                "catalog": self.catalog,
                "schema": self.schema,
            },
            "tables": {table_name: node},
        }
        path = Path(self._temp_dir.name) / f"tags-{uuid4().hex}.yml"
        path.write_text(
            yaml.safe_dump(document, sort_keys=False),
            encoding="utf-8",
        )
        return str(path)

    def _apply(self, tags_file: str, *extra_args: str) -> int:
        argv = [
            "--tags-file",
            tags_file,
            "--state-table",
            self.state_table,
            "--warehouse-id",
            self.warehouse_id,
            *extra_args,
        ]
        with patch.object(applier, "make_backend", return_value=self.backend):
            return applier.main(argv)

    def _actual(self):
        return read_actual(self.backend, {self.table})

    def _state(self):
        return read_state(self.backend, self.state_table, {self.table})

    def _table_key(self, tag_key: str) -> Key:
        return Key(self.catalog, self.schema, TABLE_NAME, None, tag_key)

    def _column_key(self, tag_key: str) -> Key:
        return Key(self.catalog, self.schema, TABLE_NAME, "email", tag_key)

    def _set_table_tag(self, tag_key: str, value: str) -> None:
        self.sql(
            f"ALTER TABLE {self.table_fqn} SET TAGS "
            f"('{tag_key}' = '{value}')"
        )

    def test_dry_run_and_managed_lifecycle(self) -> None:
        print("TEST: dry-run, initial apply, idempotency, and managed update")
        tags_file = self._write_tags(
            table_tags={TABLE_TAG: "restricted"},
            column_tags={"email": {COLUMN_TAG: "pii"}},
        )

        assert self._apply(tags_file, "--dry-run") == 0
        assert not state_table_exists(self.backend, self.state_table)
        assert self._table_key(TABLE_TAG) not in self._actual()
        assert self._column_key(COLUMN_TAG) not in self._actual()

        assert self._apply(tags_file) == 0
        actual = self._actual()
        assert actual[self._table_key(TABLE_TAG)] == "restricted"
        assert actual[self._column_key(COLUMN_TAG)] == "pii"
        state = self._state()
        assert state[self._table_key(TABLE_TAG)]["ownership"] == OWN_SCRIPT
        assert state[self._table_key(TABLE_TAG)]["status"] == "applied"
        assert state[self._column_key(COLUMN_TAG)]["ownership"] == OWN_SCRIPT

        assert self._apply(tags_file) == 0
        assert self._actual() == actual

        updated_file = self._write_tags(
            table_tags={TABLE_TAG: "confidential"},
            column_tags={"email": {COLUMN_TAG: "pii"}},
        )
        assert self._apply(updated_file) == 0
        assert self._actual()[self._table_key(TABLE_TAG)] == "confidential"
        assert (
            self._state()[self._table_key(TABLE_TAG)]["last_applied_value"]
            == "confidential"
        )

    def test_external_preservation_conflict_and_transfer(self) -> None:
        print("TEST: external preservation, conflict, and ownership transfer")
        self._set_table_tag(EXTERNAL_TAG, "approved")
        tags_file = self._write_tags(
            table_tags={
                TABLE_TAG: "confidential",
                EXTERNAL_TAG: "approved",
            },
            column_tags={"email": {COLUMN_TAG: "pii"}},
        )
        assert self._apply(tags_file) == 0
        external_key = self._table_key(EXTERNAL_TAG)
        assert self._state()[external_key]["ownership"] == OWN_EXTERNAL

        self._set_table_tag(TRANSFER_TAG, "manual")
        conflict_file = self._write_tags(
            table_tags={
                TABLE_TAG: "confidential",
                EXTERNAL_TAG: "approved",
                TRANSFER_TAG: "reviewed",
            },
            column_tags={"email": {COLUMN_TAG: "pii"}},
        )
        assert self._apply(conflict_file) == 2
        transfer_key = self._table_key(TRANSFER_TAG)
        assert self._actual()[transfer_key] == "manual"
        assert transfer_key not in self._state()

        assert self._apply(conflict_file, "--transfer-ownership") == 0
        assert self._actual()[transfer_key] == "reviewed"
        assert self._state()[transfer_key]["ownership"] == OWN_SCRIPT

    def test_stale_cleanup_and_external_drift(self) -> None:
        print("TEST: stale cleanup, external forget, and drift preservation")
        empty_file = self._write_tags()
        assert self._apply(empty_file) == 0
        actual = self._actual()
        assert self._table_key(TABLE_TAG) not in actual
        assert self._column_key(COLUMN_TAG) not in actual
        assert self._table_key(TRANSFER_TAG) not in actual
        assert actual[self._table_key(EXTERNAL_TAG)] == "approved"
        assert self._state() == {}

        managed_file = self._write_tags(table_tags={DRIFT_TAG: "managed"})
        assert self._apply(managed_file) == 0
        drift_key = self._table_key(DRIFT_TAG)
        assert self._state()[drift_key]["ownership"] == OWN_SCRIPT

        self._set_table_tag(DRIFT_TAG, "manually_changed")
        assert self._apply(empty_file) == 2
        assert self._actual()[drift_key] == "manually_changed"
        assert self._state()[drift_key]["ownership"] == OWN_SCRIPT

    def test_preflight_failures_do_not_mutate_state(self) -> None:
        print("TEST: missing table and column preflight failures")
        state_before = self._state()

        missing_table_file = self._write_tags(
            table_tags={TABLE_TAG: "restricted"},
            table_name="missing_table",
        )
        assert self._apply(missing_table_file) == 1

        missing_column_file = self._write_tags(
            column_tags={"missing_column": {COLUMN_TAG: "pii"}},
        )
        assert self._apply(missing_column_file) == 1

        assert self._state() == state_before

    def run(self) -> None:
        try:
            self.setup()
            self.test_dry_run_and_managed_lifecycle()
            self.test_external_preservation_conflict_and_transfer()
            self.test_stale_cleanup_and_external_drift()
            self.test_preflight_failures_do_not_mutate_state()
            print("PASS: governance tagging integration tests completed")
        finally:
            self.cleanup()


def process_arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--uc-catalog-name",
        "--uc_catalog_name",
        dest="uc_catalog_name",
        required=True,
        help="Unity Catalog catalog where isolated test resources are created",
    )
    parser.add_argument(
        "--warehouse-id",
        required=True,
        help="running SQL warehouse used for fixture and tag SQL",
    )
    parser.add_argument(
        "--profile",
        help="Databricks CLI profile; omit in a Databricks Apps runtime",
    )
    parser.add_argument(
        "--keep-resources",
        action="store_true",
        help="keep the isolated test schema after the run for debugging",
    )
    return parser.parse_args()


def main() -> None:
    args = process_arguments()
    workspace_client = get_workspace_client(args.profile)
    runner = GovernanceTaggingRunner(
        workspace_client=workspace_client,
        warehouse_id=args.warehouse_id,
        catalog=args.uc_catalog_name,
        keep_resources=args.keep_resources,
    )
    runner.run()


if __name__ == "__main__":
    main()
