from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.sdp_meta.cli import SDPMeta, deploy
from databricks.labs.sdp_meta.lfc.deployer import (
    CREATE,
    NOOP,
    PRUNE,
    UPDATE,
    DeploymentState,
    PartialDeploymentError,
)
from databricks.labs.sdp_meta.lfc.sdk_service import (
    IngestionReconcileResult,
    IngestionSdkService,
)
from databricks.labs.sdp_meta.lfc.state import StoredDeploymentState


def spec(
    schema="raw",
    deploy_enabled=True,
    data_flow_id="300",
    group="orders",
):
    return {
        "dataFlowId": data_flow_id,
        "dataFlowGroup": group,
        "sourceType": "POSTGRESQL",
        "connectionName": "orders_connection",
        "gatewayDetails": {
            "name": f"{group}-gateway",
            "storageCatalog": "main",
            "storageSchema": "connect",
        },
        "sourceConfigurations": "[]",
        "objects": "[]",
        "targetDetails": {
            "name": f"{group}-ingestion",
            "catalog": "main",
            "schema": schema,
        },
        "deploy": deploy_enabled,
        "gatewayPipelineConfiguration": "{}",
        "ingestionPipelineConfiguration": "{}",
        "gatewayCompute": "{}",
        "version": "7",
    }


class Pipelines:
    def __init__(self, existing=None):
        self.existing = list(existing or [])
        self.calls = []
        self.list_calls = 0
        self.next_id = 1

    def list_pipelines(self):
        self.list_calls += 1
        return list(self.existing)

    def create(self, **definition):
        pipeline_id = f"pipeline-{self.next_id}"
        self.next_id += 1
        self.calls.append(("create", definition["name"]))
        self.existing.append(
            SimpleNamespace(
                pipeline_id=pipeline_id,
                name=definition["name"],
                spec=dict(definition),
            )
        )
        return SimpleNamespace(pipeline_id=pipeline_id)

    def update(self, pipeline_id, **definition):
        self.calls.append(("update", pipeline_id, definition["name"]))
        for pipeline in self.existing:
            if pipeline.pipeline_id == pipeline_id:
                pipeline.spec = dict(definition)

    def get(self, pipeline_id):
        for pipeline in self.existing:
            if pipeline.pipeline_id == pipeline_id:
                return SimpleNamespace(spec=getattr(pipeline, "spec", {}))
        raise KeyError(pipeline_id)

    def delete(self, pipeline_id):
        self.calls.append(("delete", pipeline_id))


class Workspace:
    def __init__(self, existing=None):
        self.pipelines = Pipelines(existing)


class Repository:
    def __init__(self, specs=None, states=None, all_ids=None):
        self.specs = list(specs or [])
        self.states = list(states or [])
        self.all_ids = (
            list(all_ids)
            if all_ids is not None
            else [str(row["dataFlowId"]) for row in self.specs]
        )
        self.ensure_calls = 0
        self.saved = []
        self.deleted = []
        self.locks = {}

    def read_specs(self, data_flow_group=None, data_flow_ids=None):
        rows = self.specs
        if data_flow_group:
            rows = [
                row
                for row in rows
                if row["dataFlowGroup"] == data_flow_group
            ]
        if data_flow_ids:
            rows = [
                row
                for row in rows
                if str(row["dataFlowId"]) in set(data_flow_ids)
            ]
        return rows

    def read_states(self, allow_missing=False):
        return list(self.states)

    def read_all_spec_ids(self):
        return list(self.all_ids)

    def ensure_state_table(self):
        self.ensure_calls += 1

    def save_state(self, data_flow_id, state):
        self.saved.append((data_flow_id, state))
        self.states = [
            row for row in self.states if row.data_flow_id != data_flow_id
        ]
        self.states.append(StoredDeploymentState(data_flow_id, state))

    def acquire_lock(self, data_flow_id, owner_token):
        if data_flow_id in self.locks:
            return False
        self.locks[data_flow_id] = owner_token
        return True

    def release_lock(self, data_flow_id, owner_token):
        if self.locks.get(data_flow_id) == owner_token:
            del self.locks[data_flow_id]

    def delete_states(self, data_flow_ids):
        self.deleted.extend(data_flow_ids)


def test_first_create_then_repeat_noop_and_changed_update():
    workspace = Workspace()
    repo = Repository([spec()])
    service = IngestionSdkService(workspace, repo)

    created = service.reconcile()
    repeated = service.reconcile()
    repo.specs = [spec(schema="changed")]
    updated = service.reconcile()

    assert [action.action for action in created.actions] == [CREATE, CREATE]
    assert [action.action for action in repeated.actions] == [NOOP, NOOP]
    assert [action.action for action in updated.actions] == [UPDATE, UPDATE]
    assert [call[0] for call in workspace.pipelines.calls] == [
        "create",
        "create",
        "update",
        "update",
    ]
    assert len(repo.saved) == 4
    assert repo.ensure_calls == 2
    assert workspace.pipelines.list_calls == 3


def test_multiple_specs_share_one_pipeline_inventory_snapshot():
    workspace = Workspace()
    repo = Repository([
        spec(data_flow_id="300", group="orders"),
        spec(data_flow_id="301", group="customers"),
    ])

    result = IngestionSdkService(workspace, repo).reconcile()

    assert [action.action for action in result.actions] == [
        CREATE,
        CREATE,
        CREATE,
        CREATE,
    ]
    assert workspace.pipelines.list_calls == 1


def test_duplicate_rendered_pipeline_names_fail_before_mutation():
    workspace = Workspace()
    repo = Repository([
        spec(data_flow_id="300", group="shared"),
        spec(data_flow_id="301", group="shared"),
    ])

    with pytest.raises(ValueError, match="globally unique"):
        IngestionSdkService(workspace, repo).reconcile()

    assert workspace.pipelines.calls == []
    assert repo.ensure_calls == 0


def test_active_flow_lock_refuses_concurrent_mutation():
    workspace = Workspace()
    repo = Repository([spec()])
    repo.locks["300"] = "other-owner"

    with pytest.raises(RuntimeError, match="another ingestion reconciliation"):
        IngestionSdkService(workspace, repo).reconcile()

    assert workspace.pipelines.calls == []


def test_pending_state_recovers_tagged_creates_after_final_save_failure():
    workspace = Workspace()

    class FailFinalSaveRepository(Repository):
        fail_final_save = True

        def save_state(self, data_flow_id, state):
            if state.status == "deployed" and self.fail_final_save:
                self.fail_final_save = False
                raise RuntimeError("state write failed")
            super().save_state(data_flow_id, state)

    repo = FailFinalSaveRepository([spec()])
    service = IngestionSdkService(workspace, repo)

    with pytest.raises(RuntimeError, match="state write failed"):
        service.reconcile()

    assert repo.states[0].state.status == "pending"
    recovered = service.reconcile()

    assert [action.action for action in recovered.actions] == [UPDATE, UPDATE]
    assert repo.states[0].state.status == "deployed"


def test_live_pipeline_drift_forces_update_with_matching_fingerprint():
    workspace = Workspace()
    repo = Repository([spec()])
    service = IngestionSdkService(workspace, repo)
    service.reconcile()
    workspace.pipelines.existing[0].spec["channel"] = "PREVIEW"

    result = service.reconcile()

    assert [action.action for action in result.actions] == [UPDATE, NOOP]


def test_partial_create_saves_gateway_ownership_and_retry_recovers():
    workspace = Workspace()
    repo = Repository([spec()])
    service = IngestionSdkService(workspace, repo)
    create = workspace.pipelines.create

    def fail_ingestion(**definition):
        if "ingestion_definition" in definition:
            raise RuntimeError("transient ingestion failure")
        return create(**definition)

    workspace.pipelines.create = fail_ingestion
    with pytest.raises(PartialDeploymentError):
        service.reconcile()

    assert repo.saved[-1][1].gateway_pipeline_id == "pipeline-1"
    assert repo.saved[-1][1].ingestion_pipeline_id is None
    assert repo.saved[-1][1].fingerprint == ""

    workspace.pipelines.create = create
    recovered = service.reconcile()

    assert [action.action for action in recovered.actions] == [UPDATE, CREATE]


def test_same_name_unmanaged_drift_fails_before_mutation():
    workspace = Workspace(
        [
            SimpleNamespace(
                pipeline_id="other-gateway", name="orders-gateway"
            ),
            SimpleNamespace(
                pipeline_id="other-ingestion", name="orders-ingestion"
            ),
        ]
    )
    repo = Repository([spec()])

    with pytest.raises(RuntimeError, match="ownership drift"):
        IngestionSdkService(workspace, repo).reconcile()

    assert workspace.pipelines.calls == []
    assert repo.ensure_calls == 0
    assert repo.saved == []


def test_dry_run_and_deploy_false_do_not_write_or_create():
    workspace = Workspace()
    repo = Repository([spec()])
    service = IngestionSdkService(workspace, repo)

    planned = service.reconcile(dry_run=True)
    repo.specs = [spec(deploy_enabled=False)]
    skipped = service.reconcile()

    assert [action.action for action in planned.actions] == [CREATE, CREATE]
    assert [action.action for action in skipped.actions] == ["skip", "skip"]
    assert workspace.pipelines.calls == []
    assert repo.saved == []
    assert repo.ensure_calls == 0


def test_no_implicit_delete_and_explicit_prune_is_owned_and_dry_run_safe():
    stale = StoredDeploymentState(
        "old",
        DeploymentState(
            "owned-gateway", "owned-ingestion", "1", "fingerprint", "deployed"
        ),
    )
    workspace = Workspace()
    repo = Repository([], [stale], all_ids=[])
    service = IngestionSdkService(workspace, repo)

    without_prune = service.reconcile()
    dry_run = service.reconcile(prune=True, dry_run=True)
    service.reconcile(prune=True)

    assert without_prune.actions == []
    assert [action.action for action in dry_run.actions] == [PRUNE, PRUNE]
    assert [call for call in workspace.pipelines.calls if call[0] == "delete"] == [
        ("delete", "owned-ingestion"),
        ("delete", "owned-gateway"),
    ]
    assert repo.deleted == ["old"]
    assert repo.ensure_calls == 1


def test_prune_tolerates_already_deleted_pipelines():
    stale = StoredDeploymentState(
        "old",
        DeploymentState(
            "owned-gateway", "owned-ingestion", "1", "fingerprint", "deployed"
        ),
    )
    workspace = Workspace()

    class NotFound(Exception):
        status_code = 404

    def delete(pipeline_id):
        raise NotFound()

    workspace.pipelines.delete = delete
    repo = Repository([], [stale], all_ids=[])

    result = IngestionSdkService(workspace, repo).reconcile(prune=True)

    assert [action.action for action in result.actions] == [PRUNE, PRUNE]
    assert repo.deleted == ["old"]


def test_prune_failure_still_releases_state_for_completed_ids():
    stale_a = StoredDeploymentState(
        "a", DeploymentState(None, "ingest-a", "1", "fingerprint", "deployed")
    )
    stale_b = StoredDeploymentState(
        "b", DeploymentState(None, "ingest-b", "1", "fingerprint", "deployed")
    )
    workspace = Workspace()

    def delete(pipeline_id):
        if pipeline_id == "ingest-b":
            raise RuntimeError("delete exploded")

    workspace.pipelines.delete = delete
    repo = Repository([], [stale_a, stale_b], all_ids=[])

    with pytest.raises(RuntimeError, match="delete exploded"):
        IngestionSdkService(workspace, repo).reconcile(prune=True)

    # "a" completed, so its ownership state must be gone; "b" keeps its
    # state row and converges on the next prune retry (404s are tolerated).
    assert repo.deleted == ["a"]


def test_explicit_ingestion_layer_intercepts_interactive_deploy(capsys):
    workspace = MagicMock()
    sdp_meta = SDPMeta(workspace)
    result = IngestionReconcileResult([], True)

    with patch(
        "databricks.labs.sdp_meta.lfc.state.IngestionStateRepository"
    ) as repository_type, patch(
        "databricks.labs.sdp_meta.lfc.sdk_service.IngestionSdkService"
    ) as service_type:
        service_type.return_value.reconcile.return_value = result
        deploy(
            sdp_meta,
            {
                "layer": "ingestion",
                "ingestion-dataflowspec-table": "main.meta.specs",
                "warehouse-id": "warehouse-1",
                "data-flow-ids": "300,301",
                "dry-run": "true",
            },
        )

    repository_type.assert_called_once_with(
        workspace, "warehouse-1", "main.meta.specs", None
    )
    service_type.return_value.reconcile.assert_called_once_with(
        data_flow_group=None,
        data_flow_ids=["300", "301"],
        dry_run=True,
        prune=False,
    )
    assert '"dry_run": true' in capsys.readouterr().out


def test_invalid_ingestion_layer_is_rejected_before_interactive_deploy():
    sdp_meta = SDPMeta(MagicMock())

    with pytest.raises(ValueError, match="only supports 'ingestion'"):
        deploy(sdp_meta, {"layer": "ingsetion"})
