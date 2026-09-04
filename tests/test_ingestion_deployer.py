from types import SimpleNamespace

from databricks.labs.sdp_meta.lfc.deployer import (
    CREATE,
    DRIFT,
    NOOP,
    SKIP,
    UPDATE,
    DeploymentState,
    IngestionDeployer,
    desired_fingerprint,
    schedule_plan,
)
from databricks.labs.sdp_meta.lfc.renderer import IngestionRenderer


class FakePipelines:
    def __init__(self, existing=None):
        self.existing = list(existing or [])
        self.calls = []
        self.next_id = 1

    def list_pipelines(self):
        return list(self.existing)

    def create(self, **definition):
        pipeline_id = "created-%s" % self.next_id
        self.next_id += 1
        self.calls.append(("create", definition))
        self.existing.append(SimpleNamespace(pipeline_id=pipeline_id, name=definition["name"]))
        return SimpleNamespace(pipeline_id=pipeline_id)

    def update(self, pipeline_id, **definition):
        self.calls.append(("update", pipeline_id, definition))

    def delete(self, pipeline_id):
        self.calls.append(("delete", pipeline_id))


class FakeWorkspaceClient:
    def __init__(self, existing=None):
        self.pipelines = FakePipelines(existing)


def desired(deploy=True):
    return IngestionRenderer().render(
        {
            "name": "orders",
            "connection_name": "postgres",
            "gateway_storage": {"catalog": "meta", "schema": "connect"},
            "objects": [{"table": {"source_schema": "public", "source_table": "orders"}}],
            "deploy": deploy,
            "spec_version": "7",
        }
    )


def test_create_plan_and_deploy_orders_gateway_before_ingestion():
    workspace = FakeWorkspaceClient()
    deployer = IngestionDeployer(workspace)

    result = deployer.deploy(desired())

    assert [action.action for action in result.plan.actions] == [CREATE, CREATE]
    assert [call[0] for call in workspace.pipelines.calls] == ["create", "create"]
    gateway_call = workspace.pipelines.calls[0][1]
    ingestion_call = workspace.pipelines.calls[1][1]
    assert "gateway_definition" in gateway_call
    assert ingestion_call["ingestion_definition"].as_dict()[
        "ingestion_gateway_id"
    ] == "created-1"
    assert "libraries" not in ingestion_call
    assert "notebook" not in ingestion_call
    assert result.state == DeploymentState(
        gateway_pipeline_id="created-1",
        ingestion_pipeline_id="created-2",
        spec_version="7",
        fingerprint=result.plan.fingerprint,
        status="deployed",
    )


def test_matching_managed_fingerprint_is_noop():
    rendered = desired()
    fingerprint = desired_fingerprint(rendered.gateway, rendered.ingestion)
    state = DeploymentState("gateway-id", "ingestion-id", "7", fingerprint, "deployed")
    workspace = FakeWorkspaceClient(
        [
            SimpleNamespace(pipeline_id="gateway-id", name="orders-gateway"),
            SimpleNamespace(pipeline_id="ingestion-id", name="orders-ingestion"),
        ]
    )

    plan = IngestionDeployer(workspace).plan(rendered, state)

    assert [action.action for action in plan.actions] == [NOOP, NOOP]


def test_changed_managed_fingerprint_plans_update():
    rendered = desired()
    state = DeploymentState("gateway-id", "ingestion-id", "6", "old", "deployed")
    workspace = FakeWorkspaceClient(
        [
            SimpleNamespace(pipeline_id="gateway-id", name="orders-gateway"),
            SimpleNamespace(pipeline_id="ingestion-id", name="orders-ingestion"),
        ]
    )

    plan = IngestionDeployer(workspace).plan(rendered, state)

    assert [action.action for action in plan.actions] == [UPDATE, UPDATE]


def test_unmanaged_same_name_is_drift_and_never_mutated():
    workspace = FakeWorkspaceClient(
        [
            SimpleNamespace(pipeline_id="someone-elses-gateway", name="orders-gateway"),
            SimpleNamespace(pipeline_id="someone-elses-ingestion", name="orders-ingestion"),
        ]
    )
    deployer = IngestionDeployer(workspace)

    result = deployer.deploy(desired())

    assert [action.action for action in result.plan.actions] == [DRIFT, DRIFT]
    assert result.state.status == "drift"
    assert workspace.pipelines.calls == []


def test_deploy_false_and_dry_run_have_no_side_effects():
    workspace = FakeWorkspaceClient()
    deployer = IngestionDeployer(workspace)

    skipped = deployer.deploy(desired(deploy=False))
    planned = deployer.deploy(desired(), dry_run=True)

    assert [action.action for action in skipped.plan.actions] == [SKIP, SKIP]
    assert skipped.state.status == "skipped"
    assert planned.state.status == "planned"
    assert workspace.pipelines.calls == []


def test_plan_serialization_does_not_leak_configuration():
    rendered = desired()
    rendered.gateway["configuration"]["password"] = "{{secrets/scope/key}}"

    plan = IngestionDeployer(FakeWorkspaceClient()).plan(rendered)

    assert "configuration" not in repr(plan.actions[0])
    assert "secrets/scope/key" not in str(plan.as_dict())


def test_schedule_plan_uses_jobs_pipeline_task_metadata():
    job = schedule_plan(
        "orders schedule",
        "ingestion-id",
        {"quartz_cron_expression": "0 0 * * * ?", "timezone_id": "UTC"},
    )

    assert job["tasks"] == [
        {
            "task_key": "run_ingestion_pipeline",
            "pipeline_task": {
                "pipeline_id": "ingestion-id",
                "full_refresh": False,
            },
        }
    ]
    assert job["schedule"]["timezone_id"] == "UTC"
