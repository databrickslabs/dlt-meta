import json
from types import SimpleNamespace

import pytest

from databricks.labs.sdp_meta.lfc.deployer import DeploymentState
from databricks.labs.sdp_meta.lfc.state import (
    SPEC_COLUMNS,
    IngestionStateRepository,
    default_state_table,
    sql_literal,
)


def response(state="SUCCEEDED", columns=(), rows=(), error=None):
    return SimpleNamespace(
        statement_id="statement-1",
        status=SimpleNamespace(
            state=SimpleNamespace(value=state),
            error=SimpleNamespace(message=error) if error else None,
        ),
        manifest=SimpleNamespace(
            schema=SimpleNamespace(
                columns=[SimpleNamespace(name=name) for name in columns]
            ),
            total_chunk_count=1,
        ),
        result=SimpleNamespace(data_array=list(rows)),
    )


class Statements:
    def __init__(self, results, chunks=None):
        self.results = list(results)
        self.chunks = list(chunks or [])
        self.calls = []

    def execute_statement(self, **kwargs):
        self.calls.append(kwargs)
        return self.results.pop(0)

    def get_statement(self, statement_id):
        self.calls.append({"get": statement_id})
        return self.results.pop(0)

    def get_statement_result_chunk_n(self, statement_id, chunk_index):
        self.calls.append(
            {"chunk": (statement_id, chunk_index)}
        )
        return self.chunks.pop(0)


def repository(results, chunks=None):
    workspace = SimpleNamespace(
        statement_execution=Statements(results, chunks)
    )
    return IngestionStateRepository(
        workspace,
        "warehouse-1",
        "main.meta.ingestion_dataflowspec",
        poll_interval_seconds=0,
    )


def test_read_specs_parses_manifest_rows_case_insensitively():
    payload = {column: None for column in SPEC_COLUMNS}
    payload.update(
        {
            "dataFlowId": "300",
            "dataFlowGroup": "orders",
            "sourceType": "POSTGRESQL",
            "connectionName": "orders_connection",
            "gatewayDetails": json.dumps({"name": "orders-gateway"}),
            "targetDetails": json.dumps(
                {"name": "orders-ingestion", "catalog": "main", "schema": "raw"}
            ),
            "objects": "[]",
            "sourceConfigurations": "[]",
            "deploy": "true",
            "manageConnection": "false",
            "version": "7",
        }
    )
    columns = [column.lower() for column in SPEC_COLUMNS]
    rows = [[payload[column] for column in SPEC_COLUMNS]]
    repo = repository([response(columns=columns, rows=rows)])

    specs = repo.read_specs("orders", ["300"])

    assert specs[0]["dataFlowId"] == "300"
    assert specs[0]["gatewayDetails"]["name"] == "orders-gateway"
    assert specs[0]["deploy"] is True
    statement = repo.workspace_client.statement_execution.calls[0]["statement"]
    assert "dataFlowGroup = 'orders'" in statement
    assert "dataFlowId IN ('300')" in statement
    assert (
        "TRY_CAST(REGEXP_REPLACE(version, '^[vV]', '') "
        "AS DECIMAL(38, 18)) DESC"
    ) in statement


def test_statement_must_reach_succeeded():
    repo = repository(
        [
            response(state="PENDING"),
            response(state="FAILED", error="warehouse failed"),
        ]
    )

    with pytest.raises(RuntimeError, match="state=FAILED.*warehouse failed"):
        repo.read_states()


def test_statement_reads_followup_result_chunks():
    first = response(columns=("value",), rows=(("one",),))
    first.manifest.total_chunk_count = 2
    chunk = SimpleNamespace(data_array=[["two"]])
    repo = repository([first], [chunk])

    rows = repo._execute("SELECT value")

    assert rows == [{"value": "one"}, {"value": "two"}]


def test_sql_identifier_and_literal_safety():
    assert default_state_table("main.meta.specs") == (
        "main.meta.ingestion_deployment_state"
    )
    assert sql_literal("id' OR 1=1 --") == "'id'' OR 1=1 --'"
    with pytest.raises(ValueError, match="three-part"):
        repository([]).__class__(
            SimpleNamespace(),
            "warehouse",
            "main.meta",
        )
    with pytest.raises(ValueError, match="valid Databricks SQL"):
        IngestionStateRepository(
            SimpleNamespace(),
            "warehouse",
            "main.meta.specs;DROP_TABLE",
        )


def test_state_merge_contains_only_ownership_metadata():
    repo = repository([response()])
    repo.save_state(
        "300",
        DeploymentState("gateway-1", "ingestion-1", "7", "abc123", "deployed"),
    )

    statement = repo.workspace_client.statement_execution.calls[0]["statement"]
    assert "MERGE INTO `main`.`meta`.`ingestion_deployment_state`" in statement
    assert "gateway-1" in statement
    assert "configuration" not in statement.lower()
    assert "credential" not in statement.lower()


def test_flow_lock_is_verified_and_released_by_owner_token():
    repo = repository([
        response(),
        response(columns=["owner_token"], rows=[["owner-1"]]),
        response(),
    ])

    assert repo.acquire_lock("300", "owner-1") is True
    repo.release_lock("300", "owner-1")

    calls = repo.workspace_client.statement_execution.calls
    assert "MERGE INTO `main`.`meta`.`ingestion_deployment_state_locks`" in (
        calls[0]["statement"]
    )
    assert "owner_token = 'owner-1'" in calls[2]["statement"]
