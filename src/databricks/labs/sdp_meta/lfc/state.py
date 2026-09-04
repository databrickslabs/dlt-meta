"""SQL-backed ingestion specification and deployment-state repository."""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from databricks.labs.sdp_meta.identifiers import validate_uc_full_name
from databricks.labs.sdp_meta.lfc.deployer import DeploymentState


SPEC_COLUMNS = (
    "dataFlowId",
    "dataFlowGroup",
    "sourceType",
    "connectionName",
    "connectionSpec",
    "manageConnection",
    "gatewayDetails",
    "sourceConfigurations",
    "objects",
    "targetDetails",
    "schedule",
    "deploy",
    "gatewayPipelineConfiguration",
    "ingestionPipelineConfiguration",
    "gatewayCompute",
    "version",
    "createDate",
    "createdBy",
    "updateDate",
    "updatedBy",
)
STATE_COLUMNS = (
    "data_flow_id",
    "gateway_pipeline_id",
    "ingestion_pipeline_id",
    "spec_version",
    "fingerprint",
    "status",
    "updated_at",
)
TERMINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"})


def _quoted_full_name(name: str, kind: str) -> str:
    validate_uc_full_name(name, kind=kind, max_parts=3)
    parts = name.split(".")
    if len(parts) != 3:
        raise ValueError(f"{kind} must be a three-part catalog.schema.table name")
    return ".".join(f"`{part}`" for part in parts)


def default_state_table(spec_table: str) -> str:
    """Return the canonical state table next to a three-part spec table."""
    validate_uc_full_name(spec_table, kind="ingestion dataflowspec table", max_parts=3)
    parts = spec_table.split(".")
    if len(parts) != 3:
        raise ValueError(
            "ingestion dataflowspec table must be a three-part "
            "catalog.schema.table name"
        )
    return ".".join((parts[0], parts[1], "ingestion_deployment_state"))


def sql_literal(value: Any) -> str:
    """Render one non-secret SQL literal without allowing statement escape."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    text = str(value)
    if "\x00" in text:
        raise ValueError("SQL literal must not contain NUL characters")
    return "'" + text.replace("'", "''") + "'"


def _state_value(result: Any) -> str:
    status = getattr(result, "status", None)
    state = getattr(status, "state", None)
    raw = getattr(state, "value", state)
    return str(raw or "").rsplit(".", 1)[-1].upper()


def _column_names(result: Any) -> List[str]:
    manifest = getattr(result, "manifest", None)
    schema = getattr(manifest, "schema", None)
    return [
        str(getattr(column, "name", ""))
        for column in (getattr(schema, "columns", None) or [])
    ]


def _data_array(result: Any) -> List[List[Any]]:
    payload = getattr(result, "result", None) or result
    return [list(row) for row in (getattr(payload, "data_array", None) or [])]


@dataclass(frozen=True)
class StoredDeploymentState:
    data_flow_id: str
    state: DeploymentState
    updated_at: Optional[str] = None


class IngestionStateRepository:
    """Read desired specs and ownership state through Statement Execution."""

    def __init__(
        self,
        workspace_client: Any,
        warehouse_id: str,
        spec_table: str,
        state_table: Optional[str] = None,
        *,
        poll_interval_seconds: float = 1.0,
        max_polls: int = 120,
    ):
        if not isinstance(warehouse_id, str) or not warehouse_id.strip():
            raise ValueError("--warehouse-id is required for ingestion deployment")
        self.workspace_client = workspace_client
        self.warehouse_id = warehouse_id
        self.spec_table = spec_table
        self.state_table = state_table or default_state_table(spec_table)
        self._spec_sql = _quoted_full_name(
            self.spec_table, "ingestion dataflowspec table"
        )
        self._state_sql = _quoted_full_name(
            self.state_table, "ingestion state table"
        )
        lock_table = self.state_table + "_locks"
        self._lock_sql = _quoted_full_name(
            lock_table, "ingestion deployment lock table"
        )
        self.poll_interval_seconds = poll_interval_seconds
        self.max_polls = max_polls

    def _execute(self, statement: str) -> List[Dict[str, Any]]:
        api = self.workspace_client.statement_execution
        result = api.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=statement,
            wait_timeout="30s",
        )
        for _ in range(self.max_polls):
            state = _state_value(result)
            if state in TERMINAL_STATES:
                break
            statement_id = getattr(result, "statement_id", None)
            if not statement_id:
                raise RuntimeError(
                    "Statement Execution returned a non-terminal response "
                    "without a statement_id"
                )
            if self.poll_interval_seconds:
                time.sleep(self.poll_interval_seconds)
            result = api.get_statement(statement_id)
        state = _state_value(result)
        if state != "SUCCEEDED":
            error = getattr(getattr(result, "status", None), "error", None)
            message = getattr(error, "message", None) or str(error or "")
            raise RuntimeError(
                "Databricks SQL statement did not succeed "
                f"(state={state or 'UNKNOWN'}): {message or 'no error details'}"
            )

        columns = _column_names(result)
        rows = _data_array(result)
        manifest = getattr(result, "manifest", None)
        manifest_chunks = getattr(manifest, "chunks", None) or []
        chunk_count = int(
            getattr(manifest, "total_chunk_count", None)
            or len(manifest_chunks)
            or 1
        )
        statement_id = getattr(result, "statement_id", None)
        for chunk_index in range(1, chunk_count):
            if not statement_id:
                raise RuntimeError(
                    "Statement result has multiple chunks but no statement_id"
                )
            chunk = api.get_statement_result_chunk_n(
                statement_id, chunk_index
            )
            rows.extend(_data_array(chunk))
        if rows and not columns:
            raise RuntimeError("Statement result returned rows without a schema")
        width = len(columns)
        for row in rows:
            if len(row) != width:
                raise RuntimeError(
                    "Statement result row width does not match manifest schema"
                )
        return [dict(zip(columns, row)) for row in rows]

    @staticmethod
    def _case_insensitive(row: Mapping[str, Any]) -> Dict[str, Any]:
        return {str(key).lower(): value for key, value in row.items()}

    @classmethod
    def _normalize_spec_row(cls, row: Mapping[str, Any]) -> Dict[str, Any]:
        lower = cls._case_insensitive(row)
        normalized = {
            column: lower.get(column.lower())
            for column in SPEC_COLUMNS
        }
        for field_name in ("gatewayDetails", "targetDetails"):
            value = normalized[field_name]
            if isinstance(value, str):
                try:
                    normalized[field_name] = json.loads(value)
                except json.JSONDecodeError as err:
                    raise ValueError(
                        f"{field_name} contains invalid JSON"
                    ) from err
            elif value is None:
                normalized[field_name] = {}
        for field_name in ("deploy", "manageConnection"):
            value = normalized[field_name]
            if isinstance(value, str):
                normalized[field_name] = value.strip().lower() == "true"
        return normalized

    @staticmethod
    def _filters(
        data_flow_group: Optional[str],
        data_flow_ids: Optional[Sequence[str]],
    ) -> List[str]:
        filters = []
        if data_flow_group:
            filters.append(f"dataFlowGroup = {sql_literal(data_flow_group)}")
        if data_flow_ids:
            values = ", ".join(sql_literal(item) for item in data_flow_ids)
            filters.append(f"dataFlowId IN ({values})")
        return filters

    def read_specs(
        self,
        data_flow_group: Optional[str] = None,
        data_flow_ids: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Read the latest normalized spec for each group/id pair."""
        columns = ", ".join(SPEC_COLUMNS)
        filters = self._filters(data_flow_group, data_flow_ids)
        predicate = " AND ".join(["_sdp_meta_rank = 1", *filters])
        statement = (
            f"SELECT {columns} FROM ("
            f"SELECT {columns}, ROW_NUMBER() OVER ("
            "PARTITION BY dataFlowGroup, dataFlowId "
            "ORDER BY TRY_CAST("
            "REGEXP_REPLACE(version, '^[vV]', '') AS DECIMAL(38, 18)"
            ") DESC NULLS LAST, "
            "version DESC, updateDate DESC"
            f") AS _sdp_meta_rank FROM {self._spec_sql}"
            f") WHERE {predicate} "
            "ORDER BY dataFlowGroup, dataFlowId"
        )
        return [
            self._normalize_spec_row(row)
            for row in self._execute(statement)
        ]

    def read_all_spec_ids(self) -> List[str]:
        rows = self._execute(
            "SELECT DISTINCT dataFlowId "
            f"FROM {self._spec_sql}"
        )
        return [
            str(self._case_insensitive(row).get("dataflowid"))
            for row in rows
            if self._case_insensitive(row).get("dataflowid") is not None
        ]

    def ensure_state_table(self) -> None:
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._state_sql} ("
            "data_flow_id STRING NOT NULL, "
            "gateway_pipeline_id STRING, "
            "ingestion_pipeline_id STRING, "
            "spec_version STRING NOT NULL, "
            "fingerprint STRING NOT NULL, "
            "status STRING NOT NULL, "
            "updated_at TIMESTAMP NOT NULL"
            ") USING DELTA"
        )
        self._execute(
            f"CREATE TABLE IF NOT EXISTS {self._lock_sql} ("
            "data_flow_id STRING NOT NULL, "
            "owner_token STRING NOT NULL, "
            "expires_at TIMESTAMP NOT NULL"
            ") USING DELTA"
        )

    def acquire_lock(
        self,
        data_flow_id: str,
        owner_token: str,
        *,
        lease_minutes: int = 15,
    ) -> bool:
        """Acquire a renewable per-flow lease before Workspace mutations."""
        if lease_minutes < 1:
            raise ValueError("lease_minutes must be positive")
        self._execute(
            f"MERGE INTO {self._lock_sql} AS target "
            "USING (SELECT "
            f"{sql_literal(data_flow_id)} AS data_flow_id, "
            f"{sql_literal(owner_token)} AS owner_token"
            ") AS source "
            "ON target.data_flow_id = source.data_flow_id "
            "WHEN MATCHED AND target.expires_at <= current_timestamp() "
            "THEN UPDATE SET "
            "target.owner_token = source.owner_token, "
            "target.expires_at = current_timestamp() + "
            f"INTERVAL {lease_minutes} MINUTES "
            "WHEN NOT MATCHED THEN INSERT "
            "(data_flow_id, owner_token, expires_at) VALUES "
            "(source.data_flow_id, source.owner_token, "
            f"current_timestamp() + INTERVAL {lease_minutes} MINUTES)"
        )
        rows = self._execute(
            "SELECT owner_token FROM "
            f"{self._lock_sql} WHERE data_flow_id = "
            f"{sql_literal(data_flow_id)} "
            "AND expires_at > current_timestamp()"
        )
        return (
            len(rows) == 1
            and str(
                self._case_insensitive(rows[0]).get("owner_token")
            ) == owner_token
        )

    def release_lock(self, data_flow_id: str, owner_token: str) -> None:
        self._execute(
            f"DELETE FROM {self._lock_sql} WHERE data_flow_id = "
            f"{sql_literal(data_flow_id)} AND owner_token = "
            f"{sql_literal(owner_token)}"
        )

    def read_states(self, *, allow_missing: bool = False) -> List[StoredDeploymentState]:
        try:
            rows = self._execute(
                f"SELECT {', '.join(STATE_COLUMNS)} FROM {self._state_sql}"
            )
        except RuntimeError as err:
            if allow_missing and (
                "TABLE_OR_VIEW_NOT_FOUND" in str(err)
                or "TABLE_NOT_FOUND" in str(err)
            ):
                return []
            raise
        result = []
        seen_ids = set()
        for row in rows:
            lower = self._case_insensitive(row)
            data_flow_id = lower.get("data_flow_id")
            if data_flow_id is None:
                raise RuntimeError("ingestion state row is missing data_flow_id")
            data_flow_id = str(data_flow_id)
            if data_flow_id in seen_ids:
                raise RuntimeError(
                    "ingestion state table contains duplicate data_flow_id "
                    f"{data_flow_id!r}"
                )
            seen_ids.add(data_flow_id)
            result.append(
                StoredDeploymentState(
                    data_flow_id=data_flow_id,
                    state=DeploymentState(
                        gateway_pipeline_id=lower.get("gateway_pipeline_id"),
                        ingestion_pipeline_id=lower.get("ingestion_pipeline_id"),
                        spec_version=str(lower.get("spec_version") or "1"),
                        fingerprint=str(lower.get("fingerprint") or ""),
                        status=str(lower.get("status") or "unknown"),
                    ),
                    updated_at=(
                        str(lower["updated_at"])
                        if lower.get("updated_at") is not None
                        else None
                    ),
                )
            )
        return result

    def save_state(self, data_flow_id: str, state: DeploymentState) -> None:
        """Upsert ownership metadata only; desired configuration is never stored."""
        values = {
            "data_flow_id": data_flow_id,
            "gateway_pipeline_id": state.gateway_pipeline_id,
            "ingestion_pipeline_id": state.ingestion_pipeline_id,
            "spec_version": state.spec_version,
            "fingerprint": state.fingerprint,
            "status": state.status,
        }
        source = ", ".join(
            f"{sql_literal(value)} AS {name}"
            for name, value in values.items()
        )
        updates = ", ".join(
            f"target.{name} = source.{name}"
            for name in values
            if name != "data_flow_id"
        )
        insert_names = ", ".join((*values.keys(), "updated_at"))
        insert_values = ", ".join(
            [*(f"source.{name}" for name in values), "current_timestamp()"]
        )
        self._execute(
            f"MERGE INTO {self._state_sql} AS target "
            f"USING (SELECT {source}) AS source "
            "ON target.data_flow_id = source.data_flow_id "
            f"WHEN MATCHED THEN UPDATE SET {updates}, "
            "target.updated_at = current_timestamp() "
            f"WHEN NOT MATCHED THEN INSERT ({insert_names}) "
            f"VALUES ({insert_values})"
        )

    def delete_states(self, data_flow_ids: Iterable[str]) -> None:
        values = list(data_flow_ids)
        if not values:
            return
        literals = ", ".join(sql_literal(value) for value in values)
        self._execute(
            f"DELETE FROM {self._state_sql} "
            f"WHERE data_flow_id IN ({literals})"
        )
