"""Secure credentials and Unity Catalog connection helpers."""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping, Optional, Protocol, Sequence

from databricks.labs.sdp_meta.identifiers import validate_uc_identifier


_SENSITIVE_KEYS = frozenset({
    "password",
    "passwd",
    "username",
    "user",
    "secret",
    "secret_string",
    "secret_binary",
    "token",
    "access_token",
    "private_key",
})
_SAFE_SECRET_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._@-]{0,127}$")
_PLACEHOLDER_PATTERNS = (
    re.compile(r"\$\{[^}]+\}"),
    re.compile(r"\{\{[^}]+\}\}"),
    re.compile(r"<(?:change|replace|your|insert)[^>]*>", re.IGNORECASE),
    re.compile(r"\b(?:CHANGE_ME|REPLACE_ME|TODO)\b", re.IGNORECASE),
)
_TERMINAL_STATEMENT_STATES = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


def redact(value: Any, secret_values: Sequence[str] = ()) -> Any:
    """Return a redacted copy suitable for diagnostics."""
    secrets = tuple(
        item for item in secret_values if isinstance(item, str) and item
    )
    if isinstance(value, Mapping):
        return {
            key: (
                "<redacted>"
                if str(key).lower() in _SENSITIVE_KEYS
                else redact(item, secrets)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item, secrets) for item in value)
    if isinstance(value, str):
        result = value
        for secret in secrets:
            result = result.replace(secret, "<redacted>")
        return result
    return value


def redact_text(value: Any, secret_values: Sequence[str] = ()) -> str:
    """Convert a value to text and redact explicitly supplied secret values."""
    return str(redact(str(value), secret_values))


@dataclass(frozen=True, repr=False)
class DatabaseCredential:
    """A username/password pair whose representation never exposes values."""

    username: str
    password: str

    def __post_init__(self) -> None:
        if not isinstance(self.username, str) or not self.username:
            raise ValueError("credential username must be a non-empty string")
        if not isinstance(self.password, str) or not self.password:
            raise ValueError("credential password must be a non-empty string")

    def __repr__(self) -> str:
        return "DatabaseCredential(username=<redacted>, password=<redacted>)"

    __str__ = __repr__


class SecretProvider(Protocol):
    """Source of external database credentials."""

    def get_credential(self, secret_id: str) -> DatabaseCredential:
        """Fetch and parse one credential without persisting it locally."""


def parse_json_credential(payload: Any) -> DatabaseCredential:
    """Parse ``{"username": ..., "password": ...}`` from text or bytes."""
    if isinstance(payload, bytes):
        try:
            payload = payload.decode("utf-8")
        except UnicodeDecodeError as err:
            raise ValueError("credential secret must be UTF-8 JSON") from err
    if not isinstance(payload, str):
        raise ValueError("credential secret must be a JSON string or bytes")
    try:
        parsed = json.loads(payload)
    except (TypeError, ValueError) as err:
        raise ValueError("credential secret is not valid JSON") from err
    if not isinstance(parsed, dict):
        raise ValueError("credential secret JSON must be an object")
    username = parsed.get("username")
    password = parsed.get("password")
    if not isinstance(username, str) or not username:
        raise ValueError("credential secret requires a non-empty 'username'")
    if not isinstance(password, str) or not password:
        raise ValueError("credential secret requires a non-empty 'password'")
    return DatabaseCredential(username=username, password=password)


class ManualSecretProvider:
    """In-memory provider for manual workflows; has no cloud dependency."""

    def __init__(self, credentials: Mapping[str, DatabaseCredential]):
        self._credentials = dict(credentials)

    def get_credential(self, secret_id: str) -> DatabaseCredential:
        try:
            return self._credentials[secret_id]
        except KeyError:
            raise KeyError(
                "credential was not supplied for the requested name"
            ) from None

    def __repr__(self) -> str:
        return f"ManualSecretProvider(secret_ids={sorted(self._credentials)!r})"


@dataclass(frozen=True)
class SecretReferences:
    """Names of pre-existing Databricks secrets (contains no secret values)."""

    scope: str
    username_key: str = "username"
    password_key: str = "password"

    def __post_init__(self) -> None:
        _validate_secret_name(self.scope, "secret scope")
        _validate_secret_name(self.username_key, "username secret key")
        _validate_secret_name(self.password_key, "password secret key")
        if self.username_key == self.password_key:
            raise ValueError(
                "username and password secret keys must be different"
            )


@dataclass(frozen=True)
class SecretSyncReport:
    """Names-only result of a secret synchronization."""

    scope: str
    keys: tuple
    dry_run: bool


def sync_credential_to_scope(
    workspace_client: Any,
    references: SecretReferences,
    credential: DatabaseCredential,
    *,
    dry_run: bool = False,
) -> SecretSyncReport:
    """Create a scope if needed and put both values through the Secrets API."""
    report = SecretSyncReport(
        scope=references.scope,
        keys=(references.username_key, references.password_key),
        dry_run=dry_run,
    )
    if dry_run:
        return report

    secrets_api = workspace_client.secrets
    existing_scopes = {
        _read_field(item, "name")
        for item in secrets_api.list_scopes()
        if _read_field(item, "name")
    }
    if references.scope not in existing_scopes:
        secrets_api.create_scope(scope=references.scope)
    secrets_api.put_secret(
        scope=references.scope,
        key=references.username_key,
        string_value=credential.username,
    )
    secrets_api.put_secret(
        scope=references.scope,
        key=references.password_key,
        string_value=credential.password,
    )
    return report


def sync_provider_credential_to_scope(
    workspace_client: Any,
    provider: SecretProvider,
    external_secret_id: str,
    references: SecretReferences,
    *,
    dry_run: bool = False,
) -> SecretSyncReport:
    """Fetch from a provider and sync, without fetching at all on dry-run."""
    if not isinstance(external_secret_id, str) or not external_secret_id:
        raise ValueError("external_secret_id must be a non-empty string")
    if dry_run:
        return SecretSyncReport(
            scope=references.scope,
            keys=(references.username_key, references.password_key),
            dry_run=True,
        )
    credential = provider.get_credential(external_secret_id)
    return sync_credential_to_scope(
        workspace_client,
        references,
        credential,
        dry_run=False,
    )


@dataclass(frozen=True)
class ConnectionSpec:
    """Safe inputs for a UC database connection."""

    name: str
    connection_type: str
    host: str
    port: int
    secrets: SecretReferences
    database: Optional[str] = None

    def __post_init__(self) -> None:
        validate_uc_identifier(self.name, kind="connection name")
        validate_uc_identifier(self.connection_type, kind="connection type")
        _validate_literal(self.host, "host")
        if isinstance(self.port, bool) or not isinstance(self.port, int):
            raise ValueError("port must be an integer")
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        if self.database is not None:
            _validate_literal(self.database, "database")


def sql_string_literal(value: str) -> str:
    """Render a validated Databricks SQL string literal."""
    _validate_literal(value, "SQL string literal")
    return "'" + value.replace("'", "''") + "'"


def render_create_connection_sql(spec: ConnectionSpec) -> str:
    """Render idempotent DDL containing references, never credential values."""
    options = [
        f"HOST {sql_string_literal(spec.host)}",
        f"PORT {sql_string_literal(str(spec.port))}",
        (
            "USER secret("
            f"{sql_string_literal(spec.secrets.scope)}, "
            f"{sql_string_literal(spec.secrets.username_key)})"
        ),
        (
            "PASSWORD secret("
            f"{sql_string_literal(spec.secrets.scope)}, "
            f"{sql_string_literal(spec.secrets.password_key)})"
        ),
    ]
    if spec.database is not None:
        options.append(f"DATABASE {sql_string_literal(spec.database)}")
    return (
        f"CREATE CONNECTION IF NOT EXISTS {spec.name}\n"
        f"TYPE {spec.connection_type}\n"
        "OPTIONS (\n  "
        + ",\n  ".join(options)
        + "\n)"
    )


class PreflightAction(str, Enum):
    CREATE = "create"
    REUSE_MANAGED = "reuse_managed"
    REUSE_UNMANAGED = "reuse_unmanaged"


@dataclass(frozen=True)
class ConnectionPreflight:
    name: str
    managed: bool
    action: PreflightAction


def preflight_connection(
    workspace_client: Any,
    connection_name: str,
    *,
    managed: bool,
    desired: Optional[ConnectionSpec] = None,
) -> ConnectionPreflight:
    """Resolve managed/unmanaged behavior against the UC Connections API."""
    validate_uc_identifier(connection_name, kind="connection name")
    existing = _get_connection(
        workspace_client.connections, connection_name
    )
    if existing is not None:
        if managed and desired is not None:
            _assert_connection_matches(existing, desired)
        action = (
            PreflightAction.REUSE_MANAGED
            if managed
            else PreflightAction.REUSE_UNMANAGED
        )
        return ConnectionPreflight(connection_name, managed, action)
    if not managed:
        raise ValueError(
            f"unmanaged connection {connection_name!r} does not exist; "
            "create it outside SDP-META or select managed mode"
        )
    return ConnectionPreflight(
        connection_name, managed, PreflightAction.CREATE
    )


def execute_create_connection(
    workspace_client: Any,
    warehouse_id: str,
    spec: ConnectionSpec,
    *,
    execute: bool = False,
) -> str:
    """Render DDL and execute it only after an explicit ``execute=True``."""
    sql = render_create_connection_sql(spec)
    if not execute:
        return sql
    _reject_placeholders(sql)
    _validate_literal(warehouse_id, "warehouse_id")
    _reject_placeholders(warehouse_id)
    response = workspace_client.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    state_text = _statement_state(response)
    for _ in range(120):
        if state_text in _TERMINAL_STATEMENT_STATES:
            break
        statement_id = _read_field(response, "statement_id")
        if not statement_id:
            raise RuntimeError(
                "CREATE CONNECTION returned a non-terminal response "
                "without a statement_id"
            )
        time.sleep(1)
        response = workspace_client.statement_execution.get_statement(
            statement_id
        )
        state_text = _statement_state(response)
    if state_text != "SUCCEEDED":
        status = _read_field(response, "status")
        error = _read_field(status, "error")
        message = _read_field(error, "message")
        detail = f": {message}" if message else ""
        raise RuntimeError(
            "CREATE CONNECTION statement finished in state "
            f"{state_text or 'UNKNOWN'}{detail}"
        )
    return sql


def _statement_state(response: Any) -> str:
    status = _read_field(response, "status")
    state = _read_field(status, "state")
    state_value = _read_field(state, "value") or state
    return str(state_value or "").rsplit(".", 1)[-1].upper()


def _validate_secret_name(value: Any, kind: str) -> str:
    if not isinstance(value, str) or not _SAFE_SECRET_NAME.fullmatch(value):
        raise ValueError(
            f"{kind} must match {_SAFE_SECRET_NAME.pattern} "
            "and contain no whitespace"
        )
    return value


def _validate_literal(value: Any, kind: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{kind} must be a non-empty string")
    if "\x00" in value or "\r" in value or "\n" in value:
        raise ValueError(
            f"{kind} must not contain NUL or newline characters"
        )
    return value


def _reject_placeholders(value: str) -> None:
    if any(pattern.search(value) for pattern in _PLACEHOLDER_PATTERNS):
        raise ValueError("connection SQL contains an unresolved placeholder")


def _connection_exists(connections_api: Any, connection_name: str) -> bool:
    return _get_connection(connections_api, connection_name) is not None


def _get_connection(connections_api: Any, connection_name: str) -> Any:
    try:
        return connections_api.get(name=connection_name)
    except Exception as err:
        status_code = getattr(err, "status_code", None)
        error_code = getattr(err, "error_code", None)
        if status_code == 404 or error_code in {
            "NOT_FOUND",
            "RESOURCE_DOES_NOT_EXIST",
        }:
            return None
        raise


def _assert_connection_matches(existing: Any, desired: ConnectionSpec) -> None:
    """Reject managed reuse when visible UC settings differ from desired."""
    options = _read_field(existing, "options") or {}
    normalized = {
        str(key).strip().lower(): str(value).strip()
        for key, value in options.items()
        if value is not None
    }
    expected = {
        "host": desired.host,
        "port": str(desired.port),
    }
    if desired.database is not None:
        expected["database"] = desired.database
    drift = [
        f"{key}: existing={normalized[key]!r}, desired={value!r}"
        for key, value in expected.items()
        if key in normalized and normalized[key] != value
    ]
    existing_type = _read_field(existing, "connection_type")
    existing_type = _read_field(existing_type, "value") or existing_type
    if (
        existing_type is not None
        and str(existing_type).lower() != desired.connection_type.lower()
    ):
        drift.append(
            "connection_type: existing=%r, desired=%r"
            % (existing_type, desired.connection_type)
        )
    if drift:
        raise ValueError(
            f"managed connection {desired.name!r} differs from requested "
            "configuration; update or recreate it explicitly: "
            + "; ".join(drift)
        )


def _read_field(value: Any, field: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(field)
    return getattr(value, field, None)
