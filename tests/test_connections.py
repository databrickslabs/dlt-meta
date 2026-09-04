"""Focused tests for secure credential and UC connection helpers."""
from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from databricks.labs.sdp_meta.lfc.connections import (
    ConnectionSpec,
    DatabaseCredential,
    ManualSecretProvider,
    PreflightAction,
    SecretReferences,
    execute_create_connection,
    parse_json_credential,
    preflight_connection,
    redact,
    redact_text,
    render_create_connection_sql,
    sql_string_literal,
    sync_credential_to_scope,
    sync_provider_credential_to_scope,
)


USERNAME = "alice_no_leak_91823"
PASSWORD = "password_no_leak_74651"


class _FakeSecretsApi:
    def __init__(self, scopes=()):
        self.scopes = list(scopes)
        self.created = []
        self.puts = []

    def list_scopes(self):
        return [SimpleNamespace(name=name) for name in self.scopes]

    def create_scope(self, *, scope):
        self.created.append(scope)
        self.scopes.append(scope)

    def put_secret(self, **kwargs):
        self.puts.append(kwargs)


class _NotFound(Exception):
    status_code = 404
    error_code = "RESOURCE_DOES_NOT_EXIST"


class _FakeConnectionsApi:
    def __init__(self, names=(), error=None):
        self.names = set(names)
        self.error = error
        self.gets = []

    def get(self, *, name):
        self.gets.append(name)
        if self.error is not None:
            raise self.error
        if name not in self.names:
            raise _NotFound()
        return SimpleNamespace(name=name)


class _FakeStatementExecution:
    def __init__(self, state="SUCCEEDED", message=None, followup_states=()):
        self.calls = []
        self.states = iter((state, *followup_states))
        self.message = message

    def _response(self):
        state = next(self.states)
        error = (
            SimpleNamespace(message=self.message)
            if self.message is not None
            else None
        )
        return SimpleNamespace(
            statement_id="statement-1",
            status=SimpleNamespace(
                state=SimpleNamespace(value=state),
                error=error,
            ),
        )

    def execute_statement(self, **kwargs):
        self.calls.append(kwargs)
        return self._response()

    def get_statement(self, statement_id):
        self.calls.append({"get": statement_id})
        return self._response()


class _FakeWorkspace:
    def __init__(
        self,
        *,
        scopes=(),
        connections=(),
        connection_error=None,
        statement_state="SUCCEEDED",
        statement_message=None,
        statement_followup_states=(),
    ):
        self.secrets = _FakeSecretsApi(scopes)
        self.connections = _FakeConnectionsApi(connections, connection_error)
        self.statement_execution = _FakeStatementExecution(
            statement_state,
            statement_message,
            statement_followup_states,
        )


def _spec(**overrides):
    values = {
        "name": "postgres_connection",
        "connection_type": "POSTGRESQL",
        "host": "db.example.com",
        "port": 5432,
        "database": "sales",
        "secrets": SecretReferences("sdp-meta", "pg-user", "pg-password"),
    }
    values.update(overrides)
    return ConnectionSpec(**values)


class CredentialTests(unittest.TestCase):
    def test_parse_json_string(self):
        credential = parse_json_credential(
            json.dumps({"username": USERNAME, "password": PASSWORD})
        )
        self.assertEqual(credential.username, USERNAME)
        self.assertEqual(credential.password, PASSWORD)

    def test_parse_json_bytes(self):
        credential = parse_json_credential(
            json.dumps({"username": USERNAME, "password": PASSWORD}).encode()
        )
        self.assertEqual(credential.username, USERNAME)

    def test_parse_rejects_invalid_shapes_without_leaking_payload(self):
        cases = [
            "not json",
            "[]",
            '{"username": "only-user"}',
            '{"username": "", "password": "x"}',
            '{"username": "x", "password": 42}',
        ]
        for payload in cases:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError) as ctx:
                    parse_json_credential(payload)
                self.assertNotIn(payload, str(ctx.exception))

    def test_credential_repr_and_str_are_redacted(self):
        credential = DatabaseCredential(USERNAME, PASSWORD)
        rendered = repr(credential) + str(credential)
        self.assertNotIn(USERNAME, rendered)
        self.assertNotIn(PASSWORD, rendered)
        self.assertIn("<redacted>", rendered)

    def test_manual_provider_has_no_cloud_dependency_or_value_in_repr(self):
        provider = ManualSecretProvider(
            {"primary": DatabaseCredential(USERNAME, PASSWORD)}
        )
        self.assertEqual(provider.get_credential("primary").username, USERNAME)
        rendered = repr(provider)
        self.assertIn("primary", rendered)
        self.assertNotIn(USERNAME, rendered)
        self.assertNotIn(PASSWORD, rendered)

    def test_manual_provider_unknown_name_has_names_only_error(self):
        provider = ManualSecretProvider({})
        with self.assertRaises(KeyError) as ctx:
            provider.get_credential(PASSWORD)
        self.assertNotIn(PASSWORD, str(ctx.exception))

    def test_redaction_helpers_copy_nested_values(self):
        original = {
            "username": USERNAME,
            "password": PASSWORD,
            "nested": [{"token": PASSWORD}, f"failed for {PASSWORD}"],
        }
        result = redact(original, [PASSWORD])
        self.assertEqual(result["username"], "<redacted>")
        self.assertEqual(result["password"], "<redacted>")
        self.assertEqual(result["nested"][0]["token"], "<redacted>")
        self.assertNotIn(PASSWORD, repr(result))
        self.assertEqual(original["password"], PASSWORD)
        self.assertNotIn(PASSWORD, redact_text(original, [PASSWORD]))


class SecretSyncTests(unittest.TestCase):
    def test_dry_run_reports_names_and_makes_no_api_calls(self):
        workspace = _FakeWorkspace()
        credential = DatabaseCredential(USERNAME, PASSWORD)
        refs = SecretReferences("scope-one", "user-key", "password-key")
        report = sync_credential_to_scope(
            workspace, refs, credential, dry_run=True
        )
        self.assertEqual(report.scope, "scope-one")
        self.assertEqual(report.keys, ("user-key", "password-key"))
        self.assertTrue(report.dry_run)
        self.assertEqual(workspace.secrets.created, [])
        self.assertEqual(workspace.secrets.puts, [])
        rendered = repr(report)
        self.assertNotIn(USERNAME, rendered)
        self.assertNotIn(PASSWORD, rendered)

    def test_sync_creates_scope_and_puts_secrets(self):
        workspace = _FakeWorkspace()
        credential = DatabaseCredential(USERNAME, PASSWORD)
        refs = SecretReferences("scope-one", "user-key", "password-key")
        report = sync_credential_to_scope(workspace, refs, credential)
        self.assertFalse(report.dry_run)
        self.assertEqual(workspace.secrets.created, ["scope-one"])
        self.assertEqual(
            workspace.secrets.puts,
            [
                {
                    "scope": "scope-one",
                    "key": "user-key",
                    "string_value": USERNAME,
                },
                {
                    "scope": "scope-one",
                    "key": "password-key",
                    "string_value": PASSWORD,
                },
            ],
        )

    def test_sync_reuses_existing_scope(self):
        workspace = _FakeWorkspace(scopes=("scope-one",))
        sync_credential_to_scope(
            workspace,
            SecretReferences("scope-one"),
            DatabaseCredential(USERNAME, PASSWORD),
        )
        self.assertEqual(workspace.secrets.created, [])

    def test_provider_dry_run_does_not_fetch_secret(self):
        class FailIfFetched:
            def get_credential(self, secret_id):
                raise AssertionError(f"unexpected fetch of {secret_id}")

        workspace = _FakeWorkspace()
        report = sync_provider_credential_to_scope(
            workspace,
            FailIfFetched(),
            "external-secret",
            SecretReferences("scope-one"),
            dry_run=True,
        )
        self.assertTrue(report.dry_run)
        self.assertEqual(workspace.secrets.puts, [])

    def test_provider_fetches_and_syncs_when_not_dry_run(self):
        provider = ManualSecretProvider(
            {"external-secret": DatabaseCredential(USERNAME, PASSWORD)}
        )
        workspace = _FakeWorkspace()
        report = sync_provider_credential_to_scope(
            workspace,
            provider,
            "external-secret",
            SecretReferences("scope-one"),
        )
        self.assertFalse(report.dry_run)
        self.assertEqual(len(workspace.secrets.puts), 2)

    def test_invalid_scope_and_duplicate_keys_are_rejected(self):
        for bad_scope in ("", "has space", "x'; DROP SCOPE y"):
            with self.subTest(scope=bad_scope):
                with self.assertRaises(ValueError):
                    SecretReferences(bad_scope)
        with self.assertRaisesRegex(ValueError, "different"):
            SecretReferences("scope", "same", "same")


class SqlRenderingTests(unittest.TestCase):
    def test_render_is_idempotent_and_uses_secret_references(self):
        sql = render_create_connection_sql(_spec())
        self.assertIn(
            "CREATE CONNECTION IF NOT EXISTS postgres_connection", sql
        )
        self.assertIn("TYPE POSTGRESQL", sql)
        self.assertIn("HOST 'db.example.com'", sql)
        self.assertIn("PORT '5432'", sql)
        self.assertIn("USER secret('sdp-meta', 'pg-user')", sql)
        self.assertIn(
            "PASSWORD secret('sdp-meta', 'pg-password')", sql
        )
        self.assertIn("DATABASE 'sales'", sql)
        self.assertNotIn(USERNAME, sql)
        self.assertNotIn(PASSWORD, sql)

    def test_sql_literal_escapes_single_quote(self):
        self.assertEqual(sql_string_literal("O'Reilly"), "'O''Reilly'")
        sql = render_create_connection_sql(_spec(database="sales'archive"))
        self.assertIn("DATABASE 'sales''archive'", sql)

    def test_identifiers_and_literals_are_validated(self):
        for kwargs in (
            {"name": "bad-name"},
            {"name": "x; DROP CONNECTION y"},
            {"connection_type": "POSTGRESQL; DROP"},
            {"host": "host\nDROP CONNECTION x"},
            {"port": 0},
            {"port": 70000},
            {"port": "5432"},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    _spec(**kwargs)

    def test_database_is_optional(self):
        sql = render_create_connection_sql(_spec(database=None))
        self.assertNotIn("DATABASE", sql)


class ExecutionAndPreflightTests(unittest.TestCase):
    def test_execution_is_opt_in(self):
        workspace = _FakeWorkspace()
        sql = execute_create_connection(
            workspace, "warehouse-1", _spec(), execute=False
        )
        self.assertIn("CREATE CONNECTION", sql)
        self.assertEqual(workspace.statement_execution.calls, [])

    def test_explicit_execution_uses_statement_api(self):
        workspace = _FakeWorkspace()
        sql = execute_create_connection(
            workspace, "warehouse-1", _spec(), execute=True
        )
        self.assertEqual(
            workspace.statement_execution.calls,
            [{
                "warehouse_id": "warehouse-1",
                "statement": sql,
                "wait_timeout": "30s",
            }],
        )
        self.assertNotIn(USERNAME, repr(workspace.statement_execution.calls))
        self.assertNotIn(PASSWORD, repr(workspace.statement_execution.calls))

    def test_execution_fails_unless_statement_succeeds(self):
        workspace = _FakeWorkspace(
            statement_state="FAILED",
            statement_message="permission denied",
        )
        with self.assertRaisesRegex(
            RuntimeError, "FAILED: permission denied"
        ):
            execute_create_connection(
                workspace, "warehouse-1", _spec(), execute=True
            )

    @patch("databricks.labs.sdp_meta.lfc.connections.time.sleep")
    def test_execution_polls_until_statement_succeeds(self, sleep):
        workspace = _FakeWorkspace(
            statement_state="PENDING",
            statement_followup_states=("RUNNING", "SUCCEEDED"),
        )

        execute_create_connection(
            workspace, "warehouse-1", _spec(), execute=True
        )

        self.assertEqual(sleep.call_count, 2)
        self.assertEqual(
            workspace.statement_execution.calls[-2:],
            [{"get": "statement-1"}, {"get": "statement-1"}],
        )

    def test_execution_rejects_placeholder(self):
        workspace = _FakeWorkspace()
        spec = _spec(host="${POSTGRES_HOST}")
        with self.assertRaisesRegex(ValueError, "placeholder"):
            execute_create_connection(
                workspace, "warehouse-1", spec, execute=True
            )
        self.assertEqual(workspace.statement_execution.calls, [])

    def test_execution_rejects_placeholder_warehouse(self):
        workspace = _FakeWorkspace()
        with self.assertRaisesRegex(ValueError, "placeholder"):
            execute_create_connection(
                workspace, "${WAREHOUSE_ID}", _spec(), execute=True
            )
        self.assertEqual(workspace.statement_execution.calls, [])

    def test_dry_render_can_show_placeholder_without_execution(self):
        workspace = _FakeWorkspace()
        sql = execute_create_connection(
            workspace,
            "warehouse-1",
            _spec(host="${POSTGRES_HOST}"),
            execute=False,
        )
        self.assertIn("${POSTGRES_HOST}", sql)

    def test_managed_missing_connection_can_be_created(self):
        result = preflight_connection(
            _FakeWorkspace(), "pg_connection", managed=True
        )
        self.assertEqual(result.action, PreflightAction.CREATE)

    def test_managed_existing_connection_is_reused(self):
        result = preflight_connection(
            _FakeWorkspace(connections=("pg_connection",)),
            "pg_connection",
            managed=True,
        )
        self.assertEqual(result.action, PreflightAction.REUSE_MANAGED)

    def test_managed_existing_connection_rejects_visible_drift(self):
        workspace = _FakeWorkspace(connections=("postgres_connection",))
        workspace.connections.get = lambda **_: SimpleNamespace(
            name="postgres_connection",
            connection_type="POSTGRESQL",
            options={
                "host": "old.example.com",
                "port": "5432",
                "database": "sales",
            },
        )

        with self.assertRaisesRegex(ValueError, "differs"):
            preflight_connection(
                workspace,
                "postgres_connection",
                managed=True,
                desired=_spec(),
            )

    def test_unmanaged_existing_connection_is_reused(self):
        result = preflight_connection(
            _FakeWorkspace(connections=("pg_connection",)),
            "pg_connection",
            managed=False,
        )
        self.assertEqual(result.action, PreflightAction.REUSE_UNMANAGED)

    def test_unmanaged_missing_connection_fails(self):
        with self.assertRaisesRegex(ValueError, "does not exist"):
            preflight_connection(
                _FakeWorkspace(), "pg_connection", managed=False
            )

    def test_preflight_propagates_non_not_found_errors(self):
        failure = RuntimeError("permission denied")
        workspace = _FakeWorkspace(connection_error=failure)
        with self.assertRaises(RuntimeError) as ctx:
            preflight_connection(workspace, "pg_connection", managed=True)
        self.assertIs(ctx.exception, failure)


if __name__ == "__main__":
    unittest.main()
