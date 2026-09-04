import json
from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.sdp_meta.cli import (
    SDPMeta,
    _is_truthy_flag,
    ingestion_generate,
    lfc_connection,
    main,
)


def test_connections_defaults_to_non_executing_sql_preview(capsys):
    workspace = MagicMock()
    workspace.connections.get.side_effect = type(
        "NotFoundError",
        (Exception,),
        {"status_code": 404},
    )()

    lfc_connection(
        SDPMeta(workspace),
        {
            "connection-name": "orders_connection",
            "host": "orders.example.com",
            "scope": "orders_scope",
        },
    )

    workspace.statement_execution.execute_statement.assert_not_called()
    output = capsys.readouterr().out
    assert "CREATE CONNECTION IF NOT EXISTS orders_connection" in output
    assert "secret('orders_scope', 'username')" in output
    assert "secret('orders_scope', 'password')" in output


def test_connections_reuses_unmanaged_connection_without_sql(capsys):
    workspace = MagicMock()
    workspace.connections.get.return_value = {"name": "orders_connection"}

    lfc_connection(
        SDPMeta(workspace),
        {
            "connection-name": "orders_connection",
            "managed": "false",
        },
    )

    workspace.statement_execution.execute_statement.assert_not_called()
    assert "reusing unmanaged connection" in capsys.readouterr().out


def test_connections_rejects_invalid_managed_boolean():
    workspace = MagicMock()

    with pytest.raises(ValueError, match="true/false"):
        lfc_connection(
            SDPMeta(workspace),
            {
                "connection-name": "orders_connection",
                "managed": "flase",
            },
        )


def test_ingestion_generate_writes_and_checks_bundle_resources(tmp_path):
    onboarding = tmp_path / "onboarding.yml"
    onboarding.write_text(
        """
- data_flow_id: "300"
  data_flow_group: orders
  ingestion:
    source:
      type: POSTGRESQL
      catalog: orders_foreign
      schema: public
      connection: orders_connection
    target:
      catalog_prod: main
      schema: orders
    tables:
      - customers
"""
    )
    resources = tmp_path / "resources"
    flags = {
        "onboarding-file": str(onboarding),
        "resources-dir": str(resources),
        "env": "prod",
    }

    ingestion_generate(None, flags)
    generated = resources / "orders.ingestion.gen.yml"
    assert generated.exists()
    assert "ingestion_gateway_id" in generated.read_text()

    ingestion_generate(None, {**flags, "check": "true"})


def test_ingestion_generate_check_detects_drift(tmp_path):
    onboarding = tmp_path / "onboarding.yml"
    onboarding.write_text(
        """
- data_flow_id: "300"
  data_flow_group: orders
  ingestion:
    source:
      type: POSTGRESQL
      catalog: orders_foreign
      schema: public
      connection: orders_connection
    target: {catalog: main, schema: orders}
    tables: [customers]
"""
    )

    with pytest.raises(ValueError, match="stale"):
        ingestion_generate(
            None,
            {
                "onboarding-file": str(onboarding),
                "resources-dir": str(tmp_path / "resources"),
                "check": "true",
            },
        )


def test_ingestion_generate_strict_rejects_unknown_keys(tmp_path):
    onboarding = tmp_path / "onboarding.yml"
    onboarding.write_text(
        """
- data_flow_id: "300"
  data_flow_group: orders
  ingestion:
    source:
      type: POSTGRESQL
      catalog: orders_foreign
      schema: public
      connection: orders_connection
      slots: typo
    target: {catalog: main, schema: orders}
    tables: [customers]
"""
    )

    with pytest.raises(ValueError, match="unknown key 'slots'"):
        ingestion_generate(
            None,
            {
                "onboarding-file": str(onboarding),
                "resources-dir": str(tmp_path / "resources"),
                "strict": "true",
            },
        )


def test_boolean_flag_typos_are_rejected():
    with pytest.raises(ValueError, match="true/false"):
        _is_truthy_flag("flase")


def test_main_forwards_flags_to_ingestion_generate():
    handler = MagicMock()
    payload = json.dumps({
        "command": "ingestion-generate",
        "flags": {
            "log_level": "disabled",
            "onboarding-file": "onboarding.yml",
        },
    })

    with patch(
        "databricks.labs.sdp_meta.cli.WorkspaceClient"
    ), patch.dict(
        "databricks.labs.sdp_meta.cli.MAPPING",
        {"ingestion-generate": handler},
    ):
        main(payload)

    handler.assert_called_once()
    assert handler.call_args.kwargs["flags"]["onboarding-file"] == (
        "onboarding.yml"
    )


def test_generation_preserves_files_owned_by_another_onboarding(tmp_path):
    resources = tmp_path / "resources"

    def write_onboarding(path, flow_id, group):
        path.write_text(
            f"""
- data_flow_id: "{flow_id}"
  data_flow_group: {group}
  ingestion:
    source:
      type: POSTGRESQL
      catalog: orders_foreign
      connection: orders_connection
    target: {{catalog: main, schema: {group}}}
    tables: [customers]
"""
        )

    first = tmp_path / "first.yml"
    second = tmp_path / "second.yml"
    write_onboarding(first, "300", "orders")
    write_onboarding(second, "301", "customers")
    for onboarding in (first, second, first):
        ingestion_generate(
            None,
            {
                "onboarding-file": str(onboarding),
                "resources-dir": str(resources),
            },
        )

    assert (resources / "orders.ingestion.gen.yml").exists()
    assert (resources / "customers.ingestion.gen.yml").exists()


def test_generation_rejects_cross_manifest_filename_collision(tmp_path):
    resources = tmp_path / "resources"
    first = tmp_path / "first.yml"
    second = tmp_path / "second.yml"
    template = """
- data_flow_id: "{flow_id}"
  data_flow_group: orders
  ingestion:
    source:
      type: POSTGRESQL
      catalog: orders_foreign
      connection: orders_connection
    target: {{catalog: main, schema: orders}}
    tables: [customers]
"""
    first.write_text(template.format(flow_id="300"))
    second.write_text(template.format(flow_id="301"))
    flags = {"resources-dir": str(resources)}
    ingestion_generate(
        None, {**flags, "onboarding-file": str(first)}
    )

    with pytest.raises(ValueError, match="ownership collision"):
        ingestion_generate(
            None, {**flags, "onboarding-file": str(second)}
        )
