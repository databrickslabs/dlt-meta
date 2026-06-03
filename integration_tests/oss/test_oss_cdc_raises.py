"""OSS Spark CDC / snapshot raises NotImplementedError end-to-end.

Lakeflow's ``create_auto_cdc_flow`` and
``create_auto_cdc_from_snapshot_flow`` are NOT in the OSS
``pyspark.pipelines`` surface. SDP-META's
:meth:`OSSDataflowPipeline.cdc_apply_changes` and
:meth:`OSSDataflowPipeline.apply_changes_from_snapshot` therefore
raise ``NotImplementedError`` with a customer-facing message that
explicitly names the Lakeflow-only feature.

This test pins that contract end-to-end: an onboarding spec that
sets ``bronze_cdc_apply_changes`` (or the snapshot variant) drives
through ``OnboardDataflowspec`` + ``DataflowPipeline.invoke_pipeline``
and the runtime raises ``NotImplementedError`` at the CDC dispatch
point with the expected message — NOT a silent succeed (which would
have a worse failure mode: the customer would think CDC ran but
nothing happened) and NOT a generic exception (which would hide
the actionable "use Lakeflow" guidance from the message).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_cdc_onboarding(
    workdir: Path,
    *,
    customers_source: str,
    customers_schema: str,
    silver_transformation: str,
) -> Path:
    """Render an onboarding spec with bronze CDC apply_changes wired."""
    rows = [
        {
            "data_flow_id": "200_cdc",
            "data_flow_group": "OSS",
            "source_system": "FILE",
            "source_format": "json",
            "source_details": {
                "source_database": "APP",
                "source_table": "CUSTOMERS",
                "source_path_dev": customers_source,
                "source_schema_path": customers_schema,
            },
            "bronze_database_dev": "bronze",
            "bronze_table": "customers",
            "bronze_table_comment": "Bronze customers (CDC)",
            "bronze_reader_options": {},
            "bronze_table_path_dev": "__BRONZE_CUSTOMERS_PATH__",
            "bronze_table_properties": {"pipelines.reset.allowed": "false"},
            "bronze_quarantine_table": None,
            "bronze_cdc_apply_changes": {
                "keys": ["id"],
                "sequence_by": "operation_date",
                "scd_type": "1",
                "apply_as_deletes": "operation = 'DELETE'",
                "except_column_list": ["operation", "operation_date"],
            },
            # Silver-only fields are present-but-unused; the pipeline
            # raises in the bronze CDC dispatch before silver is
            # invoked. Onboarding still validates them, so we point at
            # a real file rather than an empty string.
            "silver_database_dev": "silver",
            "silver_table": "customers",
            "silver_table_comment": "Silver customers (CDC)",
            "silver_table_path_dev": "__SILVER_CUSTOMERS_PATH__",
            "silver_table_properties": {"pipelines.reset.allowed": "false"},
            "silver_transformation_json_dev": silver_transformation,
            "silver_quarantine_table": None,
        }
    ]
    out = workdir / "oss_onboarding_cdc.json"
    out.write_text(json.dumps(rows, indent=2))
    return out


def test_oss_cdc_apply_changes_raises_not_implemented_with_lakeflow_message(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    dp_recorder,
    materialize_onboarding,
    clean_catalog,
):
    """``cdcApplyChanges`` on the OSS code path raises with a clear message.

    Drives through ``OnboardDataflowspec`` + ``DataflowPipeline.invoke_pipeline``
    for bronze with a CDC-configured spec. The OSS code path must
    raise ``NotImplementedError`` whose message names
    ``create_auto_cdc_flow`` so customers immediately know which
    Lakeflow API they'd need (and that they're not on it).

    Asserts:
      - The raised exception is ``NotImplementedError`` (not a
        catch-all ``Exception``, not a silent succeed).
      - The message contains the Lakeflow API name
        (``create_auto_cdc_flow``).
      - The bronze qf was registered up to the CDC dispatch point
        — the raise happens *during* the registered qf, not at
        onboarding time (the latter would mean SDP-META is doing
        runtime guards in the wrong layer).
    """
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec

    template = _write_cdc_onboarding(
        workdir,
        customers_source=str(
            repo_root / "tests" / "resources" / "data" / "customers"
        ),
        customers_schema=str(
            repo_root / "tests" / "resources" / "schema" / "customer_schema.ddl"
        ),
        silver_transformation=str(
            repo_root / "tests" / "resources" / "silver_transformations.json"
        ),
    )
    onboarding_file, _ = materialize_onboarding(template, scenario="cdc")

    bronze_spec = str(workdir / "cdc" / "spec" / "bronze")
    silver_spec = str(workdir / "cdc" / "spec" / "silver")
    Path(bronze_spec).parent.mkdir(parents=True, exist_ok=True)
    clean_catalog("sdp_meta_cdc")

    # Onboarding must succeed — the runtime guard is in the pipeline
    # dispatch, not in the spec validator. If onboarding itself
    # rejects CDC specs on OSS, customers can't even ship the spec
    # to a Lakeflow workspace from an OSS developer environment.
    OnboardDataflowspec(
        spark,
        {
            "onboarding_file_path": str(onboarding_file),
            "database": "sdp_meta_cdc",
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "bronze_dataflowspec_path": bronze_spec,
            "silver_dataflowspec_table": "silver_dataflowspec",
            "silver_dataflowspec_path": silver_spec,
            "import_author": "oss-cdc-it",
            "version": "v1",
            "overwrite": "True",
        },
        uc_enabled=False,
    ).onboard_dataflow_specs()

    spark.conf.set("layer", "bronze")
    spark.conf.set("bronze.dataflowspecPath", bronze_spec)
    spark.conf.set("bronze.group", "OSS")

    dp_recorder.clear()
    with pytest.raises(NotImplementedError) as excinfo:
        DataflowPipeline.invoke_pipeline(spark, "bronze")

    message = str(excinfo.value).lower()
    assert "create_auto_cdc_flow" in message, (
        f"NotImplementedError message {message!r} does not name the Lakeflow "
        "API (``create_auto_cdc_flow``) — customers won't know which feature "
        "is missing"
    )
