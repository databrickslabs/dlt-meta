"""Standalone runner for the SDP-META OSS Spark integration test suite.

Mirrors the shape of ``integration_tests/run_integration_tests.py`` —
``init_runner_conf`` → onboarding → bronze → silver → row-count
validation → result CSV → cleanup — but for the OSS Apache Spark code
path. Runs entirely locally: no Databricks workspace, no UC catalog,
no notebooks. The bronze + silver pipelines execute against a
local Spark + Delta session via the same code path that drives the
pytest suite under ``integration_tests/oss/``.

Usage::

    # Single scenario, default temp workdir.
    python integration_tests/run_oss_integration_tests.py --source=json

    # All supported scenarios, persistent workdir for debugging.
    python integration_tests/run_oss_integration_tests.py \\
        --source=all \\
        --workdir=/tmp/sdp_meta_oss_it \\
        --keep_artifacts

Output: ``integration_test_output_<run_id>.csv`` in the CWD (or
``--output_file_path`` if provided). One row per validation
assertion; ``status`` is ``PASS`` / ``FAIL`` so the file is grep-able
in CI logs.

Exit code:
    0 — every scenario PASSED.
    1 — at least one scenario FAILED (or a hard error fired).

Set ``SDP_META_KEEP_ARTIFACTS=1`` (or pass ``--keep_artifacts``) to
preserve the workdir contents after the run for inspection. By
default the workdir is removed after a successful run unless it
was passed in via ``--workdir`` (caller-owned).

Supported ``--source`` values:

  ``json``        — vanilla file source, JSON (mirrors test_oss_json_flow)
  ``csv``         — vanilla file source, CSV  (mirrors test_oss_csv_flow)
  ``parquet``     — vanilla file source, Parquet
  ``delta``       — Delta source             (mirrors test_oss_delta_flow)
  ``dqe``         — DQE expect_or_drop       (mirrors test_oss_dqe_flow)
  ``cdc_raises``  — CDC NotImplementedError  (mirrors test_oss_cdc_raises)
  ``all``         — run every scenario above
"""
from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
import tempfile
import traceback
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Side-effect import: installs ``sys.modules["pyspark.pipelines"]`` and
# pins ``SDP_META_RUNTIME=oss`` before any sdp-meta module loads. Must
# come BEFORE the executor / fixtures imports (they transitively
# import sdp-meta). See ``integration_tests/oss/_recorder.py``.
from integration_tests.oss._recorder import DP_RECORDER as _DP_RECORDER  # noqa: E402
from integration_tests.oss._executor import FakeOSSPipelineExecutor  # noqa: E402
from integration_tests.oss._fixtures import (  # noqa: E402
    write_onboarding_template,
    write_source_data_in_format,
)

# Supported scenarios — keep the keys aligned with the ``--source``
# choices in :func:`_parse_args`. Each scenario is a callable
# ``run(spark, workdir, ctx) -> list[_Validation]``; the framework
# below collects validations into the output CSV.
SUPPORTED_SOURCES = (
    "json",
    "csv",
    "parquet",
    "delta",
    "dqe",
    "cdc_raises",
)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class _Validation:
    """One row in the integration-test-output CSV."""

    scenario: str
    layer: str
    table: str
    metric: str
    expected: str
    actual: str
    status: str  # "PASS" | "FAIL"
    note: str = ""


@dataclass
class SDPMetaOSSRunnerConf:
    """Per-run configuration mirroring :class:`SDPMetaRunnerConf` shape.

    Fields are deliberately a strict subset of the Databricks runner —
    no profile, no UC catalog, no notebook paths. Everything else is
    derived from CLI args + per-run state.
    """

    run_id: str
    source: str
    workdir: Path
    output_file_path: Path
    onboarding_file_format: str = "json"
    caller_owned_workdir: bool = False
    keep_artifacts: bool = False
    validations: list[_Validation] = field(default_factory=list)
    scenarios_run: list[str] = field(default_factory=list)
    scenarios_failed: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Spark session + flow driver — minimal copy of the conftest helpers
# (intentionally inlined so this runner doesn't import pytest).
# ---------------------------------------------------------------------------


def _build_local_spark(warehouse_dir: Path) -> Any:
    """Local Spark + Delta session. Same shape as conftest's ``spark``."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "pyspark is not installed; install ``pyspark`` to run OSS "
            "integration tests"
        ) from exc

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("sdp-meta-oss-it-runner")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .config("spark.databricks.unityCatalog.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _isolate_catalog(spark: Any, schemas: tuple[str, ...] = ("bronze", "silver")) -> None:
    """Drop schemas eagerly so cross-scenario state doesn't leak.

    Same contract as the autouse ``_oss_catalog_isolation`` fixture
    in the pytest conftest: the in-memory Hive catalog persists across
    SparkSession operations within the same JVM, so previous scenario
    table registrations would surface as stale-LOCATION warnings or
    schema mismatches on the next scenario.
    """
    for schema in schemas:
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        except Exception:
            pass


def _substitute_paths(raw: str, repo_root: Path) -> str:
    """Inline copy of conftest's ``_substitute_source_paths``."""
    rels = (
        "tests/resources/data/customers",
        "tests/resources/data/transactions",
        "tests/resources/schema/customer_schema.ddl",
        "tests/resources/schema/transactions_schema.ddl",
        "tests/resources/silver_transformations.json",
        "tests/resources/dqe/customers/bronze_data_quality_expectations.json",
    )
    for rel in rels:
        raw = raw.replace(f'"{rel}"', f'"{repo_root / rel}"')
    return raw


def _materialize_onboarding(
    template_path: Path,
    workdir: Path,
    repo_root: Path,
    *,
    scenario: str,
) -> tuple[Path, dict[str, str]]:
    """Inline copy of conftest's ``materialize_onboarding`` fixture."""
    raw = template_path.read_text()
    raw = _substitute_paths(raw, repo_root)
    bronze_customers = str(workdir / scenario / "bronze" / "customers")
    silver_customers = str(workdir / scenario / "silver" / "customers")
    bronze_transactions = str(workdir / scenario / "bronze" / "transactions")
    silver_transactions = str(workdir / scenario / "silver" / "transactions")
    for placeholder, real in {
        "__BRONZE_CUSTOMERS_PATH__": bronze_customers,
        "__SILVER_CUSTOMERS_PATH__": silver_customers,
        "__BRONZE_TRANSACTIONS_PATH__": bronze_transactions,
        "__SILVER_TRANSACTIONS_PATH__": silver_transactions,
    }.items():
        raw = raw.replace(placeholder, real)
        Path(real).parent.mkdir(parents=True, exist_ok=True)
    out = workdir / f"{scenario}_onboarding.json"
    out.write_text(raw)
    return out, {
        "bronze.customers": bronze_customers,
        "bronze.transactions": bronze_transactions,
        "silver.customers": silver_customers,
        "silver.transactions": silver_transactions,
    }


def _drive_flow(
    spark: Any,
    workdir: Path,
    repo_root: Path,
    template_path: Path,
    *,
    scenario: str,
) -> dict[str, Any]:
    """Full onboarding → bronze → silver flow. Same shape as conftest's
    ``oss_flow_driver`` fixture, sans pytest plumbing.
    """
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
    from databricks.labs.sdp_meta import oss_pipelines as _oss

    onboarding_file, expected_table_paths = _materialize_onboarding(
        template_path, workdir, repo_root, scenario=scenario
    )

    meta_schema = f"sdp_meta_{scenario}"
    _isolate_catalog(spark, ("bronze", "silver", meta_schema))
    _DP_RECORDER.clear()
    _oss.reset_kwarg_warning_state()

    bronze_spec_path = str(workdir / scenario / "spec" / "bronze")
    silver_spec_path = str(workdir / scenario / "spec" / "silver")
    Path(bronze_spec_path).parent.mkdir(parents=True, exist_ok=True)

    OnboardDataflowspec(
        spark,
        {
            "onboarding_file_path": str(onboarding_file),
            "database": meta_schema,
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "bronze_dataflowspec_path": bronze_spec_path,
            "silver_dataflowspec_table": "silver_dataflowspec",
            "silver_dataflowspec_path": silver_spec_path,
            "import_author": "oss-it-runner",
            "version": "v1",
            "overwrite": "True",
        },
        uc_enabled=False,
    ).onboard_dataflow_specs()

    spark.conf.set("layer", "bronze_silver")
    spark.conf.set("bronze.dataflowspecPath", bronze_spec_path)
    spark.conf.set("silver.dataflowspecPath", silver_spec_path)
    spark.conf.set("bronze.group", "OSS")
    spark.conf.set("silver.group", "OSS")

    _DP_RECORDER.clear()
    DataflowPipeline.invoke_pipeline(spark, "bronze")
    bronze = FakeOSSPipelineExecutor(
        spark, _DP_RECORDER, checkpoint_dir=workdir / "_checkpoints"
    ).execute()

    _DP_RECORDER.clear()
    DataflowPipeline.invoke_pipeline(spark, "silver")
    silver = FakeOSSPipelineExecutor(
        spark, _DP_RECORDER, checkpoint_dir=workdir / "_checkpoints"
    ).execute()

    return {
        "bronze": bronze,
        "silver": silver,
        "expected_table_paths": expected_table_paths,
        "onboarding_file": onboarding_file,
    }


# ---------------------------------------------------------------------------
# Per-scenario drivers
# ---------------------------------------------------------------------------


def _validate_flow_result(
    scenario: str,
    result: dict[str, Any],
    *,
    expect_silver_le_bronze: bool = True,
) -> list[_Validation]:
    """Common assertions for a JSON/CSV/Parquet/Delta full-flow scenario.

    Builds one validation row per table-count check + one per
    silver≤bronze invariant.
    """
    rows: list[_Validation] = []
    bronze_counts = result["bronze"].row_counts
    silver_counts = result["silver"].row_counts
    failures = result["bronze"].failures + result["silver"].failures

    if failures:
        for name, exc in failures:
            rows.append(
                _Validation(
                    scenario=scenario,
                    layer="executor",
                    table=name,
                    metric="execute",
                    expected="no exception",
                    actual=type(exc).__name__,
                    status="FAIL",
                    note=str(exc)[:240],
                )
            )

    for table, count in bronze_counts.items():
        rows.append(
            _Validation(
                scenario=scenario,
                layer="bronze",
                table=table,
                metric="row_count_positive",
                expected=">0",
                actual=str(count),
                status="PASS" if count > 0 else "FAIL",
            )
        )
    for table, count in silver_counts.items():
        rows.append(
            _Validation(
                scenario=scenario,
                layer="silver",
                table=table,
                metric="row_count_positive",
                expected=">0",
                actual=str(count),
                status="PASS" if count > 0 else "FAIL",
            )
        )

    if expect_silver_le_bronze:
        for bronze_name, silver_name in (
            ("bronze.customers", "silver.customers"),
            ("bronze.transactions", "silver.transactions"),
        ):
            if bronze_name in bronze_counts and silver_name in silver_counts:
                bc = bronze_counts[bronze_name]
                sc = silver_counts[silver_name]
                rows.append(
                    _Validation(
                        scenario=scenario,
                        layer="silver",
                        table=silver_name,
                        metric="silver_le_bronze",
                        expected=f"<= {bc}",
                        actual=str(sc),
                        status="PASS" if sc <= bc else "FAIL",
                    )
                )
    return rows


def _scenario_json(spark, workdir, repo_root) -> list[_Validation]:
    template = repo_root / "tests" / "resources" / "oss_onboarding.json"
    if not template.exists():
        return [
            _Validation(
                scenario="json", layer="setup", table="-",
                metric="template_exists",
                expected=str(template), actual="missing",
                status="FAIL", note="committed OSS onboarding template not found",
            )
        ]
    result = _drive_flow(spark, workdir, repo_root, template, scenario="json")
    return _validate_flow_result("json", result)


def _scenario_file_source(
    spark, workdir, repo_root, *, fmt: str
) -> list[_Validation]:
    template = write_onboarding_template(
        workdir, fmt=fmt, repo_root=repo_root, spark=spark
    )
    result = _drive_flow(spark, workdir, repo_root, template, scenario=fmt)
    return _validate_flow_result(fmt, result)


def _scenario_delta(spark, workdir, repo_root) -> list[_Validation]:
    # Reuse the test-file helpers verbatim — they're already validated.
    from integration_tests.oss.test_oss_delta_flow import (  # noqa: WPS433
        _register_delta_source_tables,
        _render_delta_onboarding,
        _write_delta_compatible_silver_transformations,
    )

    source_dir = workdir / "_sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    paths = write_source_data_in_format(spark, repo_root, source_dir, "delta")
    _register_delta_source_tables(spark, paths)

    silver_xforms = _write_delta_compatible_silver_transformations(
        workdir / "silver_transformations_delta.json"
    )
    template_path = workdir / "oss_onboarding_delta.json"
    template_path.write_text(
        _render_delta_onboarding(
            customers_path=paths["customers"],
            transactions_path=paths["transactions"],
            silver_transformation_path=str(silver_xforms),
        )
    )
    result = _drive_flow(spark, workdir, repo_root, template_path, scenario="delta")
    rows = _validate_flow_result("delta", result)

    # Identity-bronze check: row counts must match source.
    expected_source_counts = {
        "bronze.customers": spark.read.format("delta").load(paths["customers"]).count(),
        "bronze.transactions": spark.read.format("delta").load(paths["transactions"]).count(),
    }
    bronze_counts = result["bronze"].row_counts
    for table, expected in expected_source_counts.items():
        actual = bronze_counts.get(table, 0)
        rows.append(
            _Validation(
                scenario="delta",
                layer="bronze",
                table=table,
                metric="row_count_matches_source",
                expected=str(expected),
                actual=str(actual),
                status="PASS" if actual == expected else "FAIL",
            )
        )
    return rows


def _scenario_dqe(spark, workdir, repo_root) -> list[_Validation]:
    from integration_tests.oss.test_oss_dqe_flow import (  # noqa: WPS433
        _write_dqe_source,
        _write_dqe_schema_ddl,
        _write_dqe_expectations,
        _render_dqe_onboarding,
        _write_dqe_silver_transformation,
    )

    source_dir, total_rows, expected_kept_rows = _write_dqe_source(workdir)
    schema_ddl = _write_dqe_schema_ddl(workdir)
    expectations = _write_dqe_expectations(workdir)
    silver_xforms = _write_dqe_silver_transformation(
        workdir / "silver_transformations_dqe.json"
    )
    template_path = workdir / "oss_onboarding_dqe.json"
    template_path.write_text(
        _render_dqe_onboarding(
            source_path=str(source_dir),
            source_schema_path=str(schema_ddl),
            dqe_expectations_path=str(expectations),
            silver_transformation_path=str(silver_xforms),
        )
    )
    result = _drive_flow(spark, workdir, repo_root, template_path, scenario="dqe")
    rows = _validate_flow_result("dqe", result, expect_silver_le_bronze=False)
    bronze_count = result["bronze"].row_counts.get("bronze.customers", -1)
    silver_count = result["silver"].row_counts.get("silver.customers", -1)
    rows.append(
        _Validation(
            scenario="dqe",
            layer="bronze",
            table="bronze.customers",
            metric="dqe_filtered_count",
            expected=str(expected_kept_rows),
            actual=str(bronze_count),
            status="PASS" if bronze_count == expected_kept_rows else "FAIL",
            note=f"source={total_rows} rows",
        )
    )
    rows.append(
        _Validation(
            scenario="dqe",
            layer="silver",
            table="silver.customers",
            metric="silver_equals_bronze_identity",
            expected=str(bronze_count),
            actual=str(silver_count),
            status="PASS" if silver_count == bronze_count else "FAIL",
        )
    )
    return rows


def _scenario_cdc_raises(spark, workdir, repo_root) -> list[_Validation]:
    """CDC must raise ``NotImplementedError`` naming ``create_auto_cdc_flow``."""
    from integration_tests.oss.test_oss_cdc_raises import (  # noqa: WPS433
        _write_cdc_onboarding,
    )
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
    from databricks.labs.sdp_meta import oss_pipelines as _oss

    template = _write_cdc_onboarding(
        workdir,
        customers_source=str(repo_root / "tests" / "resources" / "data" / "customers"),
        customers_schema=str(
            repo_root / "tests" / "resources" / "schema" / "customer_schema.ddl"
        ),
        silver_transformation=str(
            repo_root / "tests" / "resources" / "silver_transformations.json"
        ),
    )
    onboarding_file, _ = _materialize_onboarding(
        template, workdir, repo_root, scenario="cdc_raises"
    )
    bronze_spec = str(workdir / "cdc_raises" / "spec" / "bronze")
    silver_spec = str(workdir / "cdc_raises" / "spec" / "silver")
    Path(bronze_spec).parent.mkdir(parents=True, exist_ok=True)
    _isolate_catalog(spark, ("bronze", "silver", "sdp_meta_cdc_raises"))
    _DP_RECORDER.clear()
    _oss.reset_kwarg_warning_state()
    OnboardDataflowspec(
        spark,
        {
            "onboarding_file_path": str(onboarding_file),
            "database": "sdp_meta_cdc_raises",
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "bronze_dataflowspec_path": bronze_spec,
            "silver_dataflowspec_table": "silver_dataflowspec",
            "silver_dataflowspec_path": silver_spec,
            "import_author": "oss-cdc-runner",
            "version": "v1",
            "overwrite": "True",
        },
        uc_enabled=False,
    ).onboard_dataflow_specs()

    spark.conf.set("layer", "bronze")
    spark.conf.set("bronze.dataflowspecPath", bronze_spec)
    spark.conf.set("bronze.group", "OSS")

    actual = "no exception"
    message_contains_api = False
    try:
        _DP_RECORDER.clear()
        DataflowPipeline.invoke_pipeline(spark, "bronze")
    except NotImplementedError as exc:
        actual = type(exc).__name__
        message_contains_api = "create_auto_cdc_flow" in str(exc).lower()
    except Exception as exc:  # noqa: BLE001
        actual = f"{type(exc).__name__}: {exc}"

    rows = [
        _Validation(
            scenario="cdc_raises",
            layer="bronze",
            table="bronze.customers",
            metric="cdc_raises_not_implemented",
            expected="NotImplementedError",
            actual=actual,
            status="PASS" if actual == "NotImplementedError" else "FAIL",
        ),
        _Validation(
            scenario="cdc_raises",
            layer="bronze",
            table="bronze.customers",
            metric="cdc_message_names_lakeflow_api",
            expected="contains 'create_auto_cdc_flow'",
            actual="yes" if message_contains_api else "no",
            status="PASS" if message_contains_api else "FAIL",
        ),
    ]
    return rows


_SCENARIOS: dict[str, Callable[..., list[_Validation]]] = {
    "json": _scenario_json,
    "csv": lambda spark, wd, rr: _scenario_file_source(spark, wd, rr, fmt="csv"),
    "parquet": lambda spark, wd, rr: _scenario_file_source(spark, wd, rr, fmt="parquet"),
    "delta": _scenario_delta,
    "dqe": _scenario_dqe,
    "cdc_raises": _scenario_cdc_raises,
}


# ---------------------------------------------------------------------------
# CLI + orchestration
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run SDP-META OSS Spark integration tests (local Spark + Delta). "
            "Mirrors run_integration_tests.py shape for the OSS code path."
        )
    )
    parser.add_argument(
        "--source",
        type=str.lower,
        required=False,
        default="all",
        choices=("all", *SUPPORTED_SOURCES),
        help=(
            "Scenario to run. 'all' runs every supported scenario in sequence. "
            "Default: all."
        ),
    )
    parser.add_argument(
        "--workdir",
        type=Path,
        required=False,
        default=None,
        help=(
            "Per-run working directory for materialised test data, Delta "
            "tables, dataflowspec, and the Spark warehouse. Defaults to a "
            "fresh tempdir under the system tmp. When omitted, the workdir "
            "is removed after a successful run (unless --keep_artifacts)."
        ),
    )
    parser.add_argument(
        "--output_file_path",
        type=Path,
        required=False,
        default=None,
        help=(
            "Path to write the integration-test-output CSV. Defaults to "
            "``integration_test_output_<run_id>.csv`` in CWD."
        ),
    )
    parser.add_argument(
        "--onboarding_file_format",
        type=str.lower,
        required=False,
        default="json",
        choices=("json",),
        help="Onboarding spec file format (only ``json`` supported today).",
    )
    parser.add_argument(
        "--keep_artifacts",
        action="store_true",
        help=(
            "Preserve the workdir + warehouse after the run for debugging. "
            "Equivalent to setting ``SDP_META_KEEP_ARTIFACTS=1``."
        ),
    )
    return parser.parse_args(argv)


def _write_results_csv(
    output_path: Path, validations: list[_Validation], run_id: str
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "run_id",
                "scenario",
                "layer",
                "table",
                "metric",
                "expected",
                "actual",
                "status",
                "note",
            ]
        )
        for v in validations:
            writer.writerow(
                [
                    run_id,
                    v.scenario,
                    v.layer,
                    v.table,
                    v.metric,
                    v.expected,
                    v.actual,
                    v.status,
                    v.note,
                ]
            )


def _print_summary(conf: SDPMetaOSSRunnerConf) -> None:
    total = len(conf.validations)
    passed = sum(1 for v in conf.validations if v.status == "PASS")
    failed = total - passed
    print("\n" + "=" * 72)
    print(f"SDP-META OSS integration test summary — run_id={conf.run_id}")
    print("=" * 72)
    print(f"  scenarios run    : {', '.join(conf.scenarios_run) or '(none)'}")
    if conf.scenarios_failed:
        print(f"  scenarios FAILED : {', '.join(conf.scenarios_failed)}")
    print(f"  validations      : {passed} PASS, {failed} FAIL ({total} total)")
    print(f"  output csv       : {conf.output_file_path}")
    print(f"  workdir          : {conf.workdir}"
          f"{' (preserved)' if conf.keep_artifacts else ''}")
    print("=" * 72)
    if failed:
        print("\nFailed validations:")
        for v in conf.validations:
            if v.status == "FAIL":
                print(
                    f"  [{v.scenario}/{v.layer}/{v.table}] {v.metric}: "
                    f"expected {v.expected!r}, got {v.actual!r}"
                    + (f" — {v.note}" if v.note else "")
                )


def run(conf: SDPMetaOSSRunnerConf) -> int:
    """Drive every requested scenario and write the result CSV.

    Returns the process exit code: 0 if every validation PASSED,
    1 if at least one FAILED or a hard error fired.
    """
    repo_root = PROJECT_ROOT
    warehouse_dir = conf.workdir / "_warehouse"
    spark = _build_local_spark(warehouse_dir)

    scenarios = (
        list(SUPPORTED_SOURCES) if conf.source == "all" else [conf.source]
    )

    for scenario in scenarios:
        print(f"\n[OSS-IT] running scenario: {scenario}")
        scenario_workdir = conf.workdir / scenario
        scenario_workdir.mkdir(parents=True, exist_ok=True)
        try:
            rows = _SCENARIOS[scenario](spark, scenario_workdir, repo_root)
        except Exception as exc:  # noqa: BLE001
            print(f"[OSS-IT] scenario {scenario!r} crashed: {exc}")
            traceback.print_exc()
            rows = [
                _Validation(
                    scenario=scenario,
                    layer="runner",
                    table="-",
                    metric="scenario_completed",
                    expected="no exception",
                    actual=type(exc).__name__,
                    status="FAIL",
                    note=str(exc)[:240],
                )
            ]
        conf.validations.extend(rows)
        conf.scenarios_run.append(scenario)
        if any(v.status == "FAIL" for v in rows):
            conf.scenarios_failed.append(scenario)

    _write_results_csv(conf.output_file_path, conf.validations, conf.run_id)
    _print_summary(conf)
    return 0 if not conf.scenarios_failed else 1


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_id = uuid.uuid4().hex[:12]

    # Workdir: caller-owned (preserve on exit) or runner-owned (cleanup
    # on success unless --keep_artifacts).
    if args.workdir is not None:
        workdir = args.workdir
        workdir.mkdir(parents=True, exist_ok=True)
        caller_owned = True
    else:
        workdir = Path(
            tempfile.mkdtemp(prefix=f"sdp_meta_oss_it_{run_id}_")
        )
        caller_owned = False

    keep_artifacts = (
        args.keep_artifacts
        or os.environ.get("SDP_META_KEEP_ARTIFACTS", "").strip().lower()
        in ("1", "true", "yes", "on")
    )

    output_file_path = args.output_file_path or Path(
        f"integration_test_output_{run_id}.csv"
    )

    conf = SDPMetaOSSRunnerConf(
        run_id=run_id,
        source=args.source,
        workdir=workdir,
        output_file_path=output_file_path.resolve(),
        onboarding_file_format=args.onboarding_file_format,
        caller_owned_workdir=caller_owned,
        keep_artifacts=keep_artifacts,
    )

    exit_code = 1
    try:
        exit_code = run(conf)
    finally:
        # Cleanup mirrors run_integration_tests.py: skip on
        # --keep_artifacts or SDP_META_KEEP_ARTIFACTS=1, AND skip
        # when the caller passed in --workdir (we never own
        # caller-supplied directories).
        if keep_artifacts:
            print(
                "\nSDP_META_KEEP_ARTIFACTS / --keep_artifacts set; "
                f"preserving workdir at {workdir}"
            )
        elif caller_owned:
            print(f"\n--workdir was caller-supplied; preserving {workdir}")
        else:
            try:
                shutil.rmtree(workdir, ignore_errors=True)
                print(f"\nCleaned up workdir {workdir}")
            except Exception as exc:  # noqa: BLE001
                print(f"\nCleanup of {workdir} failed: {exc}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
