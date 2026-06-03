"""Shared fixtures for the SDP-META OSS Spark integration test suite.

Critical: the recorder stub and ``SDP_META_RUNTIME=oss`` override are
installed at **module import time**, before pytest collects any test
file. That ordering is required because
``databricks.labs.sdp_meta.dataflow_pipeline`` binds
``from pyspark import pipelines as dp`` at module top — once that
binding is taken, swapping ``sys.modules["pyspark.pipelines"]`` later
cannot re-bind it. The actual install happens in
:mod:`integration_tests.oss._recorder`, which this conftest imports at
the top so the side effect fires before any test module loads.

The standalone runner under ``integration_tests/run_oss_integration_tests.py``
also imports ``_recorder`` directly (not this conftest), so both
entry points share the same singleton without one depending on the
other's pytest plumbing.

The same recorder instance is reused across the session (its ``calls``
list is cleared per-test by ``dp_recorder``). Re-installing fresh
recorders mid-session would break the already-captured ``dp`` binding
in sdp-meta's internal modules.
"""
from __future__ import annotations

import logging
from pathlib import Path

import pytest

# Side-effect import: installs ``sys.modules["pyspark.pipelines"]`` and
# pins ``SDP_META_RUNTIME=oss`` before any sdp-meta module loads.
from integration_tests.oss._recorder import DP_RECORDER as _DP_RECORDER
from integration_tests.oss._recorder import _DPRecorder


# ---------------------------------------------------------------------------
# Phase 1: safe to import sdp-meta now.
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
TESTS_RESOURCES = REPO_ROOT / "tests" / "resources"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def repo_root() -> Path:
    return REPO_ROOT


@pytest.fixture(scope="session")
def dp_recorder_singleton() -> _DPRecorder:
    """The single recorder installed into ``sys.modules`` at conftest import.

    Session-scoped — see the module docstring on why we can't swap in
    fresh recorders mid-session.
    """
    return _DP_RECORDER


@pytest.fixture
def dp_recorder(dp_recorder_singleton) -> _DPRecorder:
    """Per-test recorder view: clears the call log + warning-dedup state.

    The OSS shim's ``filter_table_kwargs`` de-dups warnings
    process-wide; without resetting that state, a test that asserts a
    warning fires would pass on the first run and silently fail on
    every subsequent run in the same session.
    """
    dp_recorder_singleton.clear()
    from databricks.labs.sdp_meta import oss_pipelines as oss_dp

    oss_dp.reset_kwarg_warning_state()
    yield dp_recorder_singleton


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark + Delta session, session-scoped.

    Skips the entire test if pyspark or delta-spark aren't on the
    classpath (mirrors how the existing Databricks integration tests
    skip when no workspace profile is configured — the integration
    suite needs real infrastructure to be meaningful).

    The Spark warehouse lives under pytest's session-scoped tmp
    tree (``tmp_path_factory``) so it's auto-cleaned across runs
    and never lands in the repo root. Earlier iterations placed it
    at ``<repo>/.pytest_oss_warehouse``, which (a) leaked stale
    schemas between runs and (b) wasn't in .gitignore — easy to
    accidentally commit.
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark is not installed; cannot run OSS integration tests")

    warehouse = tmp_path_factory.mktemp("oss_warehouse")

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("sdp-meta-oss-integration")
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
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.databricks.unityCatalog.enabled", "false")
    )
    try:
        s = builder.getOrCreate()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"could not start local Spark session (delta-spark missing?): {exc}"
        )
    s.sparkContext.setLogLevel("WARN")
    yield s
    # JVM exits with pytest; not calling stop() because session-scoped
    # fixtures can be torn down and re-created when pytest does config
    # re-init across test discovery rounds, and a stopped context
    # cannot be restarted in the same process.


@pytest.fixture
def workdir(tmp_path) -> Path:
    """Fresh per-test workdir under pytest's tmp_path tree."""
    return tmp_path


@pytest.fixture
def oss_runtime_env(monkeypatch):
    """Force ``SDP_META_RUNTIME=oss`` for the test."""
    monkeypatch.setenv("SDP_META_RUNTIME", "oss")
    yield


@pytest.fixture
def caplog_oss_shim(caplog):
    """Capture logs from the OSS shim at DEBUG level."""
    caplog.set_level(logging.DEBUG, logger="databricks.labs.sdp_meta.oss_pipelines")
    return caplog


@pytest.fixture(autouse=True)
def _oss_catalog_isolation(request):
    """Drop ``bronze`` / ``silver`` schemas eagerly between tests.

    The local Spark session uses an in-memory Hive catalog scoped to
    the entire pytest session. Without cleanup, a previous test's
    ``bronze.customers`` external-table registration survives into the
    next test — and the OSS shim's ``ensure_external_delta_table``
    side-channel then warns about a LOCATION mismatch and silently
    keeps writing to the stale path. That breaks downstream silver
    reads in confusing ways. Drop the schemas pre-test so every test
    starts from a clean catalog.

    Autouse and unconditional: runs for every test in this suite,
    including ones that bypass the high-level ``oss_flow_driver``
    fixture. Skipped only if pytest didn't construct the ``spark``
    fixture (e.g. delta-spark missing) — in that case the test will
    skip via the fixture's own ``pytest.skip``.
    """
    if "spark" not in request.fixturenames:
        yield
        return
    spark = request.getfixturevalue("spark")
    for schema in ("bronze", "silver"):
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        except Exception:
            pass
    yield


@pytest.fixture
def clean_catalog(spark):
    """Track ad-hoc schemas + their tables so teardown drops them.

    Returns a register callable for tests that create extra schemas
    beyond ``bronze`` / ``silver`` (which the autouse
    ``_oss_catalog_isolation`` covers). Pre-test ``DROP SCHEMA IF
    EXISTS bronze/silver CASCADE`` already happens; this fixture is
    for the sdp-meta dataflowspec schema and any other
    test-introduced schema names.
    """
    schemas: list[str] = []

    def register(schema: str) -> None:
        schemas.append(schema)

    yield register

    for schema in schemas:
        try:
            spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Onboarding template materialization
# ---------------------------------------------------------------------------


_RELATIVE_TO_ABSOLUTE_SUBSTITUTIONS = (
    "tests/resources/data/customers",
    "tests/resources/data/transactions",
    "tests/resources/schema/customer_schema.ddl",
    "tests/resources/schema/transactions_schema.ddl",
    "tests/resources/silver_transformations.json",
    "tests/resources/dqe/customers/bronze_data_quality_expectations.json",
)


def _substitute_source_paths(raw: str, repo_root: Path) -> str:
    """Rewrite committed-spec relative paths to absolute paths.

    The committed ``oss_onboarding.json`` template references source
    data and schema files via paths relative to repo root. Tests are
    invoked from arbitrary CWD so the substitution unconditionally
    points at the on-disk locations.

    Anchored on JSON-string boundaries (``"..."``) so per-format
    templates that already render absolute paths
    (``"/abs/path/tests/resources/..."``) don't get substring-doubled
    into ``"/abs/path/tests/resources/.../abs/path/tests/resources/..."``.
    """
    for rel in _RELATIVE_TO_ABSOLUTE_SUBSTITUTIONS:
        abs_path = str(repo_root / rel)
        raw = raw.replace(f'"{rel}"', f'"{abs_path}"')
    return raw


@pytest.fixture
def materialize_onboarding(workdir, repo_root):
    """Factory: materialize an onboarding template into the workdir.

    Returns a callable ``materialize(template_path, placeholders)``
    that substitutes both the SDP-META-style ``__BRONZE_<...>_PATH__``
    placeholders and the committed-spec relative source-data paths,
    then writes the rendered onboarding spec under ``workdir``.

    Yields the materialized onboarding file path and an
    ``expected_table_paths`` map (fully-qualified table name →
    resolved path on disk) so tests can assert path bindings without
    re-deriving them.
    """

    def materialize(template_path: Path, *, scenario: str = "default"):
        raw = Path(template_path).read_text()
        raw = _substitute_source_paths(raw, repo_root)

        bronze_customers = str(workdir / scenario / "bronze" / "customers")
        silver_customers = str(workdir / scenario / "silver" / "customers")
        bronze_transactions = str(workdir / scenario / "bronze" / "transactions")
        silver_transactions = str(workdir / scenario / "silver" / "transactions")
        placeholders = {
            "__BRONZE_CUSTOMERS_PATH__": bronze_customers,
            "__SILVER_CUSTOMERS_PATH__": silver_customers,
            "__BRONZE_TRANSACTIONS_PATH__": bronze_transactions,
            "__SILVER_TRANSACTIONS_PATH__": silver_transactions,
        }
        for placeholder, real_path in placeholders.items():
            raw = raw.replace(placeholder, real_path)
            Path(real_path).parent.mkdir(parents=True, exist_ok=True)

        out_path = workdir / f"{scenario}_onboarding.json"
        out_path.write_text(raw)

        expected_table_paths = {
            "bronze.customers": bronze_customers,
            "bronze.transactions": bronze_transactions,
            "silver.customers": silver_customers,
            "silver.transactions": silver_transactions,
        }
        return out_path, expected_table_paths

    return materialize


# ---------------------------------------------------------------------------
# Pipeline executor + driver
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_executor_factory(spark, dp_recorder, workdir):
    """Factory for :class:`FakeOSSPipelineExecutor` instances.

    Returns a no-arg callable so tests can spin up an executor
    after they've set up their own work-directory + recorder state.
    Uses the per-test ``workdir`` for checkpoints so streaming
    writes don't leak between tests.
    """
    from integration_tests.oss._executor import FakeOSSPipelineExecutor

    def factory():
        return FakeOSSPipelineExecutor(
            spark, dp_recorder, checkpoint_dir=workdir / "_checkpoints"
        )

    return factory


@pytest.fixture
def oss_flow_driver(
    spark,
    dp_recorder,
    workdir,
    materialize_onboarding,
    fake_executor_factory,
    clean_catalog,
):
    """High-level driver: onboard → bronze → silver → row counts.

    Mirrors the orchestration in ``integration_tests/run_integration_tests.py``
    but for the OSS code path:

    1. Materialise the onboarding template into the workdir.
    2. Run :class:`OnboardDataflowspec` to write bronze + silver
       dataflowspec data to Delta paths (no UC, no metastore).
    3. Set ``<layer>.dataflowspecPath`` on the Spark conf so the
       runtime resolves the spec by path.
    4. Invoke ``DataflowPipeline.invoke_pipeline(spark, "bronze")`` —
       :class:`OSSDataflowPipeline` registers each table via the
       recorder + ``ensure_external_delta_table`` side-channel.
    5. Replay the recorded ``dp.table`` registrations against real
       Spark + Delta via :class:`FakeOSSPipelineExecutor` so the
       resulting Delta tables have real data.
    6. Repeat (4-5) for the silver layer.
    7. Return row counts per fully-qualified table name so the
       caller can assert on them.

    The return value is a dict with keys ``bronze`` (row counts for
    the bronze layer), ``silver`` (row counts for the silver layer),
    and ``expected_table_paths`` (the path each table is bound to).
    """

    def drive(template_path: Path, *, scenario: str = "default") -> dict:
        from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
        from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec

        onboarding_file, expected_table_paths = materialize_onboarding(
            template_path, scenario=scenario
        )

        # Step 1: onboarding — writes Delta-backed dataflowspec.
        meta_schema = f"sdp_meta_{scenario}"
        # Drop the bronze + silver schemas EAGERLY (not just at
        # teardown) so a previous test's ``bronze.customers``
        # registration doesn't bleed into this run and cause the
        # OSS side-channel pre-create to detect a stale-LOCATION
        # mismatch. The in-memory Hive catalog is session-scoped;
        # tmp_path isolates filesystem paths but not catalog
        # entries.
        for schema in ("bronze", "silver", meta_schema):
            try:
                spark.sql(f"DROP SCHEMA IF EXISTS `{schema}` CASCADE")
            except Exception:
                pass
        clean_catalog(meta_schema)
        clean_catalog("bronze")
        clean_catalog("silver")

        bronze_spec_path = str(workdir / scenario / "spec" / "bronze")
        silver_spec_path = str(workdir / scenario / "spec" / "silver")
        Path(bronze_spec_path).parent.mkdir(parents=True, exist_ok=True)

        onboarding_params = {
            "onboarding_file_path": str(onboarding_file),
            "database": meta_schema,
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "bronze_dataflowspec_path": bronze_spec_path,
            "silver_dataflowspec_table": "silver_dataflowspec",
            "silver_dataflowspec_path": silver_spec_path,
            "import_author": "oss-it",
            "version": "v1",
            "overwrite": "True",
        }
        OnboardDataflowspec(
            spark, onboarding_params, uc_enabled=False
        ).onboard_dataflow_specs()

        # Step 2: configure the runtime to resolve the spec by path
        # (the OSS-supported addressing — no UC/metastore needed).
        spark.conf.set("layer", "bronze_silver")
        spark.conf.set("bronze.dataflowspecPath", bronze_spec_path)
        spark.conf.set("silver.dataflowspecPath", silver_spec_path)
        spark.conf.set("bronze.group", "OSS")
        spark.conf.set("silver.group", "OSS")

        # Step 3: bronze layer — record + replay.
        dp_recorder.clear()
        DataflowPipeline.invoke_pipeline(spark, "bronze")
        bronze_executor = fake_executor_factory()
        bronze_result = bronze_executor.execute()

        # Step 4: silver layer — record + replay.
        dp_recorder.clear()
        DataflowPipeline.invoke_pipeline(spark, "silver")
        silver_executor = fake_executor_factory()
        silver_result = silver_executor.execute()

        return {
            "bronze": bronze_result,
            "silver": silver_result,
            "expected_table_paths": expected_table_paths,
            "onboarding_file": onboarding_file,
            "bronze_spec_path": bronze_spec_path,
            "silver_spec_path": silver_spec_path,
        }

    return drive
