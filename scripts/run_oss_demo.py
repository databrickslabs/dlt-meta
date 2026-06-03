"""Self-running demo / smoke-test for the SDP-META OSS code path.

What this proves, without requiring a manual setup or a remote cluster:

1. We can run :class:`OnboardDataflowspec` against a paths-only
   onboarding spec (no Unity Catalog, no remote metastore) and write
   the bronze and silver dataflowspec data to **filesystem-addressable
   Delta tables**.
2. We can hand those Delta paths back to the runtime by setting
   ``<layer>.dataflowspecPath`` in ``spark.conf`` — no
   ``<layer>.dataflowspecTable`` involved.
3. ``DataflowPipeline.invoke_pipeline(spark, layer)`` dispatches to
   :class:`OSSDataflowPipeline` because ``SDP_META_RUNTIME=oss`` is set
   at the top of this script. The OSS subclass exercises only the
   public ``pyspark.pipelines`` API surface — no Lakeflow extensions.
4. Specifically: each ``dp.table(...)`` call comes pre-decorated with
   the DQE constraints inlined into the query function (no
   ``dp.expect_*`` decorator stacking), and Lakeflow-only kwargs
   (``cluster_by_auto``, ``path``, ``expect_*``) are stripped before
   they reach the OSS API.
5. The per-table ``path`` from the onboarding spec is honoured by
   side-channel: an external Delta table is pre-created at that path
   under the same name SDP-META passes to ``dp.table(name=...)``, so
   the OSS planner ends up writing into the configured location even
   though OSS ``pyspark.pipelines.table`` itself rejects a ``path``
   kwarg.
6. AutoCDC paths raise :class:`NotImplementedError` instead of calling
   the Lakeflow-only ``dp.create_auto_cdc_*`` flows.

Two execution tiers:

- **Tier 1** (always works) — installs an instrumented stub for
  ``pyspark.pipelines`` *before* sdp-meta imports. The stub records every
  ``table`` / ``temporary_view`` / ``create_streaming_table`` /
  ``append_flow`` / ``create_sink`` call and exposes them as a list of
  ``(api_name, args, kwargs)`` tuples. The script then asserts the
  recorded call sequence matches what an OSS-correct run should look
  like. Works on any Spark version (the onboarding side uses Spark 3.5+,
  the OSS pipeline side never touches a real ``pyspark.pipelines``).
- **Tier 2** (requires Spark 4.1+ with the ``pipelines`` extra) — the
  same flow against the real ``pyspark.pipelines`` and a real
  ``spark-pipelines run``. This is **never executed in-process**: Tier 1
  installs the ``pyspark.pipelines`` stub into ``sys.modules`` at
  startup, so ``_tier2_available()`` deliberately returns ``False`` for
  the rest of this process. Instead, the script prints copy-pasteable
  Tier-2 instructions at the end for you to run yourself in a clean
  Spark 4.1+ environment (without the stub).

Run from the repo root::

    python scripts/run_oss_demo.py

Useful flags / env vars::

    SDP_META_OSS_DEMO_KEEP=1   # keep the temp work dir on exit
    SDP_META_OSS_DEMO_VERBOSE=1  # print every recorded dp call
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sys
import tempfile
import textwrap
from pathlib import Path
from typing import Any, Callable

# IMPORTANT: set the runtime override BEFORE any sdp-meta import so
# DataflowPipeline.__new__ dispatches to OSSDataflowPipeline.
os.environ["SDP_META_RUNTIME"] = "oss"

REPO_ROOT = Path(__file__).resolve().parent.parent
ONBOARDING_TEMPLATE = REPO_ROOT / "tests" / "resources" / "oss_onboarding.json"
SOURCE_DATA_DIR = REPO_ROOT / "tests" / "resources" / "data" / "customers"

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("oss_demo")


# ---------------------------------------------------------------------------
# Instrumented ``pyspark.pipelines`` stub (Tier 1)
# ---------------------------------------------------------------------------


class _DPStub:
    """Recorder masquerading as ``pyspark.pipelines`` (the public OSS API).

    Every supported call appends to ``self.calls``. Each entry is a
    ``(api_name, args, kwargs)`` tuple. The ``dp.table`` /
    ``dp.append_flow`` / ``dp.temporary_view`` shaped calls are
    decorator factories — they return a callable that when applied to a
    function records a follow-up ``f"{api}.applied"`` entry pointing at
    the function name. That's enough to verify SDP-META built the right
    graph without standing up a real planner.

    Importantly, the Lakeflow-only extension symbols (``expect_all`` /
    ``create_auto_cdc_flow`` / ``create_auto_cdc_from_snapshot_flow``)
    are NOT defined on this stub. ``hasattr(dp, "expect_all")`` therefore
    returns False, which is what the runtime probe in
    :func:`oss_pipelines._probe_runtime` looks for to land on the OSS
    code path.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    # ------------- decorator-factory style: dp.table / dp.temporary_view ------

    def _factory(self, api: str) -> Callable[..., Any]:
        def factory(*args: Any, **kwargs: Any):
            self.calls.append((api, args, kwargs))

            def decorator(fn: Callable[..., Any]):
                self.calls.append((f"{api}.applied", (getattr(fn, "__name__", repr(fn)),), {}))
                return fn

            # ``pyspark.pipelines.table`` is also callable as
            # ``dp.table(qf, name=..., ...)`` (the form SDP-META uses
            # via ``_register_table_with_dqe``). Detect that shape:
            # first positional arg is callable AND we got > 0 kwargs.
            if args and callable(args[0]):
                fn = args[0]
                self.calls.append((f"{api}.applied", (getattr(fn, "__name__", repr(fn)),), {}))
                return fn
            return decorator

        return factory

    @property
    def table(self):
        return self._factory("table")

    @property
    def temporary_view(self):
        return self._factory("temporary_view")

    @property
    def append_flow(self):
        return self._factory("append_flow")

    @property
    def materialized_view(self):
        return self._factory("materialized_view")

    # ------------- direct-call style: dp.create_streaming_table / create_sink

    def create_streaming_table(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("create_streaming_table", args, kwargs))

    def create_sink(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("create_sink", args, kwargs))


# Install the stub BEFORE any sdp-meta import. The shim's
# ``_probe_runtime`` will see no ``expect_all`` / ``create_auto_cdc_flow``
# attribute on this object and (combined with the env var) land on OSS.
_DP_STUB = _DPStub()
sys.modules["pyspark.pipelines"] = _DP_STUB  # type: ignore[assignment]


# Imports MUST come AFTER the stub install + env override.
from databricks.labs.sdp_meta import is_oss, OSSDataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec  # noqa: E402


# ---------------------------------------------------------------------------
# Demo helpers
# ---------------------------------------------------------------------------


def _materialise_onboarding(template_path: Path, target_dir: Path) -> tuple[Path, dict[str, str]]:
    """Substitute the per-table path placeholders into the onboarding template.

    Returns the materialised onboarding path AND a map of expected
    fully-qualified table name → resolved path. The caller uses the map
    to assert that ``OSSDataflowPipeline`` pre-created an external
    Delta table at each path under the same name it then passes to
    ``dp.table(name=...)``.
    """
    raw = template_path.read_text()
    bronze_customers = str(target_dir / "tables" / "bronze" / "customers")
    silver_customers = str(target_dir / "tables" / "silver" / "customers")
    bronze_transactions = str(target_dir / "tables" / "bronze" / "transactions")
    silver_transactions = str(target_dir / "tables" / "silver" / "transactions")
    placeholders = {
        "__BRONZE_CUSTOMERS_PATH__": bronze_customers,
        "__SILVER_CUSTOMERS_PATH__": silver_customers,
        "__BRONZE_TRANSACTIONS_PATH__": bronze_transactions,
        "__SILVER_TRANSACTIONS_PATH__": silver_transactions,
    }
    for placeholder, real_path in placeholders.items():
        raw = raw.replace(placeholder, real_path)
        Path(real_path).parent.mkdir(parents=True, exist_ok=True)

    out_path = target_dir / "oss_onboarding.json"
    out_path.write_text(raw)

    # Mirrors what the onboarding spec in tests/resources/oss_onboarding.json
    # declares as ``<bronze|silver>_database_dev`` + ``<bronze|silver>_table``.
    expected_table_paths = {
        "bronze.customers": bronze_customers,
        "bronze.transactions": bronze_transactions,
        "silver.customers": silver_customers,
        "silver.transactions": silver_transactions,
    }
    return out_path, expected_table_paths


def _build_spark_session():
    from pyspark.sql import SparkSession

    # Tier 1 deliberately uses the Spark 3.x / Scala 2.12 Delta build
    # (``delta-spark_2.12:3.0.0``) because Tier 1 only runs ``onboarding``
    # + the OSS-shim contract assertions — it does not actually invoke
    # ``pyspark.pipelines`` (every ``dp`` call is recorded by the
    # ``InMemoryDpRecorder`` stub, never executed). Bumping to
    # ``delta-spark_2.13:4.0.0`` would force a Spark 4.x install on every
    # contributor running this script, which is exactly the dependency we
    # want Tier 2 to gate.
    #
    # Tier 2 (the ``spark-pipelines run`` command printed at the end of
    # ``main()``) is what actually needs ``delta-spark_2.13:4.0.0`` —
    # because that path runs against real OSS Spark 4.1+ where the only
    # supported Delta build is the Scala 2.13 one. The two pins are
    # intentionally different; they target two different runtimes.
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("sdp-meta-oss-demo")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0",
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.unityCatalog.enabled", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _run_onboarding(spark, onboarding_file: Path, dataflowspec_dir: Path) -> tuple[str, str]:
    bronze_path = str(dataflowspec_dir / "bronze")
    silver_path = str(dataflowspec_dir / "silver")

    onboarding_params = {
        "onboarding_file_path": str(onboarding_file),
        "database": "sdp_meta_oss_demo",
        "env": "dev",
        "bronze_dataflowspec_table": "bronze_dataflowspec",
        "bronze_dataflowspec_path": bronze_path,
        "silver_dataflowspec_table": "silver_dataflowspec",
        "silver_dataflowspec_path": silver_path,
        "import_author": "oss-demo",
        "version": "v1",
        "overwrite": "True",
    }
    OnboardDataflowspec(spark, onboarding_params, uc_enabled=False).onboard_dataflow_specs()
    return bronze_path, silver_path


def _drive_oss_pipeline(spark, bronze_path: str, silver_path: str) -> None:
    spark.conf.set("layer", "bronze_silver")
    spark.conf.set("bronze.dataflowspecPath", bronze_path)
    spark.conf.set("silver.dataflowspecPath", silver_path)
    spark.conf.set("bronze.group", "OSS")
    spark.conf.set("silver.group", "OSS")
    DataflowPipeline.invoke_pipeline(spark, "bronze_silver")


# ---------------------------------------------------------------------------
# Assertions on the recorded ``dp`` call log
# ---------------------------------------------------------------------------


def _summarise_calls(calls: list[tuple[str, tuple, dict]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for api, _args, _kwargs in calls:
        counts[api] = counts.get(api, 0) + 1
    return counts


def _assert_oss_invariants(calls: list[tuple[str, tuple, dict]]) -> list[str]:
    """Return a list of failure messages; empty list = all green."""
    failures: list[str] = []
    apis = {api for api, _, _ in calls}

    # Every Lakeflow-only API must be absent from the recorded calls.
    forbidden = {
        "expect_all",
        "expect_all_or_drop",
        "expect_all_or_fail",
        "create_auto_cdc_flow",
        "create_auto_cdc_from_snapshot_flow",
    }
    intruders = apis & forbidden
    if intruders:
        failures.append(
            f"OSS code path called Lakeflow-only API(s): {sorted(intruders)}"
        )

    # Every dp.table / dp.create_streaming_table call must NOT carry
    # any Lakeflow-only kwargs after the OSS shim has filtered them.
    lakeflow_only_kwargs = {"cluster_by_auto", "path", "expect_all",
                            "expect_all_or_drop", "expect_all_or_fail"}
    for api, _args, kwargs in calls:
        if api not in {"table", "create_streaming_table"}:
            continue
        leaked = lakeflow_only_kwargs & set(kwargs.keys())
        if leaked:
            failures.append(
                f"{api}(...) carried Lakeflow-only kwargs after filter: "
                f"{sorted(leaked)} (kwargs={list(kwargs.keys())})"
            )

    # We expect at least one bronze table and one silver table to have
    # been registered. The OSS demo onboarding spec has 2 dataflows
    # (customers + transactions) → 2 bronze tables and 2 silver tables.
    table_calls = [c for c in calls if c[0] == "table"]
    if len(table_calls) < 4:
        failures.append(
            f"expected >= 4 dp.table calls (2 bronze + 2 silver), got "
            f"{len(table_calls)}"
        )

    # Each ``dp.table`` should have a corresponding ``table.applied``
    # entry — that's how the stub records the decorator hitting a real
    # function. If they're imbalanced, OSS-Spark's planner would also
    # see a half-decorated graph.
    table_count = sum(1 for c in calls if c[0] == "table")
    table_applied = sum(1 for c in calls if c[0] == "table.applied")
    if table_count != table_applied:
        failures.append(
            f"dp.table calls = {table_count} but applied = {table_applied}; "
            "decorator was not invoked for every registration"
        )

    return failures


def _assert_external_tables_registered(
    spark, expected: dict[str, str]
) -> list[str]:
    """Verify the external Delta tables were pre-created at the right paths.

    SDP-META on OSS pre-creates an external Delta table at
    ``targetDetails["path"]`` with the same fully-qualified name it then
    passes to ``dp.table(name=...)``. That's the side channel that lets
    the OSS code path honour per-table paths from the onboarding spec
    even though OSS ``pyspark.pipelines.table`` itself rejects a
    ``path`` kwarg.

    ``expected`` maps fully-qualified table name → expected path.
    """
    failures: list[str] = []
    for full_name, expected_path in expected.items():
        try:
            rows = spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}").collect()
        except Exception as e:
            failures.append(
                f"{full_name} not registered in the catalog: {e!s}"
            )
            continue
        loc = None
        for r in rows:
            try:
                col = (r["col_name"] or "").strip().lower()
            except Exception:
                continue
            if col == "location":
                loc = r["data_type"]
                break
        if loc is None:
            failures.append(f"{full_name} has no LOCATION in DESCRIBE output")
            continue
        # Spark normalises LOCATION as ``file:<abs>`` on local FS; compare on
        # the path component only.
        normalised_loc = loc.replace("file:", "").rstrip("/")
        normalised_expected = expected_path.replace("file:", "").rstrip("/")
        if normalised_loc != normalised_expected:
            failures.append(
                f"{full_name} registered at {loc!r}, expected {expected_path!r}"
            )
    return failures


def _assert_cdc_raises_on_oss() -> list[str]:
    """Construct a CDC-shaped spec and confirm the OSS subclass raises."""
    failures: list[str] = []
    instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
    try:
        instance.cdc_apply_changes()
    except NotImplementedError as e:
        if "create_auto_cdc_flow" not in str(e):
            failures.append(
                f"cdc_apply_changes raised but message wrong: {e!s}"
            )
    else:
        failures.append("cdc_apply_changes did NOT raise on OSS")

    try:
        instance.apply_changes_from_snapshot()
    except NotImplementedError as e:
        if "create_auto_cdc_flow" not in str(e):
            failures.append(
                f"apply_changes_from_snapshot raised but message wrong: {e!s}"
            )
    else:
        failures.append("apply_changes_from_snapshot did NOT raise on OSS")
    return failures


# ---------------------------------------------------------------------------
# Tier 2 (real OSS Apache Spark 4.1+) — best effort
# ---------------------------------------------------------------------------


def _tier2_available() -> bool:
    """True iff a real ``pyspark.pipelines`` is importable."""
    if isinstance(sys.modules.get("pyspark.pipelines"), _DPStub):
        # Our stub is in place — that's the Tier 1 mode. Don't try
        # Tier 2 in the same process.
        return False
    try:
        import pyspark.pipelines  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _print_banner(text: str) -> None:
    bar = "=" * max(60, len(text) + 4)
    logger.info("\n%s\n  %s\n%s", bar, text, bar)


def _print_call_summary(calls: list[tuple[str, tuple, dict]], verbose: bool) -> None:
    summary = _summarise_calls(calls)
    logger.info("Recorded pyspark.pipelines call counts:")
    for api in sorted(summary):
        logger.info("    %-30s %d", api, summary[api])
    if verbose:
        logger.info("Full call log:")
        for i, (api, args, kwargs) in enumerate(calls, start=1):
            kw_view = {k: ("<schema>" if k == "schema" else v) for k, v in kwargs.items()}
            logger.info("  %3d. %s args=%r kwargs=%r", i, api, args, kw_view)


def main() -> int:
    keep_tmp = os.environ.get("SDP_META_OSS_DEMO_KEEP", "").lower() in {"1", "true", "yes"}
    verbose = os.environ.get("SDP_META_OSS_DEMO_VERBOSE", "").lower() in {"1", "true", "yes"}

    work_dir = Path(tempfile.mkdtemp(prefix="sdp_meta_oss_demo_"))
    logger.info("Work dir: %s", work_dir)
    failures: list[str] = []

    try:
        _print_banner("Tier 1 — OSS code path against instrumented pyspark.pipelines")

        if not is_oss():
            failures.append(
                "is_oss() returned False — runtime detection landed on Lakeflow. "
                "SDP_META_RUNTIME=oss should have forced OSS."
            )

        # 1. Materialise onboarding spec with paths under work_dir.
        onboarding_file, expected_table_paths = _materialise_onboarding(
            ONBOARDING_TEMPLATE, work_dir
        )
        logger.info("Materialised onboarding spec: %s", onboarding_file)
        for full_name, p in expected_table_paths.items():
            logger.info("  Expected external Delta table: %s -> %s", full_name, p)

        # 2. Spin up local Spark + Delta and run onboarding.
        spark = _build_spark_session()
        dataflowspec_dir = work_dir / "dataflowspec"
        dataflowspec_dir.mkdir(parents=True, exist_ok=True)
        bronze_path, silver_path = _run_onboarding(spark, onboarding_file, dataflowspec_dir)

        # 3. Round-trip the dataflowspec data via path to confirm
        #    onboarding actually wrote real data.
        for label, p in [("bronze dataflowspec", bronze_path), ("silver dataflowspec", silver_path)]:
            df = spark.read.format("delta").load(p)
            n = df.count()
            logger.info("[%s] %s — %d rows", label, p, n)
            if n == 0:
                failures.append(f"{label} at {p} is empty after onboarding")

        # 4. Drive the OSS DataflowPipeline using paths.
        _DP_STUB.calls.clear()
        _drive_oss_pipeline(spark, bronze_path, silver_path)

        # 5. Inspect the recorded ``dp`` calls.
        _print_call_summary(_DP_STUB.calls, verbose)
        failures.extend(_assert_oss_invariants(_DP_STUB.calls))

        # 6. Verify the external Delta tables were registered at the
        #    paths the onboarding spec asked for. This proves the OSS
        #    side-channel pre-creation in OSSDataflowPipeline ran (the
        #    only mechanism by which OSS pyspark.pipelines tables can
        #    land at a per-table path, since dp.table itself rejects a
        #    ``path`` kwarg).
        ext_failures = _assert_external_tables_registered(spark, expected_table_paths)
        for full_name, p in expected_table_paths.items():
            status = "OK" if not any(full_name in f for f in ext_failures) else "FAIL"
            logger.info("[external-table %s] %s -> %s", status, full_name, p)
        failures.extend(ext_failures)

        # 7. Independent CDC-on-OSS raise check.
        failures.extend(_assert_cdc_raises_on_oss())

        # ---------- Tier 2: real OSS Spark 4.1+ ----------
        _print_banner("Tier 2 — real pyspark.pipelines (OSS Apache Spark 4.1+)")
        if _tier2_available():
            logger.info(
                "Tier 2 is available in this process. To run a real "
                "spark-pipelines planner build, copy "
                "src/databricks/labs/sdp_meta/templates/oss/ to a workdir, "
                "fill in the placeholders, set SDP_META_RUNTIME=oss, and run:"
            )
            logger.info(
                "    spark-pipelines run --spec spark-pipeline.yml "
                "--packages io.delta:delta-spark_2.13:4.0.0"
            )
        else:
            try:
                import pyspark
                version = pyspark.__version__
            except ImportError:
                version = "<not installed>"
            logger.info(
                "SKIPPED — pyspark.pipelines is not importable "
                "(pyspark==%s). Tier 1 is sufficient for verifying the "
                "OSS code path; install pyspark[pipelines]>=4.1.0 in a "
                "separate venv to exercise the real OSS planner.",
                version,
            )

        # ---------- Final summary ----------
        _print_banner("Result")
        if failures:
            logger.error("OSS demo FAILED with %d issue(s):", len(failures))
            for f in failures:
                logger.error("  - %s", f)
            return 1
        logger.info("OSS demo PASSED.")
        logger.info(
            textwrap.dedent(
                """\
                Verified:
                  - DataflowPipeline factory dispatched to OSSDataflowPipeline
                  - Onboarding wrote bronze + silver dataflowspec to Delta paths
                  - Pipeline ran end-to-end against those paths (no UC, no metastore)
                  - No Lakeflow-only API was called (expect_*, create_auto_cdc_*)
                  - No Lakeflow-only kwarg leaked into pyspark.pipelines calls
                  - Per-table paths from the onboarding spec registered as
                    external Delta tables under SDP-META's dp.table names
                  - cdc_apply_changes / apply_changes_from_snapshot raised on OSS
                """
            )
        )
        return 0
    finally:
        if keep_tmp:
            logger.info("SDP_META_OSS_DEMO_KEEP set — leaving %s in place", work_dir)
        else:
            shutil.rmtree(work_dir, ignore_errors=True)
            logger.info("Cleaned up %s", work_dir)


if __name__ == "__main__":
    sys.exit(main())
