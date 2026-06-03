"""OSS Spark CSV-source end-to-end flow integration test.

Same shape as :mod:`test_oss_json_flow` but with ``source_format='csv'``
to exercise the vanilla file-source dispatcher's CSV branch.

This is the second proof-point for the OSS source-format generalization:
the same SDP-META code path that handles ``json`` should handle every
member of ``VANILLA_FILE_SOURCE_FORMATS`` (``json``, ``csv``,
``parquet``, ``orc``, ``text``, ``avro``) with no special-casing. If
this test starts failing while ``test_oss_json_flow`` keeps passing,
the regression is in the CSV-specific reader-options handling — most
likely the ``header`` / ``schema`` interplay in ``read_dlt_file_source``.
"""
from __future__ import annotations

from pathlib import Path

from integration_tests.oss._fixtures import write_onboarding_template


def test_oss_csv_source_full_pipeline_flow(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    oss_flow_driver,
):
    """End-to-end CSV: onboard → bronze → silver → validate row counts.

    Materialises the canonical JSON test data as CSV under
    ``workdir/_sources/csv/<table>/``, generates a CSV-flavoured
    onboarding template, and drives the OSS flow with it.

    Asserts the same invariants as the JSON flow test: all four tables
    register, every table holds a positive row count, silver row
    counts ≤ bronze row counts (silver only filters / projects).
    """
    template = write_onboarding_template(
        workdir, fmt="csv", repo_root=repo_root, spark=spark
    )
    result = oss_flow_driver(template, scenario="csv")

    all_failures = result["bronze"].failures + result["silver"].failures
    assert not all_failures, (
        "OSS CSV pipeline replay had per-table failures:\n  "
        + "\n  ".join(f"{name}: {exc}" for name, exc in all_failures)
    )

    bronze_counts = result["bronze"].row_counts
    silver_counts = result["silver"].row_counts

    assert set(bronze_counts) == {"bronze.customers", "bronze.transactions"}
    assert set(silver_counts) == {"silver.customers", "silver.transactions"}
    for name, count in {**bronze_counts, **silver_counts}.items():
        assert count > 0, f"{name} is empty after CSV pipeline replay (count={count})"
    assert silver_counts["silver.customers"] <= bronze_counts["bronze.customers"]
    assert silver_counts["silver.transactions"] <= bronze_counts["bronze.transactions"]


def test_oss_csv_source_format_routes_through_file_source_reader(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    dp_recorder,
    materialize_onboarding,
):
    """``source_format='csv'`` exercises the OSS-supported file-source path.

    Pins the source-format dispatch contract: ``csv`` must NOT route
    through the Auto Loader (``cloudFiles``) reader, which is
    Lakeflow-only. The onboarding-time validation must accept ``csv``
    AND the bronze pipeline must register both customers + transactions
    as ``dp.table`` calls.

    Critically, this test re-asserts after the JSON test that the
    ``filter_table_kwargs`` warning-dedup state is NOT silently
    masking a missing kwarg-warning regression (the conftest's
    ``dp_recorder`` fixture clears the dedup set per test).
    """
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec

    template = write_onboarding_template(
        workdir, fmt="csv", repo_root=repo_root, spark=spark
    )
    onboarding_file, _ = materialize_onboarding(template, scenario="csv_dispatch")

    bronze_spec = str(workdir / "csv_dispatch" / "spec" / "bronze")
    silver_spec = str(workdir / "csv_dispatch" / "spec" / "silver")
    Path(bronze_spec).parent.mkdir(parents=True, exist_ok=True)

    OnboardDataflowspec(
        spark,
        {
            "onboarding_file_path": str(onboarding_file),
            "database": "sdp_meta_csv_dispatch",
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "bronze_dataflowspec_path": bronze_spec,
            "silver_dataflowspec_table": "silver_dataflowspec",
            "silver_dataflowspec_path": silver_spec,
            "import_author": "oss-csv-it",
            "version": "v1",
            "overwrite": "True",
        },
        uc_enabled=False,
    ).onboard_dataflow_specs()

    spark.conf.set("layer", "bronze")
    spark.conf.set("bronze.dataflowspecPath", bronze_spec)
    spark.conf.set("bronze.group", "OSS")

    dp_recorder.clear()
    DataflowPipeline.invoke_pipeline(spark, "bronze")

    # The bronze layer should record 2 dp.table calls (customers +
    # transactions), each carrying name="bronze.<table>". Auto Loader-
    # specific kwargs MUST NOT appear because ``csv`` is in the
    # vanilla file-source dispatch branch, not the cloudFiles branch.
    table_calls = [c for c in dp_recorder.calls if c[0] == "table"]
    assert len(table_calls) == 2, (
        f"expected 2 bronze.table registrations for csv (one per dataflow), "
        f"got {len(table_calls)}: "
        f"{[(c[2].get('name'), sorted(c[2])) for c in table_calls]}"
    )

    # Every recorded dp.table kwargs dict MUST NOT carry the
    # Lakeflow-only ``path`` / ``cluster_by_auto`` (filtered out by
    # ``filter_table_kwargs``), and MUST carry ``name``.
    for api, _args, kwargs in table_calls:
        assert "name" in kwargs, f"{api} call missing name kwarg: {kwargs}"
        assert "path" not in kwargs, (
            f"{api}({kwargs.get('name')}) leaked Lakeflow-only 'path' kwarg — "
            "filter_table_kwargs should have stripped it on the OSS code path"
        )
        assert "cluster_by_auto" not in kwargs, (
            f"{api}({kwargs.get('name')}) leaked 'cluster_by_auto' kwarg"
        )
