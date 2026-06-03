"""OSS Spark JSON-source end-to-end flow integration test.

Mirrors the shape of ``run_integration_tests.py --source=cloudfiles`` —
onboarding, then bronze pipeline, then silver pipeline, then row count
validation — but for the OSS Apache Spark code path. The OSS pipeline
runs locally against ``json`` vanilla file source (no ``cloudFiles``;
that's a Lakeflow extension), exercises
:class:`OSSDataflowPipeline._register_table_with_dqe`
+ :func:`ensure_external_delta_table` + :func:`filter_table_kwargs`,
and the resulting bronze + silver Delta tables are SELECT COUNT(*)'d to
prove every layer wrote real data into the side-channelled per-table
paths from the onboarding spec.

This is the canonical OSS flow test. The CSV / Delta variants build on
the same fixtures and assertion shape.
"""
from __future__ import annotations

from pathlib import Path

import pytest


def test_oss_json_source_full_pipeline_flow(
    spark,
    oss_runtime_env,
    repo_root: Path,
    oss_flow_driver,
):
    """End-to-end: onboard → bronze → silver → validate row counts.

    Source: 2 dataflows declared in ``tests/resources/oss_onboarding.json``
    — customers and transactions, both reading from
    ``tests/resources/data/<table>/*.json`` with explicit DDL schemas.

    Asserts:
      - Onboarding writes the bronze + silver dataflowspec to Delta
        at filesystem paths (no UC, no metastore — proves the
        ``<layer>.dataflowspecPath`` addressing works end-to-end on OSS).
      - The OSS code path registers both customers and transactions
        tables in both layers (2 bronze + 2 silver = 4 tables total).
      - Each table is bound to the per-table path from the onboarding
        spec via the ``ensure_external_delta_table`` side-channel.
      - SELECT COUNT(*) on every resulting Delta table returns >0 rows,
        proving the recorded query function was correctly wired and
        the write landed at the configured location.
      - Bronze tables hold the full source row count; silver tables
        hold the post-transformation row count (silver applies the
        ``where_clause`` filters from ``silver_transformations.json``,
        so silver ≤ bronze).
    """
    template = repo_root / "tests" / "resources" / "oss_onboarding.json"
    if not template.exists():
        pytest.skip(f"missing OSS onboarding template at {template}")

    result = oss_flow_driver(template, scenario="json")

    bronze = result["bronze"]
    silver = result["silver"]

    # 4 tables total: 2 bronze + 2 silver. Per-table failures are
    # captured in ``failures`` (not re-raised) so we report all of
    # them in one diagnostic instead of stopping at the first.
    all_failures = bronze.failures + silver.failures
    assert not all_failures, (
        "OSS pipeline replay had per-table failures:\n  "
        + "\n  ".join(f"{name}: {exc}" for name, exc in all_failures)
    )

    bronze_counts = bronze.row_counts
    silver_counts = silver.row_counts

    # Bronze layer: both source dataflows registered + populated.
    assert set(bronze_counts.keys()) == {"bronze.customers", "bronze.transactions"}, (
        f"bronze layer registered unexpected tables: {sorted(bronze_counts)}"
    )
    for name, count in bronze_counts.items():
        assert count > 0, f"{name} is empty after bronze pipeline replay (count={count})"

    # Silver layer: both targets registered + populated.
    assert set(silver_counts.keys()) == {"silver.customers", "silver.transactions"}, (
        f"silver layer registered unexpected tables: {sorted(silver_counts)}"
    )
    for name, count in silver_counts.items():
        assert count > 0, f"{name} is empty after silver pipeline replay (count={count})"

    # Silver tables apply WHERE filters from silver_transformations.json,
    # so silver row count must be ≤ bronze row count for the same logical
    # table. A silver count > bronze count means the silver query is
    # joining or duplicating instead of filtering — a regression worth
    # surfacing here even though the spec doesn't pin exact counts.
    assert silver_counts["silver.customers"] <= bronze_counts["bronze.customers"], (
        f"silver.customers ({silver_counts['silver.customers']}) > "
        f"bronze.customers ({bronze_counts['bronze.customers']}) — silver "
        "should only filter / project, not duplicate rows"
    )
    assert silver_counts["silver.transactions"] <= bronze_counts["bronze.transactions"], (
        f"silver.transactions ({silver_counts['silver.transactions']}) > "
        f"bronze.transactions ({bronze_counts['bronze.transactions']}) — "
        "silver should only filter / project, not duplicate rows"
    )


def test_oss_external_tables_bound_to_onboarding_paths(
    spark,
    oss_runtime_env,
    repo_root: Path,
    oss_flow_driver,
):
    """Every registered Delta table is bound to its onboarding-spec path.

    Pins the side-channel contract: OSS ``pyspark.pipelines.table``
    rejects a ``path`` kwarg, so SDP-META pre-creates an external Delta
    table at the per-table path from the onboarding spec under the same
    name it then passes to ``dp.table(name=...)``. The subsequent write
    lands at the configured location.

    Without this contract, OSS would silently write tables to the
    Hive-warehouse default location and the per-table paths in the
    onboarding spec would be ignored — a data-locality regression that
    wouldn't surface from row-count assertions alone.
    """
    template = repo_root / "tests" / "resources" / "oss_onboarding.json"
    if not template.exists():
        pytest.skip(f"missing OSS onboarding template at {template}")

    result = oss_flow_driver(template, scenario="bind")
    expected = result["expected_table_paths"]

    # ``DESCRIBE TABLE EXTENDED`` reports LOCATION as a URI
    # (``file:/abs`` or ``file:///abs``) depending on Spark version;
    # ``urlparse`` normalises both to the path component cleanly,
    # whereas ``replace("file:", "")`` would leave leading slashes on
    # ``file:///``.
    from urllib.parse import urlparse

    def _location(name: str) -> str:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
        for r in rows:
            try:
                col = (r["col_name"] or "").strip().lower()
            except Exception:
                continue
            if col == "location":
                raw = r["data_type"] or ""
                parsed = urlparse(raw)
                path = parsed.path if parsed.scheme in ("", "file") else raw
                return path.rstrip("/")
        pytest.fail(f"{name} has no LOCATION in DESCRIBE EXTENDED output")
        return ""  # unreachable

    for name, expected_path in expected.items():
        actual = _location(name)
        normalised_expected = expected_path.rstrip("/")
        assert actual == normalised_expected, (
            f"{name} bound to LOCATION={actual!r}, expected {normalised_expected!r} "
            "(onboarding-spec path was not honored by the OSS side-channel)"
        )
