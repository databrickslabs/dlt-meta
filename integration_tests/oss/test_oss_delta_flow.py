"""OSS Spark Delta-source end-to-end flow integration test.

Same shape as the JSON / CSV flow tests, but with
``source_format='delta'`` and the source data pre-materialised as Delta.
This exercises the other major OSS reader branch in SDP-META —
``PipelineReaders.read_dlt_delta`` — which is independent of the
vanilla file-source dispatcher.

Delta is the source format the silver layer always uses (silver reads
from bronze Delta tables), so this test is also the layered-runtime
proof that bronze-as-Delta-source works the same regardless of whether
the underlying data was originally ingested as JSON / CSV / Parquet /
external Delta. The shape SDP-META calls ``dp.table`` with on the OSS
path is identical across these dispatch branches; only the bronze
reader differs.
"""
from __future__ import annotations

import json
from pathlib import Path

from integration_tests.oss._fixtures import write_source_data_in_format


def _register_delta_source_tables(spark, paths: dict[str, str]) -> None:
    """Register the materialised Delta data as ``delta_src.<table>`` entries.

    SDP-META's :meth:`PipelineReaders.read_dlt_delta` resolves the
    source via ``<source_catalog>.<source_database>.<source_table>``
    catalog lookup — it does NOT honor ``source_path_dev`` for the
    Delta source format. To make the materialised-on-disk Delta land
    where the reader expects, register it as an external Delta table
    under the ``delta_src`` schema referenced in the onboarding spec.
    """
    spark.sql("DROP SCHEMA IF EXISTS `delta_src` CASCADE")
    spark.sql("CREATE SCHEMA IF NOT EXISTS `delta_src`")
    for table, path in paths.items():
        spark.sql(
            f"CREATE TABLE `delta_src`.`{table}` "
            f"USING DELTA LOCATION '{path}'"
        )


def _write_delta_compatible_silver_transformations(out: Path) -> Path:
    """Write a silver_transformations.json sans ``_rescued_data``.

    The committed ``tests/resources/silver_transformations.json``
    includes ``_rescued_data`` in the silver select expressions —
    that column is added by Spark's JSON reader for malformed
    records and is therefore present on bronze tables backed by a
    JSON source. The Delta-source flow goes through
    :meth:`PipelineReaders.read_dlt_delta`, which preserves the
    upstream Delta schema verbatim; the materialised test Delta
    fixtures don't include ``_rescued_data``, so the silver
    select fails with ``UNRESOLVED_COLUMN`` on that name.

    Write a silver-transformation variant without ``_rescued_data``
    next to the test workdir and return its path so the test's
    Delta onboarding template can reference it.
    """
    transformations = [
        {
            "data_flow_id": "200_delta",
            "target_table": "customers",
            "select_exp": [
                "address",
                "email",
                "firstname",
                "id",
                "lastname",
                "operation_date",
                "operation",
            ],
            "where_clause": ["id IS NOT NULL", "email is not NULL"],
        },
        {
            "data_flow_id": "201_delta",
            "target_table": "transactions",
            "select_exp": [
                "id",
                "customer_id",
                "amount",
                "item_count",
                "operation_date",
                "operation",
            ],
            "where_clause": ["id IS NOT NULL", "amount is not NULL"],
        },
    ]
    out.write_text(json.dumps(transformations, indent=2))
    return out


def _render_delta_onboarding(
    customers_path: str,
    transactions_path: str,
    silver_transformation_path: str,
) -> str:
    """Render an onboarding template with ``source_format='delta'``.

    Delta sources don't carry a ``source_schema_path`` — the Delta
    log itself is the schema authority. The reader resolves the
    source via ``source_details['path']``; ``source_database`` /
    ``source_table`` are ignored on the path-based code path but
    we set them to non-null sentinels because the dataclass parser
    treats null values as an explicit "no metadata" signal.
    """
    rows = [
        {
            "data_flow_id": "200_delta",
            "data_flow_group": "OSS",
            "source_system": "DELTA",
            "source_format": "delta",
            "source_details": {
                "source_database": "delta_src",
                "source_table": "customers",
                "source_path_dev": customers_path,
            },
            "bronze_database_dev": "bronze",
            "bronze_table": "customers",
            "bronze_table_comment": "Bronze customers (delta source)",
            "bronze_reader_options": {},
            "bronze_table_path_dev": "__BRONZE_CUSTOMERS_PATH__",
            "bronze_table_properties": {"pipelines.reset.allowed": "false"},
            "bronze_quarantine_table": None,
            "silver_database_dev": "silver",
            "silver_table": "customers",
            "silver_table_comment": "Silver customers (delta source)",
            "silver_table_path_dev": "__SILVER_CUSTOMERS_PATH__",
            "silver_table_properties": {"pipelines.reset.allowed": "false"},
            "silver_transformation_json_dev": silver_transformation_path,
            "silver_quarantine_table": None,
        },
        {
            "data_flow_id": "201_delta",
            "data_flow_group": "OSS",
            "source_system": "DELTA",
            "source_format": "delta",
            "source_details": {
                "source_database": "delta_src",
                "source_table": "transactions",
                "source_path_dev": transactions_path,
            },
            "bronze_database_dev": "bronze",
            "bronze_table": "transactions",
            "bronze_table_comment": "Bronze transactions (delta source)",
            "bronze_reader_options": {},
            "bronze_table_path_dev": "__BRONZE_TRANSACTIONS_PATH__",
            "bronze_table_properties": {"pipelines.reset.allowed": "false"},
            "bronze_quarantine_table": None,
            "silver_database_dev": "silver",
            "silver_table": "transactions",
            "silver_table_comment": "Silver transactions (delta source)",
            "silver_table_path_dev": "__SILVER_TRANSACTIONS_PATH__",
            "silver_table_properties": {"pipelines.reset.allowed": "false"},
            "silver_transformation_json_dev": silver_transformation_path,
            "silver_quarantine_table": None,
        },
    ]
    return json.dumps(rows, indent=2)


def test_oss_delta_source_full_pipeline_flow(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    oss_flow_driver,
):
    """End-to-end Delta source: onboard → bronze → silver → row counts.

    Pre-materialises the canonical JSON test data as Delta under
    ``workdir/_sources/delta/<table>/``, then drives the OSS flow.
    Asserts the same invariants as the JSON/CSV flow tests.
    """
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

    result = oss_flow_driver(template_path, scenario="delta")

    all_failures = result["bronze"].failures + result["silver"].failures
    assert not all_failures, (
        "OSS Delta-source pipeline replay had per-table failures:\n  "
        + "\n  ".join(f"{name}: {exc}" for name, exc in all_failures)
    )

    bronze_counts = result["bronze"].row_counts
    silver_counts = result["silver"].row_counts

    assert set(bronze_counts) == {"bronze.customers", "bronze.transactions"}
    assert set(silver_counts) == {"silver.customers", "silver.transactions"}
    for name, count in {**bronze_counts, **silver_counts}.items():
        assert count > 0, f"{name} is empty after Delta-source replay (count={count})"
    assert silver_counts["silver.customers"] <= bronze_counts["bronze.customers"]
    assert silver_counts["silver.transactions"] <= bronze_counts["bronze.transactions"]


def test_oss_delta_bronze_row_count_matches_source(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    oss_flow_driver,
):
    """Bronze Delta-source flow is a no-op transformation: row counts match.

    Bronze for a Delta source is an identity read (no DQE configured
    in this fixture) — every source row should land in the bronze
    target. If bronze_counts != source_counts, either the Delta
    reader is filtering rows it shouldn't, or the executor's write
    is silently dropping data. This test catches both regressions.

    Silver IS expected to be smaller (it applies the where clauses
    in ``silver_transformations.json``); that's verified by the main
    flow test above.
    """
    source_dir = workdir / "_sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    paths = write_source_data_in_format(spark, repo_root, source_dir, "delta")
    _register_delta_source_tables(spark, paths)

    expected_source_counts = {
        "customers": spark.read.format("delta").load(paths["customers"]).count(),
        "transactions": spark.read.format("delta").load(paths["transactions"]).count(),
    }

    silver_xforms = _write_delta_compatible_silver_transformations(
        workdir / "silver_transformations_delta_identity.json"
    )
    template_path = workdir / "oss_onboarding_delta_identity.json"
    template_path.write_text(
        _render_delta_onboarding(
            customers_path=paths["customers"],
            transactions_path=paths["transactions"],
            silver_transformation_path=str(silver_xforms),
        )
    )
    result = oss_flow_driver(template_path, scenario="delta_identity")
    bronze_counts = result["bronze"].row_counts

    assert bronze_counts["bronze.customers"] == expected_source_counts["customers"], (
        f"bronze.customers row count {bronze_counts['bronze.customers']} does not "
        f"match Delta source count {expected_source_counts['customers']} — "
        "the Delta-source reader appears to be silently dropping or duplicating rows"
    )
    assert bronze_counts["bronze.transactions"] == expected_source_counts["transactions"], (
        f"bronze.transactions row count {bronze_counts['bronze.transactions']} does not "
        f"match Delta source count {expected_source_counts['transactions']} — "
        "the Delta-source reader appears to be silently dropping or duplicating rows"
    )
