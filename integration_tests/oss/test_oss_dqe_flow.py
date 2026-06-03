"""OSS Spark DQE end-to-end flow integration test.

Pins the contract that
:meth:`OSSDataflowPipeline._register_table_with_dqe` inlines
``expect_or_drop`` expectations via :func:`oss_pipelines.wrap_dqe` and
the resulting query function returns a filtered DataFrame — so the
side-channelled Delta table ends up with strictly fewer rows than the
source when at least one row fails the expectation.

Differs from the JSON / CSV / Delta flow tests in that this one *knows*
the source row count (controlled fixture) and the expected post-DQE
count, so the assertion is an equality, not a >0 sanity check. A
regression that wires DQE but accidentally short-circuits the filter
(e.g. ``wrap_dqe`` returning the unmodified qf) would slip past the
other flow tests; this one catches it.
"""
from __future__ import annotations

import json
from pathlib import Path


_DQE_SCHEMA_DDL = "id STRING, email STRING, status STRING"


def _write_dqe_source(workdir: Path) -> tuple[Path, int, int]:
    """Materialise a controlled JSON source with known DQE-failing rows.

    Returns ``(source_dir, total_rows, expected_kept_rows)``:

      - ``total_rows``: every row written to disk.
      - ``expected_kept_rows``: the count after DQE filters out
        rows where ``id IS NULL`` or ``status NOT IN ('A', 'B')``.

    The mix is deliberately small but representative of three failure
    modes: missing id (drop), bad status enum (drop), and valid rows
    (keep). If either expectation is silently no-op'd, the kept count
    drifts up to ``total_rows`` and the test fails loudly.
    """
    src_dir = workdir / "_dqe_source" / "customers"
    src_dir.mkdir(parents=True, exist_ok=True)
    records = [
        # 10 valid rows — pass both expectations.
        *[
            {"id": f"c{i:03d}", "email": f"c{i}@ex.com", "status": "A"}
            for i in range(8)
        ],
        {"id": "c008", "email": "c8@ex.com", "status": "B"},
        {"id": "c009", "email": "c9@ex.com", "status": "B"},
        # 4 rows with NULL id — fail ``valid_id`` expectation.
        {"id": None, "email": "n1@ex.com", "status": "A"},
        {"id": None, "email": "n2@ex.com", "status": "A"},
        {"id": None, "email": "n3@ex.com", "status": "B"},
        {"id": None, "email": "n4@ex.com", "status": "B"},
        # 3 rows with bad status — fail ``valid_status`` expectation.
        {"id": "c010", "email": "c10@ex.com", "status": "X"},
        {"id": "c011", "email": "c11@ex.com", "status": "Y"},
        {"id": "c012", "email": "c12@ex.com", "status": "Z"},
    ]
    # One JSON document per line so Spark's file-stream reader picks
    # up the file structure cleanly without ``multiline``.
    (src_dir / "customers.json").write_text(
        "\n".join(json.dumps(r) for r in records)
    )
    total_rows = len(records)
    expected_kept_rows = 10  # the 8 + 2 valid rows above
    return src_dir, total_rows, expected_kept_rows


def _write_dqe_schema_ddl(workdir: Path) -> Path:
    """Pin the source schema as a DDL so the bronze reader knows the columns.

    Without an explicit schema, Spark's file-stream JSON source
    refuses to start, which would mask the DQE behaviour under a
    completely different failure mode.
    """
    ddl_path = workdir / "_dqe_source" / "customers_schema.ddl"
    ddl_path.write_text(_DQE_SCHEMA_DDL)
    return ddl_path


def _write_dqe_expectations(workdir: Path) -> Path:
    """Pin the two expectations the test asserts on."""
    out = workdir / "_dqe_source" / "expectations.json"
    out.write_text(
        json.dumps(
            {
                "expect_or_drop": {
                    "valid_id": "id IS NOT NULL",
                    "valid_status": "status IN ('A', 'B')",
                }
            },
            indent=2,
        )
    )
    return out


def _render_dqe_onboarding(
    source_path: str,
    source_schema_path: str,
    dqe_expectations_path: str,
    silver_transformation_path: str,
) -> str:
    """Render an onboarding template that wires the DQE on customers."""
    return json.dumps(
        [
            {
                "data_flow_id": "200_dqe",
                "data_flow_group": "OSS",
                "source_system": "FILE",
                "source_format": "json",
                "source_details": {
                    "source_database": "APP",
                    "source_table": "CUSTOMERS",
                    "source_path_dev": source_path,
                    "source_schema_path": source_schema_path,
                },
                "bronze_database_dev": "bronze",
                "bronze_table": "customers",
                "bronze_table_comment": "Bronze customers (DQE)",
                "bronze_reader_options": {},
                "bronze_table_path_dev": "__BRONZE_CUSTOMERS_PATH__",
                "bronze_table_properties": {"pipelines.reset.allowed": "false"},
                "bronze_data_quality_expectations_json_dev": dqe_expectations_path,
                "bronze_quarantine_table": None,
                "silver_database_dev": "silver",
                "silver_table": "customers",
                "silver_table_comment": "Silver customers (DQE)",
                "silver_table_path_dev": "__SILVER_CUSTOMERS_PATH__",
                "silver_table_properties": {"pipelines.reset.allowed": "false"},
                "silver_transformation_json_dev": silver_transformation_path,
                "silver_quarantine_table": None,
            }
        ],
        indent=2,
    )


def _write_dqe_silver_transformation(out: Path) -> Path:
    """Identity silver transformation — no WHERE filter.

    Lets the test isolate DQE filtering. If silver also filters,
    the bronze-to-silver row-count delta would conflate two
    effects and the assertion would be ambiguous.
    """
    out.write_text(
        json.dumps(
            [
                {
                    "data_flow_id": "200_dqe",
                    "target_table": "customers",
                    "select_exp": ["id", "email", "status"],
                    "where_clause": [],
                }
            ],
            indent=2,
        )
    )
    return out


def test_oss_dqe_expect_or_drop_filters_rows_end_to_end(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    oss_flow_driver,
):
    """End-to-end DQE: source N rows → bronze K<N rows after expect_or_drop.

    Pins the contract that ``OSSDataflowPipeline._register_table_with_dqe``
    inlines ``expect_or_drop`` via :func:`oss_pipelines.wrap_dqe`,
    producing a query function whose output is the expectation-filtered
    DataFrame. The persisted Delta row count must equal
    ``expected_kept_rows``, not ``total_rows``.

    Silver gets an identity transformation (no WHERE clause), so
    ``silver.customers`` count must equal ``bronze.customers`` count —
    proving DQE only filters once (at bronze) and silver doesn't
    silently double-filter or duplicate.
    """
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

    result = oss_flow_driver(template_path, scenario="dqe")

    all_failures = result["bronze"].failures + result["silver"].failures
    assert not all_failures, (
        "OSS DQE flow had per-table failures:\n  "
        + "\n  ".join(f"{name}: {exc}" for name, exc in all_failures)
    )

    bronze_count = result["bronze"].row_counts["bronze.customers"]
    silver_count = result["silver"].row_counts["silver.customers"]

    assert bronze_count == expected_kept_rows, (
        f"bronze.customers row count {bronze_count} does not match expected "
        f"post-DQE count {expected_kept_rows} (source had {total_rows} rows; "
        "expect_or_drop should have removed rows where id IS NULL or "
        "status NOT IN ('A','B'))"
    )

    assert silver_count == bronze_count, (
        f"silver.customers ({silver_count}) does not match bronze.customers "
        f"({bronze_count}) under an identity silver transformation — DQE may "
        "be applied twice or the silver layer is dropping rows unexpectedly"
    )


def test_oss_dqe_filtered_rows_satisfy_expectations(
    spark,
    oss_runtime_env,
    repo_root: Path,
    workdir: Path,
    oss_flow_driver,
):
    """Every persisted bronze row passes every expect_or_drop predicate.

    Stronger than the row-count assertion above: re-evaluates the
    DQE predicates against the persisted bronze Delta data and
    requires 0 violations. Without this, a regression that drops
    the wrong rows (e.g. inverts the predicate logic) could
    accidentally still match the expected count.
    """
    source_dir, _total_rows, _expected_kept_rows = _write_dqe_source(workdir)
    schema_ddl = _write_dqe_schema_ddl(workdir)
    expectations = _write_dqe_expectations(workdir)
    silver_xforms = _write_dqe_silver_transformation(
        workdir / "silver_transformations_dqe_check.json"
    )

    template_path = workdir / "oss_onboarding_dqe_check.json"
    template_path.write_text(
        _render_dqe_onboarding(
            source_path=str(source_dir),
            source_schema_path=str(schema_ddl),
            dqe_expectations_path=str(expectations),
            silver_transformation_path=str(silver_xforms),
        )
    )

    oss_flow_driver(template_path, scenario="dqe_check")

    bronze = spark.read.format("delta").table("bronze.customers")
    null_ids = bronze.filter("id IS NULL").count()
    bad_status = bronze.filter("status NOT IN ('A', 'B')").count()
    assert null_ids == 0, (
        f"bronze.customers has {null_ids} rows with NULL id — "
        "the ``valid_id`` expectation didn't actually drop them"
    )
    assert bad_status == 0, (
        f"bronze.customers has {bad_status} rows with status NOT IN ('A','B') "
        "— the ``valid_status`` expectation didn't actually drop them"
    )
