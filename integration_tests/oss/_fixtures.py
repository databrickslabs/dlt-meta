"""Helpers for generating per-format source data + onboarding templates.

Avoids the need to commit large parallel copies of ``oss_onboarding.json``
for every format the integration suite covers. Each helper materialises
the source data in the requested format (read from the canonical JSON
under ``tests/resources/data/``) and emits an onboarding spec template
with placeholders the conftest's ``materialize_onboarding`` fixture
substitutes.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_BRONZE_ONBOARDING_PLACEHOLDERS = (
    "__BRONZE_CUSTOMERS_PATH__",
    "__BRONZE_TRANSACTIONS_PATH__",
    "__SILVER_CUSTOMERS_PATH__",
    "__SILVER_TRANSACTIONS_PATH__",
)


def write_source_data_in_format(
    spark: Any,
    repo_root: Path,
    out_dir: Path,
    fmt: str,
) -> dict[str, str]:
    """Read the canonical JSON test data and write it back as ``fmt``.

    Supported ``fmt``: ``"csv"``, ``"parquet"``, ``"orc"``, ``"delta"``.
    Returns a map of logical table name (``customers`` / ``transactions``)
    to the output directory holding the per-format data.

    The output directory is created under ``out_dir`` and includes the
    format name so successive calls for different formats don't clobber
    each other.
    """
    src_root = repo_root / "tests" / "resources" / "data"
    table_paths: dict[str, str] = {}
    for table in ("customers", "transactions"):
        src = src_root / table
        dst = out_dir / fmt / table
        dst.mkdir(parents=True, exist_ok=True)
        df = spark.read.option("multiline", "true").json(str(src))
        writer = df.write.mode("overwrite")
        if fmt == "csv":
            # Header on so the bronze reader can pick up column names
            # without a schema, mirroring how a real customer would
            # ingest a CSV dump.
            writer.option("header", "true").csv(str(dst))
        elif fmt in ("parquet", "orc"):
            getattr(writer, fmt)(str(dst))
        elif fmt == "delta":
            writer.format("delta").save(str(dst))
        else:
            raise ValueError(f"unsupported fmt={fmt!r}")
        table_paths[table] = str(dst)
    return table_paths


def render_file_source_onboarding(
    fmt: str,
    *,
    customers_source_path: str,
    transactions_source_path: str,
    customers_schema_ddl: str,
    transactions_schema_ddl: str,
    reader_options: dict | None = None,
    silver_transformation_path: str | None = None,
) -> str:
    """Render an onboarding template for a vanilla file-source flow.

    Returns JSON text with the four ``__<...>_PATH__`` placeholders the
    conftest's ``materialize_onboarding`` will substitute for per-test
    output paths. ``reader_options`` defaults to the
    ``{"header": "true"}`` shape CSV needs; pass an empty dict for
    schema-bearing binary formats like parquet/orc.

    ``silver_transformation_path`` is required to wire silver; pass
    the absolute path to the committed
    ``tests/resources/silver_transformations.json``.
    """
    if reader_options is None:
        reader_options = {"header": "true"} if fmt == "csv" else {}

    rows = [
        {
            "data_flow_id": f"200_{fmt}",
            "data_flow_group": "OSS",
            "source_system": "FILE",
            "source_format": fmt,
            "source_details": {
                "source_database": "APP",
                "source_table": "CUSTOMERS",
                "source_path_dev": customers_source_path,
                "source_schema_path": customers_schema_ddl,
            },
            "bronze_database_dev": "bronze",
            "bronze_table": "customers",
            "bronze_table_comment": f"Bronze customers ({fmt})",
            "bronze_reader_options": reader_options,
            "bronze_table_path_dev": "__BRONZE_CUSTOMERS_PATH__",
            "bronze_table_properties": {"pipelines.reset.allowed": "false"},
            "bronze_quarantine_table": None,
            "silver_database_dev": "silver",
            "silver_table": "customers",
            "silver_table_comment": f"Silver customers ({fmt})",
            "silver_table_path_dev": "__SILVER_CUSTOMERS_PATH__",
            "silver_table_properties": {"pipelines.reset.allowed": "false"},
            "silver_transformation_json_dev": silver_transformation_path,
            "silver_quarantine_table": None,
        },
        {
            "data_flow_id": f"201_{fmt}",
            "data_flow_group": "OSS",
            "source_system": "FILE",
            "source_format": fmt,
            "source_details": {
                "source_database": "APP",
                "source_table": "TRANSACTIONS",
                "source_path_dev": transactions_source_path,
                "source_schema_path": transactions_schema_ddl,
            },
            "bronze_database_dev": "bronze",
            "bronze_table": "transactions",
            "bronze_table_comment": f"Bronze transactions ({fmt})",
            "bronze_reader_options": reader_options,
            "bronze_table_path_dev": "__BRONZE_TRANSACTIONS_PATH__",
            "bronze_table_properties": {"pipelines.reset.allowed": "false"},
            "bronze_quarantine_table": None,
            "silver_database_dev": "silver",
            "silver_table": "transactions",
            "silver_table_comment": f"Silver transactions ({fmt})",
            "silver_table_path_dev": "__SILVER_TRANSACTIONS_PATH__",
            "silver_table_properties": {"pipelines.reset.allowed": "false"},
            "silver_transformation_json_dev": silver_transformation_path,
            "silver_quarantine_table": None,
        },
    ]
    return json.dumps(rows, indent=2)


def write_onboarding_template(
    workdir: Path,
    *,
    fmt: str,
    repo_root: Path,
    spark: Any,
    extra_kwargs: dict | None = None,
) -> Path:
    """Convenience wrapper: materialise data + render template + write to disk.

    Returns the path to the written ``oss_onboarding_<fmt>.json``
    template (still containing the ``__BRONZE_<...>_PATH__`` /
    ``__SILVER_<...>_PATH__`` placeholders). The caller passes this to
    the conftest's ``materialize_onboarding`` fixture to get the
    per-test substituted onboarding file ready for
    ``OnboardDataflowspec``.
    """
    source_dir = workdir / "_sources"
    source_dir.mkdir(parents=True, exist_ok=True)
    paths = write_source_data_in_format(spark, repo_root, source_dir, fmt)

    extra = extra_kwargs or {}
    rendered = render_file_source_onboarding(
        fmt,
        customers_source_path=paths["customers"],
        transactions_source_path=paths["transactions"],
        customers_schema_ddl=str(
            repo_root / "tests" / "resources" / "schema" / "customer_schema.ddl"
        ),
        transactions_schema_ddl=str(
            repo_root / "tests" / "resources" / "schema" / "transactions_schema.ddl"
        ),
        silver_transformation_path=str(
            repo_root / "tests" / "resources" / "silver_transformations.json"
        ),
        **extra,
    )
    out = workdir / f"oss_onboarding_{fmt}.json"
    out.write_text(rendered)
    return out
