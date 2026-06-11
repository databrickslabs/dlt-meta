# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

# Snapshot strategy for dtix (LFC SCD2, no-PK table).
#   cdf  (default) — custom next_snapshot_and_version lambda:
#     * O(1) fast skip when the source Delta table version has not changed.
#     * O(n) full-table read when changes exist (same cost as "full", but skips
#       the expensive run entirely on a no-change trigger).
#   full — built-in view-based apply_changes_from_snapshot.  Reads and
#     materialises the entire source table on every pipeline trigger.
#     Use as a stable reference or when the lambda causes issues.
_snapshot_method = spark.conf.get("dtix_snapshot_method", "cdf")

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from pyspark.sql import DataFrame
import traceback as _tb


def bronze_transform(df: DataFrame, dataflowSpec) -> DataFrame:
    """Rename LFC SCD2 reserved column names for the view-based ("full") path.

    DLT globally reserves __START_AT and __END_AT as system column names for
    SCD Type 2 tracking.  Any source table that already contains these columns
    (e.g. LFC SCD2 output tables like dtix) must have them renamed before DLT
    analyses the schema, otherwise DLT either raises DLTAnalysisException
    (for apply_changes) or silently drops them (for apply_changes_from_snapshot),
    making them unresolvable as keys.

    When _snapshot_method == "cdf" the lambda handles the rename directly and
    this transform is not called for dtix.  It remains active for the "full"
    view-based path where DLT creates a view over the source table.
    """
    target_table = dataflowSpec.targetDetails.get("table", "") if dataflowSpec.targetDetails else ""
    if target_table == "dtix":
        if "__START_AT" in df.columns:
            df = df.withColumnRenamed("__START_AT", "lfc_start_at")
        if "__END_AT" in df.columns:
            df = df.withColumnRenamed("__END_AT", "lfc_end_at")
        df = df.dropDuplicates()
    return df


def dtix_next_snapshot_and_version(latest_snapshot_version, dataflowSpec):
    """Custom snapshot function for the dtix LFC SCD2 no-PK table.

    Strategy:
      1. Resolve source table from the dataflowSpec (O(1) metadata look-up).
      2. Read the current Delta table version from DESCRIBE HISTORY (O(1)).
      3. If the version has not advanced since the last pipeline run return None
         immediately — DLT marks the run complete without touching any data.
      4. Otherwise read the full current state of the source (O(n)), rename
         DLT-reserved columns __START_AT → lfc_start_at and __END_AT → lfc_end_at,
         deduplicate, and return (df, current_version) for DLT to diff.

    The O(1) fast-skip in step 3 makes this preferable to the view-based path
    for frequently-triggered pipelines where the source changes infrequently.
    """
    try:
        # DLT-Meta onboarding maps source_catalog_prod → sourceDetails["catalog"],
        # source_database → sourceDetails["source_database"],
        # source_table   → sourceDetails["source_table"].
        source_catalog = dataflowSpec.sourceDetails.get("catalog")
        source_db = dataflowSpec.sourceDetails.get("source_database")
        source_tbl = dataflowSpec.sourceDetails.get("source_table")
        catalog_prefix = f"{source_catalog}." if source_catalog else ""
        full_table = f"{catalog_prefix}{source_db}.{source_tbl}"

        # O(1) Delta version check — no data scan required.
        current_version = (
            spark.sql(f"DESCRIBE HISTORY {full_table} LIMIT 1").first()["version"]
        )

        if latest_snapshot_version is not None and latest_snapshot_version >= current_version:
            print(
                f"[dtix_snapshot] no change since version {latest_snapshot_version} "
                f"(current={current_version}), skipping."
            )
            return None

        print(
            f"[dtix_snapshot] change detected: {latest_snapshot_version} → {current_version}. "
            "Reading full table."
        )
        df = spark.read.table(full_table)

        # Rename DLT-reserved columns before DLT processes the snapshot.
        if "__START_AT" in df.columns:
            df = df.withColumnRenamed("__START_AT", "lfc_start_at")
        if "__END_AT" in df.columns:
            df = df.withColumnRenamed("__END_AT", "lfc_end_at")

        # Collapse fully-identical rows (no-PK source can have duplicates).
        df = df.dropDuplicates()

        return (df, current_version)
    except Exception as e:
        print(f"[dtix_snapshot] ERROR in dtix_next_snapshot_and_version: {e}")
        _tb.print_exc()
        raise


# Wire up the chosen snapshot strategy.
# "cdf": pass the custom lambda as bronze_next_snapshot_and_version.
#        is_create_view() will return False (no view registered for dtix) and
#        apply_changes_from_snapshot() will use the lambda as its DLT source.
# "full": pass None → DLT-Meta falls back to the built-in view-based path
#         (next_snapshot_and_version_from_source_view=True) and bronze_transform
#         handles the column rename via the view.
_bronze_next_snapshot = dtix_next_snapshot_and_version if _snapshot_method == "cdf" else None
_bronze_transform = bronze_transform if _snapshot_method == "full" else None

print(
    f"[init_sdp_meta_pipeline] layer={layer} snapshot_method={_snapshot_method} "
    f"using_lambda={_bronze_next_snapshot is not None}"
)

DataflowPipeline.invoke_dlt_pipeline(
    spark,
    layer,
    bronze_custom_transform_func=_bronze_transform,
    bronze_next_snapshot_and_version=_bronze_next_snapshot,
)
