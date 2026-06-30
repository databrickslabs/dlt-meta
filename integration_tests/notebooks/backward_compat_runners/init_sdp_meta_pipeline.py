# Databricks notebook source
# Backward-compatibility integration test runner for CURRENT-shape
# pipelines (sdp-meta v0.1.0+).
#
# Mirrors ``integration_tests/notebooks/cloudfile_runners/init_sdp_meta_pipeline.py``
# but lives under ``backward_compat_runners/`` so the orchestrator
# uploads it from a single, consistent directory regardless of which
# profile (LEGACY or CURRENT) is the upgrade source. Used as the
# runner notebook when the source profile of an upgrade is CURRENT
# (e.g. v0.1.0 -> v0.1.1).
#
# Phase 1: ``sdp_meta_whl`` -> source-version sdp-meta wheel.
# Phase 2: ``sdp_meta_whl`` swapped to the target-version sdp-meta
# wheel via ``pipelines.update()``. No compat shim is needed here
# because the namespace did not change.
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl  # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
