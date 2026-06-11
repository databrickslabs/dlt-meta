# Databricks notebook source
# Backward-compatibility integration test runner for LEGACY-shape
# pipelines (dlt-meta v0.0.1 .. v0.0.10).
#
# This notebook is INTENTIONALLY a byte-for-byte clone of the v0.0.10
# init notebook customers run in production. The whole point of the
# backward-compat test is to prove that a v0.0.10 customer who flips
# their ``dlt_meta_whl`` config from a v0.0.10 wheel to a v0.0.11
# wheel -- without changing any other line in their pipeline or
# notebook -- gets a working upgrade.
#
# Single-key, single-install contract
# -----------------------------------
# Phase 1 (SOURCE wheel = v0.0.10 dlt_meta):
#   dlt_meta_whl -> /Volumes/.../dlt_meta-0.0.10.whl
#
# Phase 2 cross-namespace (legacy -> current):
#   dlt_meta_whl -> /Volumes/.../databricks_labs_sdp_meta-0.0.11.whl
#
# How ``from src.* import …`` keeps resolving on Phase 2
# ------------------------------------------------------
# The v0.0.11 main wheel BUNDLES a real top-level ``src`` package (plus
# a ``dlt_meta`` package), both re-exporting ``databricks.labs.sdp_meta.*``.
# After ``%pip install`` lands the wheel in ``site-packages/``, the
# legacy ``from src.dataflow_pipeline import …`` line below walks normal
# import machinery: it finds ``src/`` in site-packages, runs its
# ``__init__`` (which registers ``src.<sub>`` ->
# ``databricks.labs.sdp_meta.<sub>`` aliases in ``sys.modules``), and
# resolves to the canonical ``databricks.labs.sdp_meta.dataflow_pipeline``
# symbols.
#
# This does NOT rely on a ``.pth`` startup hook. Serverless ``%pip
# install`` does not re-trigger ``site.py``'s ``.pth`` scan, so the
# v0.0.11 wheel deliberately resolves the legacy namespace through a
# real package rather than a startup hook. No explicit ``import
# dlt_meta`` is needed here. (See the top-level ``setup.py`` for the
# bundling mechanism.)
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl  # noqa : E999

# COMMAND ----------

layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
