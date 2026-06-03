"""Databricks Labs SDP-META Framework.

A metadata-driven framework for Spark Declarative Pipelines. Runs on
Databricks Lakeflow (the superset runtime that ships extensions like
``expect_*`` and ``create_auto_cdc_flow``) and on OSS Apache Spark 4.1+
``pyspark.pipelines`` (the public subset). Runtime selection is automatic;
override with ``SDP_META_RUNTIME={databricks,oss}``.

Example usage:

    from databricks.labs.sdp_meta import DataflowPipeline
    DataflowPipeline.invoke_pipeline(spark, "bronze")

The legacy names ``invoke_dlt_pipeline`` / ``run_dlt`` / ``DLTSinkWriter``
remain available unchanged.
"""
from databricks.labs.sdp_meta.__about__ import __version__, __package_name__

__all__ = [
    "__version__",
    "__package_name__",
    "DataflowPipeline",
    "OSSDataflowPipeline",
    "AppendFlowWriter",
    "SinkWriter",
    "DLTSinkWriter",
    "is_oss",
    "is_databricks",
]


# Lazy attribute → ``(submodule, attr_name)`` dispatch table.
# ``__getattr__`` defers importing the runtime modules (which in turn import
# ``pyspark.pipelines``) until the symbols are actually requested. Keeps
# ``import databricks.labs.sdp_meta`` cheap and importable in environments
# without pyspark installed (e.g. CLI-only paths).
_LAZY_EXPORTS = {
    "DataflowPipeline": ("databricks.labs.sdp_meta.dataflow_pipeline", "DataflowPipeline"),
    "OSSDataflowPipeline": (
        "databricks.labs.sdp_meta.oss_dataflow_pipeline", "OSSDataflowPipeline"),
    "AppendFlowWriter": ("databricks.labs.sdp_meta.pipeline_writers", "AppendFlowWriter"),
    "SinkWriter": ("databricks.labs.sdp_meta.pipeline_writers", "SinkWriter"),
    "DLTSinkWriter": ("databricks.labs.sdp_meta.pipeline_writers", "DLTSinkWriter"),
    "is_oss": ("databricks.labs.sdp_meta.oss_pipelines", "is_oss"),
    "is_databricks": ("databricks.labs.sdp_meta.oss_pipelines", "is_databricks"),
}


def __getattr__(name):
    try:
        module_path, attr_name = _LAZY_EXPORTS[name]
    except KeyError:
        raise AttributeError(
            f"module 'databricks.labs.sdp_meta' has no attribute {name!r}"
        ) from None
    import importlib
    return getattr(importlib.import_module(module_path), attr_name)
