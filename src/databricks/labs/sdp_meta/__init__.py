"""Databricks Labs SDP-META Framework.

A metadata-driven framework for Databricks Lakeflow Spark Declarative Pipelines.

The framework uses the Lakeflow Spark Declarative Pipelines runtime for building
automated bronze and silver data pipelines.

Example usage:
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
    from databricks.labs.sdp_meta.cli import SDPMeta
    from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
"""
from databricks.labs.sdp_meta.__about__ import __version__, __package_name__

__all__ = [
    "__version__",
    "__package_name__",
]
