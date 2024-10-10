# Databricks notebook source
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl

# COMMAND ----------

layer = spark.conf.get("layer", None)

from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
