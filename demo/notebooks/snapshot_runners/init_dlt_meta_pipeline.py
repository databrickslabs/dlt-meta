# Databricks notebook source
dlt_meta_whl = spark.conf.get("dlt_meta_whl")
%pip install $dlt_meta_whl

# COMMAND ----------

# Databricks notebook source
# DBTITLE 1,DLT Snapshot Processing Logic
import dlt
from src.dataflow_spec import BronzeDataflowSpec

def exist(path):
    try:
        if dbutils.fs.ls(path) is None:
            return False
        else:
            return True
    except:
        return False


def next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
    latest_snapshot_version = latest_snapshot_version or 0
    next_version = latest_snapshot_version + 1    
    bronze_dataflow_spec: BronzeDataflowSpec = dataflow_spec
    options = bronze_dataflow_spec.readerConfigOptions
    snapshot_format =  bronze_dataflow_spec.sourceDetails["snapshot_format"]
    snapshot_root_path = bronze_dataflow_spec.sourceDetails['path']    
    snapshot_path = f"{snapshot_root_path}{next_version}.csv"
    if (exist(snapshot_path)):
        snapshot = spark.read.format(snapshot_format).options(**options).load(snapshot_path)
        return (snapshot, next_version)
    else:
        # No snapshot available
        return None 


# COMMAND ----------

layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer, bronze_next_snapshot_and_version=next_snapshot_and_version, silver_next_snapshot_and_version=None)
# DataflowPipeline.invoke_dlt_pipeline(spark, layer)
