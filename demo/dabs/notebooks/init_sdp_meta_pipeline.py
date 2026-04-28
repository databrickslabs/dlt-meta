# Databricks notebook source
sdp_meta_whl = spark.conf.get("sdp_meta_whl")
%pip install $sdp_meta_whl  # noqa: E999

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.functions import current_date

def custom_transform_func_test(input_df, _) -> DataFrame:

  if layer == "bronze":
    dummy_param = spark.conf.get("dummy_param",None)
  else:
    dummy_param = "Test NA"

  return (input_df
        .withColumn('last_updated_on', current_date())
        .withColumn('some_dummy_from_task_param', lit(dummy_param) )
        )

# COMMAND ----------

layer = spark.conf.get("layer", None)

from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer, bronze_custom_transform_func=custom_transform_func_test,
silver_custom_transform_func=custom_transform_func_test
)
