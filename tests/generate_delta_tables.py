"""Generates test delta tables locally."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta.pip_utils import configure_spark_with_delta_pip


builder = (
    SparkSession.builder.appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
customers_parquet_df = spark.read.options(**options).json("tests/resources/data/customers")
customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta").mode("overwrite").save(
    "tests/resources/delta/customers"
)

transactions_parquet_df = spark.read.options(**options).json("tests/resources/data/transactions")
transactions_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta").mode("overwrite").save(
    "tests/resources/delta/transactions")
