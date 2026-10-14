# Databricks notebook source
from pyspark.sql import functions as F


def _sql_name(*parts):
    return ".".join(f"`{part}`" for part in parts)

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("silver_schema", "")
dbutils.widgets.text("gold_schema", "")

catalog = dbutils.widgets.get("uc_catalog_name")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")

orders = spark.read.table(_sql_name(catalog, silver_schema, "orders"))
gold = (
    orders.groupBy("customer_tier", "region")
    .agg(
        F.count("*").alias("order_count"),
        F.sum("amount").alias("gross_amount"),
    )
    .withColumn("refreshed_at", F.current_timestamp())
)

target = _sql_name(catalog, gold_schema, "orders_by_tier_region")
gold.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    target
)
print(f"Built Gold table {target}")
