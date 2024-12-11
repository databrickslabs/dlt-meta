# Databricks notebook source
# DBTITLE 1,Install datagen
# MAGIC %pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Notebook inputs
dbutils.widgets.text("base_input_path","", "base_input_path")
dbutils.widgets.text("table_count","", "table_count")
dbutils.widgets.text("table_column_count","", "table_column_count")
dbutils.widgets.text("table_data_rows_count","", "table_data_rows_count")
dbutils.widgets.text("dlt_meta_schema","", "dlt_meta_schema")
dbutils.widgets.text("uc_catalog_name","", "uc_catalog_name")
dbutils.widgets.text("bronze_schema","", "bronze_schema")
dbutils.widgets.text("silver_schema","", "bronze_schema")



base_input_path = dbutils.widgets.get("base_input_path")
table_column_count = int(dbutils.widgets.get("table_column_count"))
table_data_rows_count = int(dbutils.widgets.get("table_data_rows_count"))
table_count = int(dbutils.widgets.get("table_count"))
dlt_meta_schema = dbutils.widgets.get("dlt_meta_schema")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

# DBTITLE 1,Data Generator Functions
import dbldatagen as dg
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, collect_list, struct, col
from pyspark.sql.types import StringType, StructType, StructField, MapType, ArrayType, FloatType, IntegerType

builder = SparkSession.builder.appName("DLT-META_TECH_SUMMIT")
spark = builder.getOrCreate()


def generate_table_data(spark, base_input_path, column_count, data_rows, table_count):
    table_path = f"{base_input_path}/resources/data/input/table"
    table_path = table_path+"_{}"
    for i in range(1, (table_count + 1)):
        df_spec = (dg.DataGenerator(spark, name="dlt_meta_demo", rows=data_rows, partitions=4)
                   .withIdOutput()
                   .withColumn("r", FloatType(),
                               expr="floor(rand() * 350) * (86400 + 3600)",
                               numColumns=column_count)
                   .withColumn("code1", IntegerType(), minValue=100, maxValue=(table_count + 200))
                   .withColumn("code2", IntegerType(), minValue=1, maxValue=(table_count + 10))
                   .withColumn("code3", StringType(), values=['a', 'b', 'c'])
                   .withColumn("code4", StringType(), values=['a', 'b', 'c'], random=True)
                   .withColumn("code5", StringType(), values=['a', 'b', 'c'], random=True, weights=[9, 1, 1]))
        df = df_spec.build()
        df.coalesce(1).write.mode("append").option("header", "True").csv(table_path.format(i))


def generate_onboarding_file(spark, base_input_path, table_count, dlt_meta_schema):
    data_flow_spec_columns = [
        "data_flow_id",
        "data_flow_group",
        "source_system",
        "source_format",
        "source_details",
        "bronze_database_prod",
        "bronze_table",
        "bronze_reader_options",
        "bronze_data_quality_expectations_json_prod",
        "bronze_database_quarantine_prod",
        "bronze_quarantine_table",
        "silver_database_prod",
        "silver_table",
        "silver_transformation_json_prod",
        "silver_data_quality_expectations_json_prod"
    ]

    data_flow_spec_schema = StructType(
        [
            StructField("data_flow_id", StringType(), True),
            StructField("data_flow_group", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("source_format", StringType(), True),
            StructField("source_details", MapType(StringType(), StringType(), True), True),
            StructField("bronze_database_prod", StringType(), True),
            StructField("bronze_table", StringType(), True),
            StructField(
                "bronze_reader_options",
                MapType(StringType(), StringType(), True),
                True,
            ),
            StructField("bronze_data_quality_expectations_json_prod", StringType(), True),
            StructField("bronze_database_quarantine_prod", StringType(), True),
            StructField("bronze_quarantine_table", StringType(), True),
            StructField("silver_database_prod", StringType(), True),
            StructField("silver_table", StringType(), True),
            StructField("silver_transformation_json_prod", StringType(), True),
            StructField("silver_data_quality_expectations_json_prod", StringType(), True)
        ]
    )
    dbfs_path = base_input_path#f"{base_input_path}/resources/data/input"
    data = []
    for row_id in range(1, (table_count+1)):
        data_flow_id = row_id
        table_name = f"table_{row_id}"
        data_flow_group = "A1"
        source_system = "mysql"
        source_format = "cloudFiles"
        input_path = base_input_path+"/resources/data/input/table_{}"
        source_details = {"source_database": "demo", "source_table": f"{table_name}", "source_path_prod":input_path.format(row_id)}
        bronze_database_prod = f"{uc_catalog_name}.{bronze_schema}"
        bronze_table = f"{table_name}"
        bronze_reader_options = {
            "cloudFiles.format": "csv",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
            "header": "true"
        }
        bronze_data_quality_expectations_json_prod = f"{dbfs_path}/conf/dqe/dqe.json"
        bronze_database_quarantine_prod = f"{uc_catalog_name}.{bronze_schema}"
        bronze_quarantine_table = f"{table_name}_quarantine"
        silver_database_prod = f"{uc_catalog_name}.{silver_schema}"
        silver_table = f"{table_name}"
        silver_transformation_json_prod = f"{dbfs_path}/conf/silver_transformations.json"
        silver_data_quality_expectations_json_prod = f"{dbfs_path}/conf/dqe/silver_dqe.json"
        onboarding_row = (
            data_flow_id,
            data_flow_group,
            source_system,
            source_format,
            source_details,
            bronze_database_prod,
            bronze_table,
            bronze_reader_options,
            bronze_data_quality_expectations_json_prod,
            bronze_database_quarantine_prod,
            bronze_quarantine_table,
            silver_database_prod,
            silver_table,
            silver_transformation_json_prod,
            silver_data_quality_expectations_json_prod
        )
        data.append(onboarding_row)

    data_flow_spec_rows_df = spark.createDataFrame(data, data_flow_spec_schema).toDF(*data_flow_spec_columns)
    data_flow_spec_rows_df.show()
    (data_flow_spec_rows_df.agg(to_json(collect_list(struct([col(col_name) for col_name in data_flow_spec_schema.fieldNames()])))).alias("d")
     .coalesce(1).write.mode("overwrite").text(f"{base_input_path}/conf/onboarding_auto.txt")    
    )
    files_list = dbutils.fs.ls(f"{base_input_path}/conf/onboarding_auto.txt")
    for ff in files_list:
      if ff.path.endswith(".txt"):
        dbutils.fs.cp(ff.path,f"{base_input_path}/conf/onboarding.json")
        break
    dbutils.fs.rm(f"{base_input_path}/conf/onboarding_auto.txt",True)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {bronze_database_prod} COMMENT '{bronze_database_prod}'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_database_prod} COMMENT '{silver_database_prod}'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dlt_meta_schema} COMMENT '{dlt_meta_schema}'")



def generate_silver_transformation_json(spark, base_input_path, table_count):
    st_columns = ["target_table", "select_exp"]
    st_schema = StructType(
        [
            StructField("target_table", StringType(), True),
            StructField("select_exp", ArrayType(StringType(), True), True),

        ]
    )
    data = []
    for row_id in range(1, (table_count+1)):
        target_table = f"table_{row_id}"
        select_exp = [
            "id", "r_0", "r_1", "r_2", "r_3", "r_4",
            "concat(code1,' ',code2) as new_code", "code3", "code4", "code5", "_rescued_data"]
        line = (target_table, select_exp)
        data.append(line)
    
    silver_transformations_rows_df = spark.createDataFrame(data, st_schema).toDF(*st_columns)
    silver_transformations_rows_df.show()
    silver_transformations_rows_df.coalesce(1).write.mode("overwrite").json(
        f"{base_input_path}/conf/silver_transformations_auto.json"
    )
    (silver_transformations_rows_df.agg(to_json(collect_list(struct([col(col_name) for col_name in st_schema.fieldNames()])))).alias("d")
     .coalesce(1).write.mode("overwrite").text(f"{base_input_path}/conf/silver_transformations_auto.txt")    
    )    
    files_list = dbutils.fs.ls(f"{base_input_path}/conf/silver_transformations_auto.txt")
    for ff in files_list:
      if ff.path.endswith(".txt"):
        dbutils.fs.cp(ff.path,f"{base_input_path}/conf/silver_transformations.json")
        break
    dbutils.fs.rm(f"{base_input_path}/conf/silver_transformations_auto.txt",True)

def generate_dqe_json(base_input_path):
  dqe_json = """{
              "expect_or_drop": {
                  "no_rescued_data": "_rescued_data IS NULL",
                  "valid_product_id": "id IS NOT NULL AND id>0"
              },
              "expect_or_quarantine": {
                  "quarantine_rule": "_rescued_data IS NOT NULL OR id IS NULL OR id=0"
              }
          }"""
  dbutils.fs.put(f"{base_input_path}/conf/dqe/dqe.json", dqe_json, True)
  silver_dqe_json = """{
              "expect_or_drop": {
                  "valid_product_id": "id IS NOT NULL AND id>0"
              }
          }"""
  dbutils.fs.put(f"{base_input_path}/conf/dqe/silver_dqe.json", dqe_json, True)  


# COMMAND ----------

# DBTITLE 1,Generate Test Data
generate_table_data(spark, base_input_path, table_column_count, table_data_rows_count, table_count)

# COMMAND ----------

# DBTITLE 1,Generates Onboarding files
generate_onboarding_file(spark, base_input_path, table_count, dlt_meta_schema)
generate_silver_transformation_json(spark, base_input_path, table_count)
generate_dqe_json(base_input_path)