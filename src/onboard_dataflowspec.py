"""OnboardDataflowSpec class provides bronze/silver onboarding features."""

import copy
import dataclasses
import json
import logging

import pyspark.sql.types as T
from pyspark.sql import functions as f
from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

from src.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils, SilverDataflowSpec
from src.metastore_ops import DeltaPipelinesInternalTableOps, DeltaPipelinesMetaStoreOps

logger = logging.getLogger("databricks.labs.dltmeta")
logger.setLevel(logging.INFO)


class OnboardDataflowspec:
    """OnboardDataflowSpec class provides bronze/silver onboarding features."""

    def __init__(self, spark, dict_obj, bronze_schema_mapper=None, uc_enabled=False):
        """Onboard Dataflowspec Constructor."""
        self.spark = spark
        self.dict_obj = dict_obj
        self.bronze_dict_obj = copy.deepcopy(dict_obj)
        self.silver_dict_obj = copy.deepcopy(dict_obj)
        self.uc_enabled = uc_enabled
        self.__initialize_paths(uc_enabled)
        self.bronze_schema_mapper = bronze_schema_mapper
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.onboard_file_type = None

    def __initialize_paths(self, uc_enabled):
        if "silver_dataflowspec_table" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_table"]
        if "silver_dataflowspec_path" in self.bronze_dict_obj:
            del self.bronze_dict_obj["silver_dataflowspec_path"]

        if "bronze_dataflowspec_table" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_table"]
        if "bronze_dataflowspec_path" in self.silver_dict_obj:
            del self.silver_dict_obj["bronze_dataflowspec_path"]
        if uc_enabled:
            if "bronze_dataflowspec_path" in self.bronze_dict_obj:
                del self.bronze_dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.silver_dict_obj:
                del self.silver_dict_obj["silver_dataflowspec_path"]

    @staticmethod
    def __validate_dict_attributes(attributes, dict_obj):
        """Validate dict attributes method will validate dict attributes keys.

        Args:
            attributes ([type]): [description]
            dict_obj ([type]): [description]

        Raises:
            ValueError: [description]
        """
        if sorted(set(attributes)) != sorted(set(dict_obj.keys())):
            attributes_keys = set(dict_obj.keys())
            logger.info("In validate dict attributes")
            logger.info(f"expected: {set(attributes)}, actual: {attributes_keys}")
            logger.info(
                "missing attributes : {}".format(
                    set(attributes).difference(attributes_keys)
                )
            )
            raise ValueError(
                f"missing attributes : {set(attributes).difference(attributes_keys)}"
            )

    def onboard_dataflow_specs(self):
        """
        Onboard_dataflow_specs method will onboard dataFlowSpecs for bronze, silver and gold.

        This method takes in a SparkSession object and a dictionary object containing the following attributes:
        - onboarding_file_path: The path to the onboarding file.
        - database: The name of the database to onboard the dataflow specs to.
        - env: The environment to onboard the dataflow specs to.
        - bronze_dataflowspec_table: The name of the bronze dataflow specs table.
        - bronze_dataflowspec_path: The path to the bronze dataflow specs.
        - silver_dataflowspec_table: The name of the silver dataflow specs table.
        - silver_dataflowspec_path: The path to the silver dataflow specs.
        - import_author: The author of the import.
        - version: The version of the import.
        - overwrite: Whether to overwrite existing dataflow specs or not.

        If the `uc_enabled` flag is set to True, the dictionary object must contain all the attributes listed above.
        If the `uc_enabled` flag is set to False, the dictionary object must contain all the attributes listed above
        except for `bronze_dataflowspec_path` and `silver_dataflowspec_path`.

        This method calls the `onboard_bronze_dataflow_spec` and `onboard_silver_dataflow_spec` methods to onboard
        the bronze and silver dataflow specs respectively.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        if self.uc_enabled:
            if "bronze_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["bronze_dataflowspec_path"]
            if "silver_dataflowspec_path" in self.dict_obj:
                del self.dict_obj["silver_dataflowspec_path"]
            self.__validate_dict_attributes(attributes, self.dict_obj)
        else:
            attributes.append("bronze_dataflowspec_path")
            attributes.append("silver_dataflowspec_path")
            self.__validate_dict_attributes(attributes, self.dict_obj)
        self.onboard_bronze_dataflow_spec()
        self.onboard_silver_dataflow_spec()

    def register_bronze_dataflow_spec_tables(self):
        """Register bronze/silver dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["bronze_dataflowspec_table"],
            self.dict_obj["bronze_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded bronze table={self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["bronze_dataflowspec_table"]}"""
        ).show()

    def register_silver_dataflow_spec_tables(self):
        """Register bronze dataflow specs tables."""
        self.deltaPipelinesMetaStoreOps.create_database(
            self.dict_obj["database"], "dlt-meta database"
        )
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.dict_obj["database"],
            self.dict_obj["silver_dataflowspec_table"],
            self.dict_obj["silver_dataflowspec_path"],
        )
        logger.info(
            f"""onboarded silver table={self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        )
        self.spark.read.table(
            f"""{self.dict_obj["database"]}.{self.dict_obj["silver_dataflowspec_table"]}"""
        ).show()

    def onboard_silver_dataflow_spec(self):
        """
        Onboard silver dataflow spec.

        Args:
            onboarding_df (pyspark.sql.DataFrame): DataFrame containing the onboarding file data.
            dict_obj (dict): Dictionary containing the required attributes for onboarding silver dataflow spec.
                Required attributes:
                    - onboarding_file_path (str): Path of the onboarding file.
                    - database (str): Name of the database.
                    - env (str): Environment name.
                    - silver_dataflowspec_table (str): Name of the silver dataflow spec table.
                    - silver_dataflowspec_path (str): Path of the silver dataflow spec file. if uc_enabled is False
                    - import_author (str): Name of the import author.
                    - version (str): Version of the dataflow spec.
                    - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "silver_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.silver_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("silver_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )
        silver_data_flow_spec_df = self.__get_silver_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )
        columns = StructType(
            [
                StructField("select_exp", ArrayType(StringType(), True), True),
                StructField(
                    "target_partition_cols", ArrayType(StringType(), True), True
                ),
                StructField("target_table", StringType(), True),
                StructField("where_clause", ArrayType(StringType(), True), True),
            ]
        )

        emp_rdd = []
        env = dict_obj["env"]
        silver_transformation_json_df = self.spark.createDataFrame(
            data=emp_rdd, schema=columns
        )
        silver_transformation_json_file = onboarding_df.select(
            f"silver_transformation_json_{env}"
        ).dropDuplicates()

        silver_transformation_json_files = silver_transformation_json_file.collect()
        for row in silver_transformation_json_files:
            silver_transformation_json_df = silver_transformation_json_df.union(
                self.spark.read.option("multiline", "true")
                .schema(columns)
                .json(row[f"silver_transformation_json_{env}"])
            )

        logger.info(silver_transformation_json_file)

        silver_data_flow_spec_df = silver_transformation_json_df.join(
            silver_data_flow_spec_df,
            silver_transformation_json_df.target_table
            == silver_data_flow_spec_df.targetDetails["table"],
        )
        silver_dataflow_spec_df = (
            silver_data_flow_spec_df.drop("target_table")  # .drop("path")
            .drop("target_partition_cols")
            .withColumnRenamed("select_exp", "selectExp")
            .withColumnRenamed("where_clause", "whereClause")
        )

        silver_dataflow_spec_df = self.__add_audit_columns(
            silver_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )

        silver_fields = [field.name for field in dataclasses.fields(SilverDataflowSpec)]
        silver_dataflow_spec_df = silver_dataflow_spec_df.select(silver_fields)
        database = dict_obj["database"]
        table = dict_obj["silver_dataflowspec_table"]

        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    silver_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                silver_dataflow_spec_df.write.mode("overwrite").format("delta").option(
                    "mergeSchema", "true"
                ).save(dict_obj["silver_dataflowspec_path"])
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["silver_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["silver_dataflowspec_path"]
                )
            logger.info("In Merge block for Silver")
            self.deltaPipelinesInternalTableOps.merge(
                silver_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_silver_dataflow_spec_tables()

    def onboard_bronze_dataflow_spec(self):
        """
        Onboard bronze dataflow spec.

        This function reads the onboarding file and creates bronze dataflow spec. It adds audit columns to the dataframe
        If overwrite is True, it overwrites the table or file with the new dataframe. If overwrite is False,
        it merges the new dataframe with the existing dataframe.
        dict_obj (dict): Dictionary containing the required attributes for onboarding bronze dataflow spec.
            Required attributes:
                - onboarding_file_path (str): Path of the onboarding file.
                - database (str): Name of the database.
                - env (str): Environment name.
                - bronze_dataflowspec_table (str): Name of the bronze dataflow spec table.
                - bronze_dataflowspec_path (str): Path of the bronze dataflow spec file. if uc_enabled is False
                - import_author (str): Name of the import author.
                - version (str): Version of the dataflow spec.
                - overwrite (str): Whether to overwrite the existing dataflow spec table/file or not.

        Args:
            None

        Returns:
            None
        """
        attributes = [
            "onboarding_file_path",
            "database",
            "env",
            "bronze_dataflowspec_table",
            "import_author",
            "version",
            "overwrite",
        ]
        dict_obj = self.bronze_dict_obj
        if self.uc_enabled:
            self.__validate_dict_attributes(attributes, dict_obj)
        else:
            attributes.append("bronze_dataflowspec_path")
            self.__validate_dict_attributes(attributes, dict_obj)

        onboarding_df = self.__get_onboarding_file_dataframe(
            dict_obj["onboarding_file_path"]
        )

        bronze_dataflow_spec_df = self.__get_bronze_dataflow_spec_dataframe(
            onboarding_df, dict_obj["env"]
        )

        bronze_dataflow_spec_df = self.__add_audit_columns(
            bronze_dataflow_spec_df,
            {
                "import_author": dict_obj["import_author"],
                "version": dict_obj["version"],
            },
        )
        bronze_fields = [field.name for field in dataclasses.fields(BronzeDataflowSpec)]
        bronze_dataflow_spec_df = bronze_dataflow_spec_df.select(bronze_fields)
        database = dict_obj["database"]
        table = dict_obj["bronze_dataflowspec_table"]
        if dict_obj["overwrite"] == "True":
            if self.uc_enabled:
                (
                    bronze_dataflow_spec_df.write.format("delta")
                    .mode("overwrite")
                    .option("mergeSchema", "true")
                    .saveAsTable(f"{database}.{table}")
                )
            else:
                (
                    bronze_dataflow_spec_df.write.mode("overwrite")
                    .format("delta")
                    .option("mergeSchema", "true")
                    .save(path=dict_obj["bronze_dataflowspec_path"])
                )
        else:
            if self.uc_enabled:
                original_dataflow_df = self.spark.read.format("delta").table(
                    f"{database}.{table}"
                )
            else:
                self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
                    database, table, dict_obj["bronze_dataflowspec_path"]
                )
                original_dataflow_df = self.spark.read.format("delta").load(
                    dict_obj["bronze_dataflowspec_path"]
                )

            logger.info("In Merge block for Bronze")
            self.deltaPipelinesInternalTableOps.merge(
                bronze_dataflow_spec_df,
                f"{database}.{table}",
                ["dataFlowId"],
                original_dataflow_df.columns,
            )
        if not self.uc_enabled:
            self.register_bronze_dataflow_spec_tables()

    def __delete_none(self, _dict):
        """Delete None values recursively from all of the dictionaries"""
        filtered = {k: v for k, v in _dict.items() if v is not None}
        _dict.clear()
        _dict.update(filtered)
        return _dict

    def __get_onboarding_file_dataframe(self, onboarding_file_path):
        onboarding_df = None
        if onboarding_file_path.lower().endswith(".json"):
            onboarding_df = self.spark.read.option("multiline", "true").json(
                onboarding_file_path
            )
            onboarding_df.show()
            self.onboard_file_type = "json"
            onboarding_df_dupes = (
                onboarding_df.groupBy("data_flow_id").count().filter("count > 1")
            )
            if len(onboarding_df_dupes.head(1)) > 0:
                onboarding_df_dupes.show()
                raise Exception("onboarding file have duplicated data_flow_ids! ")
        else:
            raise Exception(
                "Onboarding file format not supported! Please provide json file format"
            )
        return onboarding_df

    def __add_audit_columns(self, df, dict_obj):
        """Add_audit_columns method will add AuditColumns like version, dates, author.

        Args:
            df ([type]): [description]
            dict_obj ([type]): attributes = ["import_author", "version"]

        Returns:
            [type]: attributes = ["import_author", "version"]
        """
        attributes = ["import_author", "version"]
        self.__validate_dict_attributes(attributes, dict_obj)

        df = (
            df.withColumn("version", f.lit(dict_obj["version"]))
            .withColumn("createDate", f.current_timestamp())
            .withColumn("createdBy", f.lit(dict_obj["import_author"]))
            .withColumn("updateDate", f.current_timestamp())
            .withColumn("updatedBy", f.lit(dict_obj["import_author"]))
        )
        return df

    def __get_bronze_schema(self, metadata_file):
        """Get schema from metadafile in json format.

        Args:
            metadata_file ([string]): metadata schema file path
        """
        ddlSchemaStr = self.spark.read.text(
            paths=metadata_file, wholetext=True
        ).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        logger.info(spark_schema)
        schema = json.dumps(spark_schema.jsonValue())
        return schema

    def __validate_mandatory_fields(self, onboarding_row, mandatory_fields):
        for field in mandatory_fields:
            if not onboarding_row[field]:
                raise Exception(f"Missing field={field} in onboarding_row")

    def __get_bronze_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get bronze dataflow spec method will convert onboarding dataframe to Bronze Dataflowspec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "schema",
            "partitionColumns",
            "cdcApplyChanges",
            "applyChangesFromSnapshot",
            "dataQualityExpectations",
            "quarantineTargetDetails",
            "quarantineTableProperties",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy",
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("schema", StringType(), True),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("applyChangesFromSnapshot", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField(
                    "quarantineTargetDetails",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField(
                    "quarantineTableProperties",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True),
            ]
        )
        data = []
        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            "source_details",
            f"bronze_database_{env}",
            "bronze_table",
            "bronze_reader_options",
        ]  # , f"bronze_table_path_{env}"
        for onboarding_row in onboarding_rows:
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"bronze_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            bronze_data_flow_spec_id = onboarding_row["data_flow_id"]
            bronze_data_flow_spec_group = onboarding_row["data_flow_group"]
            if "source_format" not in onboarding_row:
                raise Exception(f"Source format not provided for row={onboarding_row}")

            source_format = onboarding_row["source_format"]
            if source_format.lower() not in [
                "cloudfiles",
                "eventhub",
                "kafka",
                "delta",
                "snapshot"
            ]:
                raise Exception(
                    f"Source format {source_format} not supported in DLT-META! row={onboarding_row}"
                )
            source_details, bronze_reader_config_options, schema = (
                self.get_bronze_source_details_reader_options_schema(
                    onboarding_row, env
                )
            )
            bronze_target_format = "delta"
            bronze_target_details = {
                "database": onboarding_row["bronze_database_{}".format(env)],
                "table": onboarding_row["bronze_table"],
            }
            if not self.uc_enabled:
                if f"bronze_table_path_{env}" in onboarding_row:
                    bronze_target_details["path"] = onboarding_row[f"bronze_table_path_{env}"]
                else:
                    raise Exception(f"bronze_table_path_{env} not provided in onboarding_row={onboarding_row}")
            bronze_table_properties = {}
            if (
                "bronze_table_properties" in onboarding_row
                and onboarding_row["bronze_table_properties"]
            ):
                bronze_table_properties = self.__delete_none(
                    onboarding_row["bronze_table_properties"].asDict()
                )

            partition_columns = [""]
            if (
                "bronze_partition_columns" in onboarding_row
                and onboarding_row["bronze_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["bronze_partition_columns"]:
                    partition_columns = onboarding_row["bronze_partition_columns"].split(",")
                else:
                    partition_columns = [onboarding_row["bronze_partition_columns"]]

            cluster_by = self.__get_cluster_by_properties(onboarding_row, bronze_table_properties,
                                                          "bronze_cluster_by")

            cdc_apply_changes = None
            if (
                "bronze_cdc_apply_changes" in onboarding_row
                and onboarding_row["bronze_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "bronze")
                cdc_apply_changes = json.dumps(
                    self.__delete_none(
                        onboarding_row["bronze_cdc_apply_changes"].asDict()
                    )
                )
            apply_changes_from_snapshot = None
            if ("bronze_apply_changes_from_snapshot" in onboarding_row
                    and onboarding_row["bronze_apply_changes_from_snapshot"]):
                self.__validate_apply_changes_from_snapshot(onboarding_row, "bronze")
                apply_changes_from_snapshot = json.dumps(
                    self.__delete_none(onboarding_row["bronze_apply_changes_from_snapshot"].asDict())
                )
            data_quality_expectations = None
            quarantine_target_details = {}
            quarantine_table_properties = {}
            if f"bronze_data_quality_expectations_json_{env}" in onboarding_row:
                bronze_data_quality_expectations_json = onboarding_row[
                    f"bronze_data_quality_expectations_json_{env}"
                ]
                if bronze_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        bronze_data_quality_expectations_json
                    )
                    if onboarding_row["bronze_quarantine_table"]:
                        quarantine_target_details, quarantine_table_properties = self.__get_quarantine_details(
                            env, onboarding_row
                        )

            append_flows, append_flows_schemas = self.get_append_flows_json(
                onboarding_row, "bronze", env
            )
            bronze_row = (
                bronze_data_flow_spec_id,
                bronze_data_flow_spec_group,
                source_format,
                source_details,
                bronze_reader_config_options,
                bronze_target_format,
                bronze_target_details,
                bronze_table_properties,
                schema,
                partition_columns,
                cdc_apply_changes,
                apply_changes_from_snapshot,
                data_quality_expectations,
                quarantine_target_details,
                quarantine_table_properties,
                append_flows,
                append_flows_schemas,
                cluster_by
            )
            data.append(bronze_row)
            # logger.info(bronze_parition_columns)

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)

        return data_flow_spec_rows_df

    def __get_cluster_by_properties(self, onboarding_row, table_properties, cluster_key):
        cluster_by = None
        if cluster_key in onboarding_row and onboarding_row[cluster_key]:
            if table_properties.get('pipelines.autoOptimize.zOrderCols', None) is not None:
                raise Exception(f"Can not support zOrder and cluster_by together at {cluster_key}")
            cluster_by = onboarding_row[cluster_key]
        return cluster_by

    def __get_quarantine_details(self, env, onboarding_row):
        quarantine_table_partition_columns = ""
        quarantine_target_details = {}
        quarantine_table_properties = {}
        quarantine_table_cluster_by = None
        if (
            "bronze_quarantine_table_partitions" in onboarding_row
            and onboarding_row["bronze_quarantine_table_partitions"]
        ):
            # Split if this is a list separated by commas
            if "," in onboarding_row["bronze_quarantine_table_partitions"]:
                quarantine_table_partition_columns = onboarding_row["bronze_quarantine_table_partitions"].split(",")
            else:
                quarantine_table_partition_columns = onboarding_row["bronze_quarantine_table_partitions"]
        if (
            "bronze_quarantine_table_properties" in onboarding_row
            and onboarding_row["bronze_quarantine_table_properties"]
        ):
            quarantine_table_properties = self.__delete_none(
                onboarding_row["bronze_quarantine_table_properties"].asDict()
            )

        quarantine_table_cluster_by = self.__get_cluster_by_properties(onboarding_row, quarantine_table_properties,
                                                                       "bronze_quarantine_table_cluster_by")
        if (
            f"bronze_database_quarantine_{env}" in onboarding_row
            and onboarding_row[f"bronze_database_quarantine_{env}"]
        ):
            quarantine_target_details = {"database": onboarding_row[f"bronze_database_quarantine_{env}"],
                                         "table": onboarding_row["bronze_quarantine_table"],
                                         "partition_columns": quarantine_table_partition_columns,
                                         "cluster_by": quarantine_table_cluster_by
                                         }
        if not self.uc_enabled and f"bronze_quarantine_table_path_{env}" in onboarding_row:
            quarantine_target_details["path"] = onboarding_row[f"bronze_quarantine_table_path_{env}"]

        return quarantine_target_details, quarantine_table_properties

    def get_append_flows_json(self, onboarding_row, layer, env):
        append_flows = None
        append_flows_schema = {}
        if (
            f"{layer}_append_flows" in onboarding_row
            and onboarding_row[f"{layer}_append_flows"]
        ):
            self.__validate_append_flow(onboarding_row, layer)
            json_append_flows = onboarding_row[f"{layer}_append_flows"]
            from pyspark.sql.types import Row

            af_list = []
            for json_append_flow in json_append_flows:
                json_append_flow = json_append_flow.asDict()
                append_flow_map = {}
                for key in json_append_flow.keys():
                    if isinstance(json_append_flow[key], Row):
                        fs = json_append_flow[key].__fields__
                        mp = {}
                        for ff in fs:
                            if f"source_path_{env}" == ff:
                                mp["path"] = json_append_flow[key][f"{ff}"]
                            elif "source_schema_path" == ff:
                                source_schema_path = json_append_flow[key][f"{ff}"]
                                if source_schema_path:
                                    schema = self.__get_bronze_schema(
                                        source_schema_path
                                    )
                                    append_flows_schema[json_append_flow["name"]] = (
                                        schema
                                    )
                            else:
                                mp[f"{ff}"] = json_append_flow[key][f"{ff}"]
                        append_flow_map[key] = self.__delete_none(mp)
                    else:
                        append_flow_map[key] = json_append_flow[key]
                af_list.append(self.__delete_none(append_flow_map))
            append_flows = json.dumps(af_list)
        return append_flows, append_flows_schema

    def __validate_apply_changes(self, onboarding_row, layer):
        cdc_apply_changes = onboarding_row[f"{layer}_cdc_apply_changes"]
        json_cdc_apply_changes = self.__delete_none(cdc_apply_changes.asDict())
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(
            DataflowSpecUtils.cdc_applychanges_api_attributes
        ).difference(payload_keys)
        logger.info(
            f"""missing cdc payload keys:{missing_cdc_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
            payload_keys
        ):
            missing_mandatory_attr = set(
                DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes
            ) - set(payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_cdc_apply_changes = {missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_cdc_apply_changes atrributes
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

    def __validate_apply_changes_from_snapshot(self, onboarding_row, layer):
        apply_changes_from_snapshot = onboarding_row[f"{layer}_apply_changes_from_snapshot"]
        json_apply_changes_from_snapshot = self.__delete_none(apply_changes_from_snapshot.asDict())
        logger.info(f"actual applyChangesFromSnapshot={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = (
            set(DataflowSpecUtils.apply_changes_from_snapshot_api_attributes).difference(payload_keys)
        )
        logger.info(
            f"""missing applyChangesFromSnapshot payload keys:{missing_apply_changes_from_snapshot_payload_keys}
                for onboarding row = {onboarding_row}"""
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(
                f"""mandatory missing atrributes for {layer}_apply_changes_from_snapshot = {
                missing_mandatory_attr}
                for onboarding row = {onboarding_row}"""
            )
        else:
            logger.info(
                f"""all mandatory {layer}_apply_changes_from_snapshot atrributes
                 {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

    def get_bronze_source_details_reader_options_schema(self, onboarding_row, env):
        """Get bronze source reader options.

        Args:
            onboarding_row ([type]): [description]

        Returns:
            [type]: [description]
        """
        source_details = {}
        bronze_reader_config_options = {}
        schema = None
        source_format = onboarding_row["source_format"]
        bronze_reader_options_json = (
            onboarding_row["bronze_reader_options"]
            if "bronze_reader_options" in onboarding_row
            else {}
        )
        if bronze_reader_options_json:
            bronze_reader_config_options = self.__delete_none(
                bronze_reader_options_json.asDict()
            )
        source_details_json = onboarding_row["source_details"]
        if source_details_json:
            source_details_file = self.__delete_none(source_details_json.asDict())
            if (source_format.lower() == "cloudfiles"
                    or source_format.lower() == "delta"
                    or source_format.lower() == "snapshot"):
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                if "source_database" in source_details_file:
                    source_details["source_database"] = source_details_file[
                        "source_database"
                    ]
                if "source_table" in source_details_file:
                    source_details["source_table"] = source_details_file["source_table"]
                if "source_metadata" in source_details_file:
                    source_metadata_dict = self.__delete_none(
                        source_details_file["source_metadata"].asDict()
                    )
                    if "select_metadata_cols" in source_metadata_dict:
                        select_metadata_cols = self.__delete_none(
                            source_metadata_dict["select_metadata_cols"].asDict()
                        )
                        source_metadata_dict["select_metadata_cols"] = select_metadata_cols
                    source_details["source_metadata"] = json.dumps(
                        self.__delete_none(source_metadata_dict)
                    )
            if source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
            elif source_format.lower() == "eventhub" or source_format.lower() == "kafka":
                source_details = source_details_file
            elif source_format.lower() == "snapshot":
                snapshot_format = source_details_file.get("snapshot_format", None)
                if snapshot_format is None:
                    raise Exception("snapshot_format is missing in the source_details")
                source_details["snapshot_format"] = snapshot_format
                if f"source_path_{env}" in source_details_file:
                    source_details["path"] = source_details_file[f"source_path_{env}"]
                else:
                    raise Exception(f"source_path_{env} is missing in the source_details")
            if "source_schema_path" in source_details_file:
                source_schema_path = source_details_file["source_schema_path"]
                if source_schema_path:
                    if self.bronze_schema_mapper is not None:
                        schema = self.bronze_schema_mapper(
                            source_schema_path, self.spark
                        )
                    else:
                        schema = self.__get_bronze_schema(source_schema_path)
                else:
                    logger.info(f"no input schema provided for row={onboarding_row}")
                logger.info("spark_schmea={}".format(schema))

        return source_details, bronze_reader_config_options, schema

    def __validate_append_flow(self, onboarding_row, layer):
        append_flows = onboarding_row[f"{layer}_append_flows"]
        for append_flow in append_flows:
            json_append_flow = append_flow.asDict()
            logger.info(f"actual appendFlow={json_append_flow}")
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = set(
                DataflowSpecUtils.append_flow_api_attributes_defaults
            ).difference(payload_keys)
            logger.info(
                f"""missing append flow payload keys:{missing_append_flow_payload_keys}
                    for onboarding row = {onboarding_row}"""
            )
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(
                payload_keys
            ):
                missing_mandatory_attr = set(
                    DataflowSpecUtils.append_flow_mandatory_attributes
                ) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(
                    f"""mandatory missing atrributes for {layer}_append_flow = {missing_mandatory_attr}
                    for onboarding row = {onboarding_row}"""
                )
            else:
                logger.info(
                    f"""all mandatory {layer}_append_flow atrributes
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

    def __get_data_quality_expecations(self, json_file_path):
        """Get Data Quality expections from json file.

        Args:
            json_file_path ([type]): [description]
        """
        json_string = None
        if json_file_path and json_file_path.endswith(".json"):
            expectations_df = self.spark.read.text(json_file_path, wholetext=True)
            expectations_arr = expectations_df.collect()
            if len(expectations_arr) == 1:
                json_string = expectations_df.collect()[0]["value"]
        return json_string

    def __get_silver_dataflow_spec_dataframe(self, onboarding_df, env):
        """Get silver_dataflow_spec method transform onboarding dataframe to silver dataflowSpec dataframe.

        Args:
            onboarding_df ([type]): [description]
            spark (SparkSession): [description]

        Returns:
            [type]: [description]
        """
        data_flow_spec_columns = [
            "dataFlowId",
            "dataFlowGroup",
            "sourceFormat",
            "sourceDetails",
            "readerConfigOptions",
            "targetFormat",
            "targetDetails",
            "tableProperties",
            "partitionColumns",
            "cdcApplyChanges",
            "dataQualityExpectations",
            "appendFlows",
            "appendFlowsSchemas",
            "clusterBy"
        ]
        data_flow_spec_schema = StructType(
            [
                StructField("dataFlowId", StringType(), True),
                StructField("dataFlowGroup", StringType(), True),
                StructField("sourceFormat", StringType(), True),
                StructField(
                    "sourceDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "readerConfigOptions",
                    MapType(StringType(), StringType(), True),
                    True,
                ),
                StructField("targetFormat", StringType(), True),
                StructField(
                    "targetDetails", MapType(StringType(), StringType(), True), True
                ),
                StructField(
                    "tableProperties", MapType(StringType(), StringType(), True), True
                ),
                StructField("partitionColumns", ArrayType(StringType(), True), True),
                StructField("cdcApplyChanges", StringType(), True),
                StructField("dataQualityExpectations", StringType(), True),
                StructField("appendFlows", StringType(), True),
                StructField("appendFlowsSchemas", MapType(StringType(), StringType(), True), True),
                StructField("clusterBy", ArrayType(StringType(), True), True)
            ]
        )
        data = []

        onboarding_rows = onboarding_df.collect()
        mandatory_fields = [
            "data_flow_id",
            "data_flow_group",
            f"silver_database_{env}",
            "silver_table",
            f"silver_transformation_json_{env}",
        ]  # f"silver_table_path_{env}",

        for onboarding_row in onboarding_rows:
            try:
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            except ValueError:
                mandatory_fields.append(f"silver_table_path_{env}")
                self.__validate_mandatory_fields(onboarding_row, mandatory_fields)
            silver_data_flow_spec_id = onboarding_row["data_flow_id"]
            silver_data_flow_spec_group = onboarding_row["data_flow_group"]
            silver_reader_config_options = {}

            silver_target_format = "delta"

            bronze_target_details = {
                "database": onboarding_row["bronze_database_{}".format(env)],
                "table": onboarding_row["bronze_table"],
            }
            silver_target_details = {
                "database": onboarding_row["silver_database_{}".format(env)],
                "table": onboarding_row["silver_table"],
            }

            if not self.uc_enabled:
                bronze_target_details["path"] = onboarding_row[
                    f"bronze_table_path_{env}"
                ]
                silver_target_details["path"] = onboarding_row[
                    f"silver_table_path_{env}"
                ]

            silver_table_properties = {}
            if (
                "silver_table_properties" in onboarding_row
                and onboarding_row["silver_table_properties"]
            ):
                silver_table_properties = self.__delete_none(
                    onboarding_row["silver_table_properties"].asDict()
                )

            silver_parition_columns = [""]
            if (
                "silver_partition_columns" in onboarding_row
                and onboarding_row["silver_partition_columns"]
            ):
                # Split if this is a list separated by commas
                if "," in onboarding_row["silver_partition_columns"]:
                    silver_parition_columns = onboarding_row["silver_partition_columns"].split(",")
                else:
                    silver_parition_columns = [onboarding_row["silver_partition_columns"]]

            silver_cluster_by = self.__get_cluster_by_properties(onboarding_row, silver_table_properties,
                                                                 "silver_cluster_by")

            silver_cdc_apply_changes = None
            if (
                "silver_cdc_apply_changes" in onboarding_row
                and onboarding_row["silver_cdc_apply_changes"]
            ):
                self.__validate_apply_changes(onboarding_row, "silver")
                silver_cdc_apply_changes_row = onboarding_row[
                    "silver_cdc_apply_changes"
                ]
                if self.onboard_file_type == "json":
                    silver_cdc_apply_changes = json.dumps(
                        self.__delete_none(silver_cdc_apply_changes_row.asDict())
                    )
            data_quality_expectations = None
            if f"silver_data_quality_expectations_json_{env}" in onboarding_row:
                silver_data_quality_expectations_json = onboarding_row[
                    f"silver_data_quality_expectations_json_{env}"
                ]
                if silver_data_quality_expectations_json:
                    data_quality_expectations = self.__get_data_quality_expecations(
                        silver_data_quality_expectations_json
                    )
            append_flows, append_flow_schemas = self.get_append_flows_json(
                onboarding_row, layer="silver", env=env
            )
            silver_row = (
                silver_data_flow_spec_id,
                silver_data_flow_spec_group,
                "delta",
                bronze_target_details,
                silver_reader_config_options,
                silver_target_format,
                silver_target_details,
                silver_table_properties,
                silver_parition_columns,
                silver_cdc_apply_changes,
                data_quality_expectations,
                append_flows,
                append_flow_schemas,
                silver_cluster_by
            )
            data.append(silver_row)
            logger.info(f"silver_data ==== {data}")

        data_flow_spec_rows_df = self.spark.createDataFrame(
            data, data_flow_spec_schema
        ).toDF(*data_flow_spec_columns)
        return data_flow_spec_rows_df
