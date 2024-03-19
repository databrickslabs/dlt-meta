"""Dataflow Spec related utilities."""
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)


@dataclass
class BronzeDataflowSpec:
    """A schema to hold a dataflow spec used for writing to the bronze layer."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: map
    readerConfigOptions: map
    targetFormat: str
    targetDetails: map
    tableProperties: map
    schema: str
    partitionColumns: list
    cdcApplyChanges: str
    dataQualityExpectations: str
    quarantineTargetDetails: map
    quarantineTableProperties: map
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str


@dataclass
class SilverDataflowSpec:
    """A schema to hold a dataflow spec used for writing to the silver layer."""

    dataFlowId: str
    dataFlowGroup: str
    sourceFormat: str
    sourceDetails: map
    readerConfigOptions: map
    targetFormat: str
    targetDetails: map
    tableProperties: map
    selectExp: list
    whereClause: list
    partitionColumns: list
    cdcApplyChanges: str
    dataQualityExpectations: str
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str


@dataclass
class CDCApplyChanges:
    """CDC ApplyChanges structure."""

    keys: list
    sequence_by: str
    where: str
    ignore_null_updates: bool
    apply_as_deletes: str
    apply_as_truncates: str
    column_list: list
    except_column_list: list
    scd_type: str
    track_history_column_list: list
    track_history_except_column_list: list


class DataflowSpecUtils:
    """A collection of methods for working with DataflowSpec."""

    cdc_applychanges_api_mandatory_attributes = ["keys", "sequence_by", "scd_type"]
    cdc_applychanges_api_attributes = [
        "keys",
        "sequence_by",
        "where",
        "ignore_null_updates",
        "apply_as_deletes",
        "apply_as_truncates",
        "column_list",
        "except_column_list",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list"

    ]
    cdc_applychanges_api_attributes_defaults = {
        "where": None,
        "ignore_null_updates": False,
        "apply_as_deletes": None,
        "apply_as_truncates": None,
        "column_list": None,
        "except_column_list": None,
        "track_history_column_list": None,
        "track_history_except_column_list": None

    }

    @staticmethod
    def _get_dataflow_spec(
        spark: SparkSession,
        layer: str,
        dataflow_spec_df: DataFrame = None,
        group: str = None,
        dataflow_ids: str = None,
    ) -> DataFrame:
        """Get DataflowSpec for given parameters.

        Can be configured using spark config values, used for optionally filtering
        the returned data to a group or list of DataflowIDs
        """
        if not group:
            group = spark.conf.get(f"{layer}.group", None)

        if not dataflow_ids:
            dataflow_ids = spark.conf.get(f"{layer}.dataflowIds", None)

        if not dataflow_spec_df:
            dataflow_spec_table = spark.conf.get(f"{layer}.dataflowspecTable")
            dataflow_spec_df = spark.read.table(dataflow_spec_table)

        if group or dataflow_ids:
            dataflow_spec_df = dataflow_spec_df.where(
                col("dataFlowGroup") == lit(group) if group else f"dataFlowId in ('{dataflow_ids}')"
            )

        version_history = Window.partitionBy(col("dataFlowGroup"), col("dataFlowId")).orderBy(col("version").desc())
        dataflow_spec_df = (
            dataflow_spec_df.withColumn("row_num", row_number().over(version_history))
            .where(col("row_num") == lit(1))  # latest version
            .drop(col("row_num"))
        )

        return dataflow_spec_df

    @staticmethod
    def get_bronze_dataflow_spec(spark) -> List[BronzeDataflowSpec]:
        """Get bronze dataflow spec."""
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(spark, "bronze")
        dataflow_spec_rows = DataflowSpecUtils._get_dataflow_spec(spark, "bronze").collect()
        bronze_dataflow_spec_list: list[BronzeDataflowSpec] = []
        for row in dataflow_spec_rows:
            bronze_dataflow_spec_list.append(BronzeDataflowSpec(**row.asDict()))
        logger.info(f"bronze_dataflow_spec_list={bronze_dataflow_spec_list}")
        return bronze_dataflow_spec_list

    @staticmethod
    def get_silver_dataflow_spec(spark) -> List[SilverDataflowSpec]:
        """Get silver dataflow spec list."""
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(spark, "silver")

        dataflow_spec_rows = DataflowSpecUtils._get_dataflow_spec(spark, "silver").collect()
        silver_dataflow_spec_list: list[SilverDataflowSpec] = []
        for row in dataflow_spec_rows:
            silver_dataflow_spec_list.append(SilverDataflowSpec(**row.asDict()))
        return silver_dataflow_spec_list

    @staticmethod
    def check_spark_dataflowpipeline_conf_params(spark, layer_arg):
        """Check dataflowpipine config params."""
        layer = spark.conf.get("layer", None)
        if layer is None:
            raise Exception(
                f"""parameter {layer_arg} is missing in spark.conf.
                 Please set spark.conf.set({layer_arg},'silver') """
            )
        dataflow_spec_table = spark.conf.get(f"{layer}.dataflowspecTable", None)
        if dataflow_spec_table is None:
            raise Exception(
                f"""parameter {layer_arg}.dataflowspecTable is missing in sparkConf
                Please set spark.conf.set('{layer_arg}.dataflowspecTable'='database.dataflowSpecTableName')"""
            )

        group = spark.conf.get(f"{layer}.group", None)
        dataflow_ids = spark.conf.get(f"{layer}.dataflowIds", None)

        if group is None and dataflow_ids is None:
            raise Exception(
                f"""please provide {layer}.group or {layer}.dataflowIds in spark.conf
                 Please set spark.conf.set('{layer}.group'='groupName')
                 OR
                 spark.conf.set('{layer}.dataflowIds'='comma seperated dataflowIds')
                 """
            )

    @staticmethod
    def get_partition_cols(partition_columns):
        """Get partition columns."""
        partition_cols = None
        if partition_columns:
            if len(partition_columns) == 1:
                if partition_columns[0] == "" or partition_columns[0].strip() == "":
                    partition_cols = None
                else:
                    partition_cols = partition_columns
            else:
                partition_cols = list(filter(None, partition_columns))
        return partition_cols

    @staticmethod
    def get_cdc_apply_changes(cdc_apply_changes) -> CDCApplyChanges:
        """Get CDC Apply changes metadata."""
        logger.info(cdc_apply_changes)
        json_cdc_apply_changes = json.loads(cdc_apply_changes)
        logger.info(f"actual mergeInfo={json_cdc_apply_changes}")
        payload_keys = json_cdc_apply_changes.keys()
        missing_cdc_payload_keys = set(DataflowSpecUtils.cdc_applychanges_api_attributes).difference(payload_keys)
        logger.info(f"missing cdc payload keys:{missing_cdc_payload_keys}")
        if set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes) - set(
                payload_keys
            )
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for merge info")
        else:
            logger.info(
                f"""all mandatory keys
                {DataflowSpecUtils.cdc_applychanges_api_mandatory_attributes} exists"""
            )

        for missing_cdc_payload_key in missing_cdc_payload_keys:
            json_cdc_apply_changes[
                missing_cdc_payload_key
            ] = DataflowSpecUtils.cdc_applychanges_api_attributes_defaults[missing_cdc_payload_key]

        logger.info(f"final mergeInfo={json_cdc_apply_changes}")
        return CDCApplyChanges(**json_cdc_apply_changes)

    @staticmethod
    def get_schema_json(spark, dataflow_spec):
        """Get schema json from dataflow spec."""
        source_path = dataflow_spec.sourceDetails["path"]
        reader_config_options = dataflow_spec.readerConfigOptions
        schema_json = None
        if reader_config_options["cloudFiles.format"].lower() == "parquet":
            schema = spark.read.options(**reader_config_options).parquet(source_path).limit(100).schema
            schema_json = schema.jsonValue()
        elif reader_config_options["cloudFiles.format"].lower() == "json":
            schema = (
                spark.read.options(**reader_config_options)
                .json(source_path)
                .limit(100)
                .schema
            )
            schema_json = schema.jsonValue()
        elif reader_config_options["cloudFiles.format"].lower() == "csv":
            json_schema_string_value = dataflow_spec.schema
            schema_json = json.loads(json_schema_string_value)
        else:
            raise Exception(
                f"""Dataflow schema not supported for type = {type(dataflow_spec.dataFlowId)}!
                    Please correct dataflowSpec or implement schema reader options"""
            )
        return schema_json
