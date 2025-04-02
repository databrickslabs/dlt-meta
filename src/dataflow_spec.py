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
    applyChangesFromSnapshot: str
    dataQualityExpectations: str
    quarantineTargetDetails: map
    quarantineTableProperties: map
    appendFlows: str
    appendFlowsSchemas: map
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: list
    sinks: str


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
    appendFlows: str
    appendFlowsSchemas: map
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str
    clusterBy: list
    sinks: str


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
    flow_name: str
    once: bool
    ignore_null_updates_column_list: list
    ignore_null_updates_except_column_list: list


@dataclass
class ApplyChangesFromSnapshot:
    """CDC ApplyChangesFromSnapshot structure."""
    keys: list
    scd_type: str
    track_history_column_list: list
    track_history_except_column_list: list


@dataclass
class AppendFlow:
    """Append Flow structure."""
    name: str
    comment: str
    create_streaming_table: bool
    source_format: str
    source_details: map
    reader_options: map
    spark_conf: map
    once: bool


@dataclass
class DLTSink:
    name: str
    format: str
    options: map
    select_exp: list
    where_clause: str


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
        "track_history_except_column_list",
        "flow_name",
        "once",
        "ignore_null_updates_column_list",
        "ignore_null_updates_except_column_list"
    ]

    cdc_applychanges_api_attributes_defaults = {
        "where": None,
        "ignore_null_updates": False,
        "apply_as_deletes": None,
        "apply_as_truncates": None,
        "column_list": None,
        "except_column_list": None,
        "track_history_column_list": None,
        "track_history_except_column_list": None,
        "flow_name": None,
        "once": False,
        "ignore_null_updates_column_list": None,
        "ignore_null_updates_except_column_list": None
    }

    append_flow_mandatory_attributes = ["name", "source_format", "create_streaming_table", "source_details"]
    sink_mandatory_attributes = ["name", "format", "options"]
    supported_sink_formats = ["delta", "kafka", "eventhub"]

    append_flow_api_attributes_defaults = {
        "comment": None,
        "create_streaming_table": False,
        "reader_options": None,
        "spark_conf": None,
        "once": False
    }

    sink_attributes_defaults = {
        "select_exp": None,
        "where_clause": None
    }

    additional_bronze_df_columns = [
        "appendFlows",
        "appendFlowsSchemas",
        "applyChangesFromSnapshot",
        "clusterBy",
        "sinks"
    ]
    additional_silver_df_columns = [
        "dataQualityExpectations",
        "appendFlows",
        "appendFlowsSchemas",
        "clusterBy",
        "sinks"
    ]
    additional_cdc_apply_changes_columns = ["flow_name", "once"]
    apply_changes_from_snapshot_api_attributes = [
        "keys",
        "scd_type",
        "track_history_column_list",
        "track_history_except_column_list"
    ]
    apply_changes_from_snapshot_api_mandatory_attributes = ["keys", "scd_type"]
    additional_apply_changes_from_snapshot_columns = ["track_history_column_list", "track_history_except_column_list"]
    apply_changes_from_snapshot_api_attributes_defaults = {
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
                col("dataFlowGroup") == lit(group) if group else f"dataFlowId in ({dataflow_ids})"
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
            target_row = DataflowSpecUtils.populate_additional_df_cols(
                row.asDict(),
                DataflowSpecUtils.additional_bronze_df_columns
            )
            bronze_dataflow_spec_list.append(BronzeDataflowSpec(**target_row))
        logger.info(f"bronze_dataflow_spec_list={bronze_dataflow_spec_list}")
        return bronze_dataflow_spec_list

    @staticmethod
    def populate_additional_df_cols(onboarding_row_dict, additional_columns):
        for column in additional_columns:
            if column not in onboarding_row_dict.keys():
                onboarding_row_dict[column] = None
        return onboarding_row_dict

    @staticmethod
    def get_silver_dataflow_spec(spark) -> List[SilverDataflowSpec]:
        """Get silver dataflow spec list."""
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(spark, "silver")

        dataflow_spec_rows = DataflowSpecUtils._get_dataflow_spec(spark, "silver").collect()
        silver_dataflow_spec_list: list[SilverDataflowSpec] = []
        for row in dataflow_spec_rows:
            target_row = DataflowSpecUtils.populate_additional_df_cols(
                row.asDict(),
                DataflowSpecUtils.additional_silver_df_columns
            )
            silver_dataflow_spec_list.append(SilverDataflowSpec(**target_row))
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
        dataflow_spec_table = spark.conf.get(f"{layer_arg}.dataflowspecTable", None)
        if dataflow_spec_table is None:
            raise Exception(
                f"""parameter {layer_arg}.dataflowspecTable is missing in sparkConf
                Please set spark.conf.set('{layer_arg}.dataflowspecTable'='database.dataflowSpecTableName')"""
            )

        group = spark.conf.get(f"{layer_arg}.group", None)
        dataflow_ids = spark.conf.get(f"{layer_arg}.dataflowIds", None)

        if group is None and dataflow_ids is None:
            raise Exception(
                f"""please provide {layer_arg}.group or {layer}.dataflowIds in spark.conf
                 Please set spark.conf.set('{layer}.group'='groupName')
                 OR
                 spark.conf.set('{layer_arg}.dataflowIds'='comma seperated dataflowIds')
                 """
            )

    @staticmethod
    def get_partition_cols(partition_columns):
        """Get partition columns."""
        partition_cols = None
        if partition_columns:
            if isinstance(partition_columns, str):
                # quarantineTableProperties cluster by
                partition_cols = partition_columns.split(',')

            else:
                if len(partition_columns) == 1:
                    if partition_columns[0] == "" or partition_columns[0].strip() == "":
                        partition_cols = None
                    else:
                        partition_cols = partition_columns
                else:
                    partition_cols = list(filter(None, partition_columns))
        return partition_cols

    @staticmethod
    def get_apply_changes_from_snapshot(apply_changes_from_snapshot) -> ApplyChangesFromSnapshot:
        """Get Apply changes from snapshot metadata."""
        logger.info(apply_changes_from_snapshot)
        json_apply_changes_from_snapshot = json.loads(apply_changes_from_snapshot)
        logger.info(f"actual mergeInfo={json_apply_changes_from_snapshot}")
        payload_keys = json_apply_changes_from_snapshot.keys()
        missing_apply_changes_from_snapshot_payload_keys = set(
            DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes).difference(payload_keys)
        logger.info(
            f"missing apply changes from snapshot payload keys:"
            f"{missing_apply_changes_from_snapshot_payload_keys}"
        )
        if set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(payload_keys):
            missing_mandatory_attr = set(DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes) - set(
                payload_keys)
            logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
            raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for merge info")
        else:
            logger.info(
                f"""all mandatory keys
                {DataflowSpecUtils.apply_changes_from_snapshot_api_mandatory_attributes} exists"""
            )

        for missing_apply_changes_from_snapshot_payload_key in missing_apply_changes_from_snapshot_payload_keys:
            json_apply_changes_from_snapshot[
                missing_apply_changes_from_snapshot_payload_key
            ] = DataflowSpecUtils.cdc_applychanges_api_attributes_defaults[
                missing_apply_changes_from_snapshot_payload_key]

        logger.info(f"final mergeInfo={json_apply_changes_from_snapshot}")
        json_apply_changes_from_snapshot = DataflowSpecUtils.populate_additional_df_cols(
            json_apply_changes_from_snapshot,
            DataflowSpecUtils.additional_apply_changes_from_snapshot_columns
        )
        return ApplyChangesFromSnapshot(**json_apply_changes_from_snapshot)

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
        json_cdc_apply_changes = DataflowSpecUtils.populate_additional_df_cols(
            json_cdc_apply_changes,
            DataflowSpecUtils.additional_cdc_apply_changes_columns
        )
        return CDCApplyChanges(**json_cdc_apply_changes)

    @staticmethod
    def get_append_flows(append_flows) -> list[AppendFlow]:
        """Get append flow metadata."""
        logger.info(append_flows)
        json_append_flows = json.loads(append_flows)
        logger.info(f"actual appendFlow={json_append_flows}")
        list_append_flows = []
        for json_append_flow in json_append_flows:
            payload_keys = json_append_flow.keys()
            missing_append_flow_payload_keys = (
                set(DataflowSpecUtils.append_flow_api_attributes_defaults)
                .difference(payload_keys)
            )
            logger.info(f"missing append flow payload keys:{missing_append_flow_payload_keys}")
            if set(DataflowSpecUtils.append_flow_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = (
                    set(DataflowSpecUtils.append_flow_mandatory_attributes)
                    - set(payload_keys)
                )
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for append flow")
            else:
                logger.info(
                    f"""all mandatory keys
                    {DataflowSpecUtils.append_flow_mandatory_attributes} exists"""
                )

            for missing_append_flow_payload_key in missing_append_flow_payload_keys:
                json_append_flow[
                    missing_append_flow_payload_key
                ] = DataflowSpecUtils.append_flow_api_attributes_defaults[missing_append_flow_payload_key]

            logger.info(f"final appendFlow={json_append_flow}")
            list_append_flows.append(AppendFlow(**json_append_flow))
        return list_append_flows

    def get_db_utils(spark):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)

    def get_sinks(sinks, spark) -> list[DLTSink]:
        """Get sink metadata."""
        logger.info(sinks)
        json_sinks = json.loads(sinks)
        dlt_sinks = []
        for json_sink in json_sinks:
            logger.info(f"actual sink={json_sink}")
            payload_keys = json_sink.keys()
            missing_sink_payload_keys = set(DataflowSpecUtils.sink_mandatory_attributes).difference(payload_keys)
            logger.info(f"missing sink payload keys:{missing_sink_payload_keys}")
            if set(DataflowSpecUtils.sink_mandatory_attributes) - set(payload_keys):
                missing_mandatory_attr = set(DataflowSpecUtils.sink_mandatory_attributes) - set(payload_keys)
                logger.info(f"mandatory missing keys= {missing_mandatory_attr}")
                raise Exception(f"mandatory missing keys= {missing_mandatory_attr} for sink")
            else:
                logger.info(
                    f"""all mandatory keys
                    {DataflowSpecUtils.sink_mandatory_attributes} exists"""
                )
            format = json_sink['format']
            if format not in DataflowSpecUtils.supported_sink_formats:
                raise Exception(f"Unsupported sink format: {format}")
            if 'options' in json_sink.keys():
                json_sink['options'] = json.loads(json_sink['options'])
            if format == "kafka" and 'options' in json_sink.keys():
                kafka_options_json = json_sink['options']
                dbutils = DataflowSpecUtils.get_db_utils(spark)
                if "kafka_sink_servers_secret_scope_name" in kafka_options_json.keys() and \
                   "kafka_sink_servers_secret_scope_key" in kafka_options_json.keys():
                    kbs_secrets_scope = kafka_options_json["kafka_sink_servers_secret_scope_name"]
                    kbs_secrets_key = kafka_options_json["kafka_sink_servers_secret_scope_key"]
                    json_sink['options']["kafka.bootstrap.servers"] = \
                        dbutils.secrets.get(kbs_secrets_scope, kbs_secrets_key)
                    del json_sink['options']['kafka_sink_servers_secret_scope_name']
                    del json_sink['options']['kafka_sink_servers_secret_scope_key']
                    ssl_truststore_location = kafka_options_json.get("kafka.ssl.truststore.location", None)
                    ssl_keystore_location = kafka_options_json.get("kafka.ssl.keystore.location", None)
                    if ssl_truststore_location and ssl_keystore_location:
                        truststore_scope = kafka_options_json.get("kafka.ssl.truststore.secrets.scope", None)
                        truststore_key = kafka_options_json.get("kafka.ssl.truststore.secrets.key", None)
                        keystore_scope = kafka_options_json.get("kafka.ssl.keystore.secrets.scope", None)
                        keystore_key = kafka_options_json.get("kafka.ssl.keystore.secrets.key", None)
                        if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                            dbutils = DataflowSpecUtils.get_db_utils(spark)
                            json_sink['options']['kafka.ssl.truststore.location'] = ssl_truststore_location
                            json_sink['options']['kafka.ssl.keystore.location'] = ssl_keystore_location
                            json_sink['options']['kafka.ssl.keystore.password'] = dbutils.secrets.get(
                                keystore_scope, keystore_key
                            )
                            json_sink['options']['kafka.ssl.truststore.password'] = dbutils.secrets.get(
                                truststore_scope, truststore_key)
                            del json_sink['options']['kafka.ssl.truststore.secrets.scope']
                            del json_sink['options']['kafka.ssl.truststore.secrets.key']
                            del json_sink['options']['kafka.ssl.keystore.secrets.scope']
                            del json_sink['options']['kafka.ssl.keystore.secrets.key']
                        else:
                            params = ["kafka.ssl.truststore.secrets.scope",
                                      "kafka.ssl.truststore.secrets.key",
                                      "kafka.ssl.keystore.secrets.scope",
                                      "kafka.ssl.keystore.secrets.key"
                                      ]
                            raise Exception(
                                f"Kafka ssl required params are: {params}! provided options are :{kafka_options_json}"
                            )
            if format == "eventhub" and 'options' in json_sink.keys():
                dbutils = DataflowSpecUtils.get_db_utils(spark)
                kafka_options_json = json_sink['options']
                eh_namespace = kafka_options_json["eventhub.namespace"]
                eh_port = kafka_options_json["eventhub.port"]
                eh_name = kafka_options_json["eventhub.name"]
                eh_shared_key_name = kafka_options_json["eventhub.accessKeyName"]
                secret_name = kafka_options_json["eventhub.accessKeySecretName"]
                if not secret_name:
                    # set default value if "eventhub.accessKeySecretName" is not specified
                    secret_name = eh_shared_key_name
                secret_scope = kafka_options_json.get("eventhub.secretsScopeName")
                eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
                eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
                eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
                eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
                sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

                eh_conn_options = {
                    "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
                    "topic": eh_name,
                    "kafka.sasl.mechanism": "PLAIN",
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.jaas.config": sasl_config
                }
                json_sink['options']['kafka.bootstrap.servers'] = eh_conn_options['kafka.bootstrap.servers']
                json_sink['options']['kafka.sasl.mechanism'] = eh_conn_options['kafka.sasl.mechanism']
                json_sink['options']['kafka.security.protocol'] = eh_conn_options['kafka.security.protocol']
                json_sink['options']['kafka.sasl.jaas.config'] = eh_conn_options['kafka.sasl.jaas.config']
                json_sink['options']['topic'] = eh_conn_options['topic']
                del json_sink['options']['eventhub.namespace']
                del json_sink['options']['eventhub.port']
                del json_sink['options']['eventhub.name']
                del json_sink['options']['eventhub.accessKeyName']
                del json_sink['options']['eventhub.accessKeySecretName']
                del json_sink['options']['eventhub.secretsScopeName']
                # DLT interacts with EventHub API as Kafka, change format before invoking sink.
                json_sink['format'] = 'kafka'
            if 'select_exp' in json_sink.keys():
                json_sink['select_exp'] = json_sink['select_exp']
            if 'where_clause' in json_sink.keys():
                json_sink['where_clause'] = json_sink['where_clause']
            for missing_sink_payload_key in missing_sink_payload_keys:
                json_sink[
                    missing_sink_payload_key
                ] = DataflowSpecUtils.sink_attributes_defaults[missing_sink_payload_key]
            logger.info(f"final sink={json_sink}")
            dlt_sinks.append(DLTSink(**json_sink))
        return dlt_sinks
