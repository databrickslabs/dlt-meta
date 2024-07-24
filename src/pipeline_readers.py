"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class PipelineReaders:
    """PipelineReader Class.

    Returns:
        _type_: _description_
    """
    def __init__(self, spark, source_format, source_details, reader_config_options, schema_json=None):
        """Init."""
        self.spark = spark
        self.source_format = source_format
        self.source_details = source_details
        self.reader_config_options = reader_config_options
        self.schema_json = schema_json

    def read_dlt_cloud_files(self) -> DataFrame:
        """Read dlt cloud files.

        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")
        input_df = None
        source_path = self.source_details["path"]
        if self.schema_json and self.source_format != "delta":
            schema = StructType.fromJson(self.schema_json)
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .schema(schema)
                .load(source_path)
            )
        else:
            input_df = (
                self.spark.readStream.format(self.source_format)
                .options(**self.reader_config_options)
                .load(source_path)
            )
        if self.source_details and "source_metadata" in self.source_details.keys():
            input_df = PipelineReaders.add_cloudfiles_metadata(self.source_details, input_df)
        return input_df

    @staticmethod
    def add_cloudfiles_metadata(sourceDetails, input_df):
        source_metadata_json = json.loads(sourceDetails.get("source_metadata"))
        keys = source_metadata_json.keys()
        autoloader_metadata_column_flag = False
        source_metadata_col_name = "_metadata"
        input_df = input_df.selectExpr("*", f"{source_metadata_col_name}")
        if "select_metadata_cols" in source_metadata_json:
            select_metadata_cols = source_metadata_json["select_metadata_cols"]
            for select_metadata_col in select_metadata_cols:
                input_df = input_df.withColumn(select_metadata_col, col(select_metadata_cols[select_metadata_col]))
        if "include_autoloader_metadata_column" in keys:
            autoloader_metadata_column = source_metadata_json["include_autoloader_metadata_column"]
            autoloader_metadata_column_flag = True if autoloader_metadata_column.lower() == "true" else False
            if autoloader_metadata_column_flag and "autoloader_metadata_col_name" in source_metadata_json:
                custom_source_metadata_col_name = source_metadata_json["autoloader_metadata_col_name"]
                if custom_source_metadata_col_name != source_metadata_col_name:
                    input_df = input_df.withColumnRenamed(f"{source_metadata_col_name}",
                                                          f"{custom_source_metadata_col_name}")
            elif autoloader_metadata_column_flag and "autoloader_metadata_col_name" not in source_metadata_json:
                input_df = input_df.withColumnRenamed("_metadata", "source_metadata")
        else:
            input_df = input_df.drop(f"{source_metadata_col_name}")
        return input_df

    def read_dlt_delta(self) -> DataFrame:
        """Read dlt delta.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
        Returns:
            DataFrame: _description_
        """
        logger.info("In read_dlt_cloud_files func")

        if self.reader_config_options and len(self.reader_config_options) > 0:
            return (
                self.spark.readStream.options(**self.reader_config_options).table(
                    f"""{self.source_details["source_database"]}
                        .{self.source_details["source_table"]}"""
                )
            )
        else:
            return (
                self.spark.readStream.table(
                    f"""{self.source_details["source_database"]}
                        .{self.source_details["source_table"]}"""
                )
            )

    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        from pyspark.dbutils import DBUtils
        return DBUtils(self.spark)

    def read_kafka(self) -> DataFrame:
        """Read eventhub with dataflowspec and schema.

        Args:
            spark (_type_): _description_
            bronze_dataflow_spec (_type_): _description_
            schema_json (_type_): _description_

        Returns:
            DataFrame: _description_
        """
        if self.source_format == "eventhub":
            kafka_options = self.get_eventhub_kafka_options()
        elif self.source_format == "kafka":
            kafka_options = self.get_kafka_options()
        raw_df = (
            self.spark
            .readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
            # add date, hour, and minute columns derived from eventhub enqueued timestamp
            .selectExpr("*", "to_date(timestamp) as date", "hour(timestamp) as hour", "minute(timestamp) as minute")
        )
        if self.schema_json:
            schema = StructType.fromJson(self.schema_json)
            return (
                raw_df.withColumn("parsed_records", from_json(col("value").cast("string"), schema))
            )
        else:
            return raw_df

    def get_eventhub_kafka_options(self):
        """Get eventhub options from dataflowspec."""
        dbutils = self.get_db_utils()
        eh_namespace = self.source_details.get("eventhub.namespace")
        eh_port = self.source_details.get("eventhub.port")
        eh_name = self.source_details.get("eventhub.name")
        eh_shared_key_name = self.source_details.get("eventhub.accessKeyName")
        secret_name = self.source_details.get("eventhub.accessKeySecretName")
        if not secret_name:
            # set default value if "eventhub.accessKeySecretName" is not specified
            secret_name = eh_shared_key_name
        secret_scope = self.source_details.get("eventhub.secretsScopeName")
        eh_shared_key_value = dbutils.secrets.get(secret_scope, secret_name)
        eh_shared_key_value = f"SharedAccessKeyName={eh_shared_key_name};SharedAccessKey={eh_shared_key_value}"
        eh_conn_str = f"Endpoint=sb://{eh_namespace}.servicebus.windows.net/;{eh_shared_key_value}"
        eh_kafka_str = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
        sasl_config = f"{eh_kafka_str} required username=\"$ConnectionString\" password=\"{eh_conn_str}\";"

        eh_conn_options = {
            "kafka.bootstrap.servers": f"{eh_namespace}.servicebus.windows.net:{eh_port}",
            "subscribe": eh_name,
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.jaas.config": sasl_config
        }
        kafka_options = {**eh_conn_options, **self.reader_config_options}
        return kafka_options

    def get_kafka_options(self):
        """Get kafka options from dataflowspec."""
        kafka_base_ops = {
            "kafka.bootstrap.servers": self.source_details.get("kafka.bootstrap.servers"),
            "subscribe": self.source_details.get("subscribe")
        }
        ssl_truststore_location = self.source_details.get("kafka.ssl.truststore.location", None)
        ssl_keystore_location = self.source_details.get("kafka.ssl.keystore.location", None)
        if ssl_truststore_location and ssl_keystore_location:
            truststore_scope = self.source_details.get("kafka.ssl.truststore.secrets.scope", None)
            truststore_key = self.source_details.get("kafka.ssl.truststore.secrets.key", None)
            keystore_scope = self.source_details.get("kafka.ssl.keystore.secrets.scope", None)
            keystore_key = self.source_details.get("kafka.ssl.keystore.secrets.key", None)
            if (truststore_scope and truststore_key and keystore_scope and keystore_key):
                dbutils = self.get_db_utils()
                kafka_ssl_conn = {
                    "kafka.ssl.truststore.location": ssl_truststore_location,
                    "kafka.ssl.keystore.location": ssl_keystore_location,
                    "kafka.ssl.keystore.password": dbutils.secrets.get(keystore_scope, keystore_key),
                    "kafka.ssl.truststore.password": dbutils.secrets.get(truststore_scope, truststore_key)
                }
                kafka_options = {**kafka_base_ops, **kafka_ssl_conn, **self.reader_config_options}
            else:
                params = ["kafka.ssl.truststore.secrets.scope",
                          "kafka.ssl.truststore.secrets.key",
                          "kafka.ssl.keystore.secrets.scope",
                          "kafka.ssl.keystore.secrets.key"
                          ]
                raise Exception(f"Kafka ssl required params are: {params}! provided options are :{self.source_details}")
        else:
            kafka_options = {**kafka_base_ops, **self.reader_config_options}
        return kafka_options
