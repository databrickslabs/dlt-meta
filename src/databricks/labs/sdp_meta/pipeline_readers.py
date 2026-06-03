"""PipelineReaders providers DLT readers functionality."""
import logging
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import from_json, col

logger = logging.getLogger('databricks.labs.sdp_meta')
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

    def _read_file_source_dataframe(self) -> DataFrame:
        """Format-agnostic file-source read.

        Wraps the ``spark.readStream.format(<fmt>).options(...).load(<path>)``
        call shared by the Auto Loader path (``read_dlt_cloud_files``)
        and the vanilla file-source path (``read_dlt_file_source``).
        Honours ``self.schema_json`` when supplied — Spark requires a
        schema for non-Delta streaming reads in many cases, and the two
        public entry points both want this same gating.

        Private (leading-underscore) so the dispatcher in
        ``DataflowPipeline.read_bronze`` keeps using the public
        ``read_dlt_cloud_files`` / ``read_dlt_file_source`` methods —
        the split between Auto Loader and OSS-friendly file sources
        is a contract surface, not a refactor opportunity.
        """
        source_path = self.source_details["path"]
        reader = self.spark.readStream.format(self.source_format).options(
            **self.reader_config_options
        )
        # ``self.source_format != "delta"`` mirrors the original guard
        # before the autoloader / vanilla-file-source split — Delta
        # reads have their own schema discovery via the table log,
        # so we only attach a user-supplied schema for non-Delta
        # streaming reads.
        if self.schema_json and self.source_format != "delta":
            reader = reader.schema(StructType.fromJson(self.schema_json))
        return reader.load(source_path)

    def read_dlt_cloud_files(self) -> DataFrame:
        """Read a Databricks Auto Loader (``cloudFiles``) source.

        Lakeflow / DBR only — the ``cloudFiles`` source is the
        Databricks-proprietary Auto Loader and does NOT exist in
        OSS Apache Spark. The ``read_bronze`` dispatcher in
        ``DataflowPipeline`` routes ``cloudFiles`` here, and only
        here, so the Auto Loader-specific behaviour stays out of
        the OSS code path:

        * the ``cloudFiles.format`` / ``cloudFiles.inferColumnTypes``
          / ``cloudFiles.rescuedDataColumn`` reader options are passed
          through verbatim by the generic ``_read_file_source_dataframe``
          helper, but they only mean something to the Auto Loader
          source itself;
        * the ``add_cloudfiles_metadata`` post-read enrichment is
          gated on the Auto Loader-specific ``source_metadata``
          block in ``source_details`` (keys like
          ``include_autoloader_metadata_column`` /
          ``autoloader_metadata_col_name``).

        For vanilla streaming file sources (``json``, ``csv``,
        ``parquet``, ``orc``, ``text``, ``avro``) use
        ``read_dlt_file_source`` instead — that path skips both the
        Auto Loader option assumptions and the autoloader metadata
        helper.
        """
        logger.info("In read_dlt_cloud_files func (Auto Loader / cloudFiles)")
        input_df = self._read_file_source_dataframe()
        if self.source_details and "source_metadata" in self.source_details.keys():
            input_df = PipelineReaders.add_cloudfiles_metadata(self.source_details, input_df)
        return input_df

    def read_dlt_file_source(self) -> DataFrame:
        """Read a vanilla Spark streaming file source.

        Handles the OSS-friendly file formats — ``json``, ``csv``,
        ``parquet``, ``orc``, ``text``, ``avro`` — via
        ``spark.readStream.format(<fmt>).load(<path>)``. Works on
        both Databricks Lakeflow and OSS Apache Spark, which is why
        ``oss_onboarding.json`` and the OSS demo use this path.

        Deliberately does NOT call ``add_cloudfiles_metadata``: the
        ``source_metadata`` config block (with keys like
        ``include_autoloader_metadata_column`` /
        ``autoloader_metadata_col_name``) is Auto Loader-specific
        and silently doing nothing on vanilla sources would be the
        worst kind of footgun. If you set ``source_metadata`` on a
        non-``cloudFiles`` source it is intentionally ignored at the
        reader; the onboarding parser still records it for auditing
        but the runtime treats it as a no-op for this format. Use
        ``read_dlt_cloud_files`` if you need autoloader metadata
        behaviour.

        For Auto Loader (``cloudFiles``) use ``read_dlt_cloud_files``.

        Raises:
            ValueError: when ``schema_json`` is missing. Vanilla
                streaming file sources (unlike ``cloudFiles``)
                cannot infer schema and raise an opaque
                ``AnalysisException`` deep inside Spark at
                ``readStream.load()`` time. We pre-check and raise
                a clear, actionable message pointing at
                ``source_schema_path`` instead.
        """
        if not self.schema_json:
            raise ValueError(
                f"source_format={self.source_format!r} requires a schema, "
                "but none was supplied. Vanilla streaming file sources "
                "(json / csv / parquet / orc / text / avro) cannot infer "
                "their schema at readStream time the way Auto Loader "
                "(cloudFiles) can. Set ``source_schema_path`` in the "
                "onboarding spec's ``source_details`` to point at a DDL "
                "file describing the source columns, or migrate to "
                "``source_format: cloudFiles`` on Databricks Lakeflow."
            )
        logger.info(
            "In read_dlt_file_source func (vanilla Spark file source: %s)",
            self.source_format,
        )
        return self._read_file_source_dataframe()

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

        source_cl = self.source_details.get('source_catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        table_path = f"{source_cl_name}{self.source_details['source_database']}.{self.source_details['source_table']}"

        if self.source_format == "snapshot":
            reader = self.spark.read
        else:
            reader = self.spark.readStream

        if self.reader_config_options:
            return reader.options(**self.reader_config_options).table(table_path)
        else:
            return reader.table(table_path)

    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        try:
            from pyspark.dbutils import DBUtils
            return DBUtils(self.spark)
        except ImportError:
            raise RuntimeError(
                "DBUtils is not available. "
                "Secret management features (Kafka/EventHub with secrets) require Databricks runtime."
            )

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
        kafka_broker = self.source_details.get("kafka.bootstrap.servers", None)
        if not kafka_broker:
            kafka_source_servers_secrets_scope_key = self.source_details.get(
                "kafka_source_servers_secrets_scope_key",
                None
            )
            kafka_source_servers_secrets_scope_name = self.source_details.get(
                "kafka_source_servers_secrets_scope_name", None)
            if kafka_source_servers_secrets_scope_key and kafka_source_servers_secrets_scope_name:
                dbutils = self.get_db_utils()
                kafka_broker = dbutils.secrets.get(
                    kafka_source_servers_secrets_scope_name, kafka_source_servers_secrets_scope_key)
            else:
                raise Exception(
                    f"Kafka broker details not found for source_details={self.source_details}!"
                )
        topic = self.source_details.get("subscribe", None)
        if not topic:
            raise Exception(f"Kafka topic details not found for source_details={self.source_details}!")
        kafka_base_ops = {
            "kafka.bootstrap.servers": kafka_broker,
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
