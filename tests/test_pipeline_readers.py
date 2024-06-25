"""Test for pipeline readers."""
from datetime import datetime
import sys
import os
from src.dataflow_spec import BronzeDataflowSpec
from src.pipeline_readers import PipelineReaders
from tests.utils import DLTFrameworkTestCase
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
sys.modules["dlt"] = MagicMock()
sys.modules["pyspark.dbutils"] = MagicMock()


from src.onboard_dataflowspec import OnboardDataflowspec
import pyspark.sql.types as T

dbutils = MagicMock()
DBUtils = MagicMock()
spark = MagicMock()
spark.readStream = MagicMock()


class PipelineReadersTests(DLTFrameworkTestCase):
    """Pieline readers unit tests."""

    bronze_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/localtest/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest"
    }

    bronze_eventhub_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "eventhub",
        "sourceDetails": {
            "source_schema_path": "tests/resources/schema/eventhub_iot_schema.ddl",
            "eventhub.accessKeyName": "iotIngestionAccessKey",
            "eventhub.name": "iot",
            "eventhub.accessKeySecretName": "iotIngestionAccessKey",
            "eventhub.secretsScopeName": "eventhubs_creds",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "eventhub.namespace": "standard",
            "eventhub.port": "9093"
        },
        "readerConfigOptions": {
            "maxOffsetsPerTrigger": "50000",
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
            "kafka.request.timeout.ms": "60000",
            "kafka.session.timeout.ms": "60000"
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/localtest/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest"
    }

    bronze_eventhub_dataflow_spec_omit_secret_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "eventhub",
        "sourceDetails": {
            "source_schema_path": "tests/resources/schema/eventhub_iot_schema.ddl",
            "eventhub.accessKeyName": "iotIngestionAccessKey",
            "eventhub.name": "iot",
            "eventhub.secretsScopeName": "eventhubs_creds",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "eventhub.namespace": "standard",
            "eventhub.port": "9093"
        },
        "readerConfigOptions": {
            "maxOffsetsPerTrigger": "50000",
            "startingOffsets": "latest",
            "failOnDataLoss": "false",
            "kafka.request.timeout.ms": "60000",
            "kafka.session.timeout.ms": "60000"
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/localtest/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest"
    }

    bronze_kafka_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "kafka",
        "sourceDetails": {
            "source_schema_path": "tests/resources/schema/eventhub_iot_schema.ddl",
            "subscribe": "iot",
            "kafka.bootstrap.servers": "127.0.0.1:9092"
        },
        "readerConfigOptions": {
            "maxOffsetsPerTrigger": "50000",
            "startingOffsets": "latest"
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/localtest/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest"
    }

    def setUp(self):
        """Set initial resources."""
        super().setUp()
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["bronze_dataflowspec_table"],
            self.onboarding_bronze_silver_params_map["bronze_dataflowspec_path"],
        )

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(
            self.onboarding_bronze_silver_params_map["database"],
            self.onboarding_bronze_silver_params_map["silver_dataflowspec_table"],
            self.onboarding_bronze_silver_params_map["silver_dataflowspec_path"],
        )

    def test_read_cloud_files_positive(self):
        """Test read_cloud_files positive."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        schema_ddl = "tests/resources/schema/customer_schema.ddl"
        ddlSchemaStr = self.spark.read.text(paths=schema_ddl, wholetext=True).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        schema = spark_schema.jsonValue()
        schema_map = {"schema": schema}
        bronze_map.update(schema_map)
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            schema
        )
        customer_df = pipeline_readers.read_dlt_cloud_files()
        self.assertIsNotNone(customer_df)

    @patch.object(SparkSession, "readStream")
    def test_read_cloud_files_no_schema(self, SparkSession):
        """Test read_cloud_files positive."""
        mock_format = MagicMock()
        mock_options = MagicMock()
        mock_load = MagicMock()
        SparkSession.readStream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.load.return_value = mock_load

        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            SparkSession,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        pipeline_readers.read_dlt_cloud_files()
        SparkSession.readStream.format.assert_called_once_with("json")
        SparkSession.readStream.format.return_value.options.assert_called_once_with(
            **bronze_dataflow_spec.readerConfigOptions
        )
        SparkSession.readStream.format.return_value.options.return_value.load.assert_called_once_with(
            bronze_dataflow_spec.sourceDetails["path"])

    def test_read_delta_positive(self):
        """Test read_cloud_files positive."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "delta"}
        bronze_map.update(source_format_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS source_bronze")
        full_path = os.path.abspath("tests/resources/delta/customers")
        self.spark.sql(f"CREATE TABLE if not exists source_bronze.customer USING DELTA LOCATION '{full_path}' ")

        source_details_map = {"sourceDetails": {"source_database": "source_bronze", "table": "customer"}}

        bronze_map.update(source_details_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        customer_df = pipeline_readers.read_dlt_delta()
        self.assertIsNotNone(customer_df)

    def test_read_delta_with_read_config_positive(self):
        """Test read_cloud_files positive."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "delta"}
        bronze_map.update(source_format_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS source_bronze")
        full_path = os.path.abspath("tests/resources/delta/customers")
        self.spark.sql(f"CREATE TABLE if not exists source_bronze.customer USING DELTA LOCATION '{full_path}' ")
        source_details_map = {"sourceDetails": {"source_database": "source_bronze", "table": "customer"}}
        bronze_map.update(source_details_map)
        reader_config = {"readerConfigOptions": {"maxFilesPerTrigger": "1"}}
        bronze_map.update(reader_config)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        customer_df = pipeline_readers.read_dlt_delta()
        self.assertIsNotNone(customer_df)

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_get_eventhub_kafka_options(self, get_db_utils, dbutils):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_eventhub_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        kafka_options = pipeline_readers.get_eventhub_kafka_options()
        self.assertIsNotNone(kafka_options)

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_get_eventhub_kafka_options_omit_secret(self, get_db_utils, dbutils):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_eventhub_dataflow_spec_omit_secret_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        kafka_options = pipeline_readers.get_eventhub_kafka_options()
        self.assertIsNotNone(kafka_options)

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_get_kafka_options_ssl_exception(self, get_db_utils, dbutils):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        source_details = bronze_map['sourceDetails']
        source_details_map = {
            **source_details,
            "kafka.ssl.truststore.location": "tmp:/location",
            "kafka.ssl.keystore.location": "tmp:/location",
        }
        bronze_map['sourceDetails'] = source_details_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        with self.assertRaises(Exception):
            pipeline_readers.get_kafka_options()

    def test_get_kafka_options_positive(self):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        kafka_options = pipeline_readers.get_kafka_options()
        self.assertIsNotNone(kafka_options)

    def test_get_db_utils(self):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        dbutils = pipeline_readers.get_db_utils()
        self.assertIsNotNone(dbutils)

    @patch.object(SparkSession, "readStream", return_value={"called"})
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_kafka_positive(self, SparkSession, dbutils):
        """Test kafka read positive."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        source_details = bronze_map['sourceDetails']
        source_details_map = {
            **source_details,
            "kafka.ssl.truststore.location": "tmp:/location",
            "kafka.ssl.keystore.location": "tmp:/location",
            "kafka.ssl.truststore.secrets.scope": "databricks",
            "kafka.ssl.truststore.secrets.key": "databricks",
            "kafka.ssl.keystore.secrets.scope": "databricks",
            "kafka.ssl.keystore.secrets.key": "databricks"
        }
        bronze_map['sourceDetails'] = source_details_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            SparkSession,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        customer_df = pipeline_readers.read_kafka()
        self.assertIsNotNone(customer_df)

    @patch.object(SparkSession, "readStream", return_value={"called"})
    def test_eventhub_positive(self, SparkSession):
        """Test eventhub read positive."""
        bronze_map = PipelineReadersTests.bronze_eventhub_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            SparkSession,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        customer_df = pipeline_readers.read_kafka()
        self.assertIsNotNone(customer_df)
