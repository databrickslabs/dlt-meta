"""Test for pipeline readers."""
from datetime import datetime
import sys
import os
import json
from pyspark.sql.functions import lit, struct
from pyspark.sql.types import StructType
from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec
from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders
from tests.utils import SDPFrameworkTestCase
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
sys.modules["pyspark.pipelines"] = MagicMock()
sys.modules["pyspark.dbutils"] = MagicMock()


from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
import pyspark.sql.types as T

dbutils = MagicMock()
DBUtils = MagicMock()
spark = MagicMock()
spark.readStream = MagicMock()


class PipelineReadersTests(SDPFrameworkTestCase):
    """Pieline readers unit tests."""

    bronze_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {
            "path": "tests/resources/data/customers",
        },
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/localtest/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "appendFlowsSchemas": None,
        "sinks": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
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
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "appendFlowsSchemas": None,
        "sinks": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
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
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "appendFlowsSchemas": None,
        "sinks": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
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
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": None,
        "quarantineTargetDetails": None,
        "quarantineTableProperties": None,
        "appendFlows": None,
        "appendFlowsSchemas": None,
        "sinks": None,
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "sdp-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "sdp-meta-unittest",
        "clusterBy": [""],
        "clusterByAuto": False,
        "cdcApplyChangesFlows": None,
        "cdcApplyChangesFlowsSchemas": None,
        "rowFilter": None,
        "quarantineRowFilter": None,
    }

    def setUp(self):
        """Set initial resources.

        Note: ``setUp`` is a regular instance method (not @classmethod)
        because the base class ``SDPFrameworkTestCase.setUp`` is also
        an instance method. The historical ``@classmethod``
        annotation here was a bug paired with the matching bug in
        the base class -- both have been fixed in the same refactor
        that moved Spark startup into ``setUpClass`` for perf.
        """
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

    @patch.object(PipelineReaders, "add_cloudfiles_metadata", return_value={"called"})
    @patch.object(SparkSession, "readStream")
    def test_read_cloud_files_withmetadata_cols_positive(self, SparkSession, add_cloudfiles_metadata):
        """Test read_cloud_files positive."""
        mock_format = MagicMock()
        mock_options = MagicMock()
        mock_load = MagicMock()
        mock_schema = MagicMock()
        SparkSession.readStream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.schema.return_value = mock_schema
        mock_schema.load.return_value = mock_load
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        schema_ddl = "tests/resources/schema/customer_schema.ddl"
        ddlSchemaStr = self.spark.read.text(paths=schema_ddl, wholetext=True).collect()[0]["value"]
        spark_schema = T._parse_datatype_string(ddlSchemaStr)
        schema = spark_schema.jsonValue()
        schema_map = {"schema": schema}
        bronze_map.update(schema_map)
        source_metdata_json = {
            "include_autoloader_metadata_column": "True",
            "autoloader_metadata_col_name": "source_metadata",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
        }
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            SparkSession,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            schema
        )
        pipeline_readers.read_dlt_cloud_files()
        SparkSession.readStream.format.assert_called_once_with("json")
        SparkSession.readStream.format.return_value.options.assert_called_once_with(
            **bronze_dataflow_spec.readerConfigOptions
        )
        struct_schema = StructType.fromJson(schema)
        SparkSession.readStream.format.return_value.options.return_value.schema.assert_called_once_with(struct_schema)
        (SparkSession.readStream.format.return_value.options.return_value.schema
         .return_value.load.assert_called_once_with(bronze_dataflow_spec.sourceDetails["path"]))
        assert add_cloudfiles_metadata.called

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

        source_details_map = {"sourceDetails": {"source_database": "source_bronze", "source_table": "customer"}}

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
        source_details_map = {"sourceDetails": {"source_database": "source_bronze", "source_table": "customer"}}
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

    @patch.object(PipelineReaders, "get_db_utils")
    def test_eventhub_service_credential_avoids_sas_options(self, get_db_utils):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {
                "databricks.serviceCredential": "eventhubs-managed-identity",
                "startingOffsets": "earliest",
            },
        )

        kafka_options = reader.get_eventhub_kafka_options()

        self.assertEqual(
            kafka_options,
            {
                "kafka.bootstrap.servers": "standard.servicebus.windows.net:9093",
                "subscribe": "iot",
                "databricks.serviceCredential": "eventhubs-managed-identity",
                "startingOffsets": "earliest",
            },
        )
        get_db_utils.assert_not_called()

    def test_eventhub_service_credential_rejects_sasl_options(self):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {
                "databricks.serviceCredential": "eventhubs-managed-identity",
                "kafka.sasl.mechanism": "PLAIN",
            },
        )

        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            reader.get_eventhub_kafka_options()

    def test_eventhub_service_credential_rejects_case_insensitive_sasl_options(self):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {
                "Databricks.ServiceCredential": "eventhubs-managed-identity",
                "Kafka.Sasl.Login.Callback.Handler.Class": "example.Handler",
            },
        )

        with self.assertRaisesRegex(ValueError, "Kafka.Sasl.Login.Callback.Handler.Class"):
            reader.get_eventhub_kafka_options()

    def test_eventhub_service_credential_rejects_sas_source_details(self):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
                "eventhub.accessKeyName": "legacy-policy",
            },
            {"databricks.serviceCredential": "eventhubs-managed-identity"},
        )

        with self.assertRaisesRegex(ValueError, "SAS Event Hubs source details"):
            reader.get_eventhub_kafka_options()

    def test_eventhub_rejects_endpoint_or_topic_reader_option_overrides(self):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {
                "databricks.serviceCredential": "eventhubs-managed-identity",
                "kafka.bootstrap.servers": "other.servicebus.windows.net:9093",
                "subscribe": "other-topic",
            },
        )

        with self.assertRaisesRegex(ValueError, "Kafka connection or SAS options"):
            reader.get_eventhub_kafka_options()

    def test_eventhub_service_credential_rejects_all_subscription_options(self):
        source_details = {
            "eventhub.namespace": "standard",
            "eventhub.name": "iot",
            "eventhub.port": "9093",
        }
        for option in ("subscribe", "subscribePattern", "assign"):
            with self.subTest(option=option):
                reader = PipelineReaders(
                    self.spark,
                    "eventhub",
                    source_details,
                    {
                        "databricks.serviceCredential": "eventhubs-managed-identity",
                        option: "another-topic",
                    },
                )
                with self.assertRaisesRegex(ValueError, option):
                    reader.get_eventhub_kafka_options()

    def test_eventhub_service_credential_validates_credential_value(self):
        source_details = {
            "eventhub.namespace": "standard",
            "eventhub.name": "iot",
            "eventhub.port": "9093",
        }
        for credential in ("   ", 1):
            with self.subTest(credential=credential):
                reader = PipelineReaders(
                    self.spark,
                    "eventhub",
                    source_details,
                    {"databricks.serviceCredential": credential},
                )
                with self.assertRaisesRegex(ValueError, "non-empty service credential"):
                    reader.get_eventhub_kafka_options()

        reader = PipelineReaders(
            self.spark,
            "eventhub",
            source_details,
            {"databricks.serviceCredential": " eventhubs-managed-identity "},
        )
        self.assertEqual(
            reader.get_eventhub_kafka_options()["databricks.serviceCredential"],
            "eventhubs-managed-identity",
        )

    @patch.object(PipelineReaders, "get_db_utils")
    def test_eventhub_service_credential_key_is_case_insensitive(self, get_db_utils):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {"databricks.servicecredential": "eventhubs-managed-identity"},
        )

        options = reader.get_eventhub_kafka_options()

        self.assertEqual(
            options["databricks.serviceCredential"],
            "eventhubs-managed-identity",
        )
        self.assertNotIn("databricks.servicecredential", options)
        get_db_utils.assert_not_called()

    def test_eventhub_rejects_duplicate_case_insensitive_service_credential_keys(self):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
            },
            {
                "databricks.serviceCredential": "first-credential",
                "Databricks.ServiceCredential": "second-credential",
            },
        )

        with self.assertRaisesRegex(ValueError, "cannot be configured more than once"):
            reader.get_eventhub_kafka_options()

    def test_eventhub_service_credential_requires_endpoint_details(self):
        details = {
            "eventhub.namespace": "standard",
            "eventhub.name": "iot",
            "eventhub.port": "9093",
        }
        for field in details:
            with self.subTest(field=field):
                source_details = details.copy()
                del source_details[field]
                reader = PipelineReaders(
                    self.spark,
                    "eventhub",
                    source_details,
                    {"databricks.serviceCredential": "eventhubs-managed-identity"},
                )
                with self.assertRaisesRegex(ValueError, field):
                    reader.get_eventhub_kafka_options()

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value="shared_access_key")
    def test_eventhub_sas_allows_reader_option_overrides(self, get_db_utils, dbutils):
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            {
                "eventhub.namespace": "standard",
                "eventhub.name": "iot",
                "eventhub.port": "9093",
                "eventhub.accessKeyName": "policy",
                "eventhub.secretsScopeName": "scope",
            },
            {
                "kafka.bootstrap.servers": "private.servicebus.windows.net:9093",
                "subscribe": "private-topic",
            },
        )

        options = reader.get_eventhub_kafka_options()

        self.assertEqual(
            options["kafka.bootstrap.servers"],
            "private.servicebus.windows.net:9093",
        )
        self.assertEqual(options["subscribe"], "private-topic")
        self.assertNotIn("databricks.serviceCredential", options)

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value="shared_access_key")
    def test_eventhub_sas_drops_null_service_credential_and_rejects_subscription_conflicts(
        self, get_db_utils, dbutils
    ):
        source_details = {
            "eventhub.namespace": "standard",
            "eventhub.name": "iot",
            "eventhub.port": "9093",
            "eventhub.accessKeyName": "policy",
            "eventhub.secretsScopeName": "scope",
        }
        reader = PipelineReaders(
            self.spark,
            "eventhub",
            source_details,
            {"Databricks.ServiceCredential": None},
        )
        options = reader.get_eventhub_kafka_options()
        self.assertNotIn("Databricks.ServiceCredential", options)

        for option in ("SubscribePattern", "ASSIGN"):
            with self.subTest(option=option):
                reader = PipelineReaders(
                    self.spark,
                    "eventhub",
                    source_details,
                    {option: "another-topic"},
                )
                with self.assertRaisesRegex(ValueError, option):
                    reader.get_eventhub_kafka_options()

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

    def test_get_db_utils_import_error(self):
        """Test get_db_utils raises RuntimeError when DBUtils is not available."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        with patch.dict(sys.modules, {"pyspark.dbutils": None}):
            with self.assertRaises(RuntimeError) as context:
                pipeline_readers.get_db_utils()
            self.assertIn("DBUtils is not available", str(context.exception))

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

    def test_add_cloudfiles_metadata(self):
        """Test add_cloudfiles_metadata."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        source_metdata_json = {
            "include_autoloader_metadata_column": "True",
            "autoloader_metadata_col_name": "source_metadata",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'source_metadata', 'source_metadata', 'input_file_name', 'input_file_path']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_add_cloudfiles_metadata_cols_with_include_autoloader_metadata_column(self):
        """Test add_cloudfiles_metadata."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        source_metdata_json = {
            "include_autoloader_metadata_column": "True",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'source_metadata', 'source_metadata', 'input_file_name', 'input_file_path']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_add_cloudfiles_metadata_cols_only(self):
        """Test add_cloudfiles_metadata."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        source_metdata_json = {
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'input_file_name', 'input_file_path']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_add_cloudfiles_with_include_autoloader_metadata_column_only(self):
        """Test add_cloudfiles_metadata."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map
        source_format_map = {"sourceFormat": "json"}
        bronze_map.update(source_format_map)
        source_metdata_json = {
            "include_autoloader_metadata_column": "True"
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'source_metadata', 'source_metadata']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    @patch.object(PipelineReaders, "add_cloudfiles_metadata", return_value={"called"})
    @patch.object(SparkSession, "readStream")
    def test_read_cloud_files_without_source_metadata(self, mock_readstream, mock_add_cloudfiles_metadata):
        """Test read_dlt_cloud_files without source metadata to cover lines 49-51."""
        mock_format = MagicMock()
        mock_options = MagicMock()
        mock_load = MagicMock()
        mock_readstream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.load.return_value = mock_load

        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map.copy()
        # Remove source_metadata to test the negative path
        source_details = bronze_map["sourceDetails"].copy()
        if "source_metadata" in source_details:
            del source_details["source_metadata"]
        bronze_map["sourceDetails"] = source_details

        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            mock_readstream,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        result = pipeline_readers.read_dlt_cloud_files()

        # Should not call add_cloudfiles_metadata when no source_metadata
        mock_add_cloudfiles_metadata.assert_not_called()
        self.assertIsNotNone(result)

    def test_add_cloudfiles_metadata_with_custom_column_name_different_from_default(self):
        """Test add_cloudfiles_metadata with custom column name different from _metadata to cover lines 69-71."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map.copy()
        source_metdata_json = {
            "include_autoloader_metadata_column": "True",
            "autoloader_metadata_col_name": "custom_metadata_column",
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'custom_metadata_column', 'custom_metadata_column', 'input_file_name', 'input_file_path']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_add_cloudfiles_metadata_with_autoloader_flag_but_no_custom_name(self):
        """Test add_cloudfiles_metadata with autoloader flag but no custom name to cover lines 72-73."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map.copy()
        source_metdata_json = {
            "include_autoloader_metadata_column": "True"
            # No autoloader_metadata_col_name specified
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'source_metadata', 'source_metadata']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_add_cloudfiles_metadata_without_autoloader_flag(self):
        """Test add_cloudfiles_metadata without autoloader flag to cover lines 74-76."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map.copy()
        source_metdata_json = {
            "select_metadata_cols": {
                "input_file_name": "_metadata.file_name",
                "input_file_path": "_metadata.file_path"
            }
            # No include_autoloader_metadata_column specified
        }
        expected_cols = ['address', 'email', 'firstname', 'id', 'lastname', 'operation', 'operation_date',
                         'input_file_name', 'input_file_path']
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        bronze_dataflow_spec.sourceDetails["source_metadata"] = json.dumps(source_metdata_json)
        df = (self.spark.read.json("tests/resources/data/customers")
              .withColumn('_metadata', struct(*[lit("filename").alias("file_name"),
                                                lit("file_path").alias('file_path')])))
        df = PipelineReaders.add_cloudfiles_metadata(bronze_dataflow_spec.sourceDetails, df)
        self.assertIsNotNone(df)
        self.assertEqual(df.columns, expected_cols)

    def test_read_delta_with_snapshot_format(self):
        """Test read_dlt_delta with snapshot format to cover line 94."""
        bronze_map = PipelineReadersTests.bronze_dataflow_spec_map.copy()
        source_format_map = {"sourceFormat": "snapshot"}
        bronze_map.update(source_format_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS source_bronze")
        full_path = os.path.abspath("tests/resources/delta/customers")
        self.spark.sql(f"CREATE TABLE if not exists source_bronze.customer USING DELTA LOCATION '{full_path}' ")

        source_details_map = {"sourceDetails": {"source_database": "source_bronze", "source_table": "customer"}}
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

    @patch.object(PipelineReaders, "get_kafka_options")
    def test_read_kafka_with_kafka_source_format(self, mock_get_kafka_options):
        """Test read_kafka with kafka source format to cover lines 121-123."""
        # Create a completely mocked Spark session to avoid JVM gateway issues
        mock_spark = MagicMock()
        mock_readstream = MagicMock()
        mock_format = MagicMock()
        mock_options = MagicMock()
        mock_load = MagicMock()
        mock_selectexpr = MagicMock()

        mock_spark.readStream = mock_readstream
        mock_readstream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.load.return_value = mock_load
        mock_load.selectExpr.return_value = mock_selectexpr

        mock_get_kafka_options.return_value = {"kafka.bootstrap.servers": "localhost:9092"}

        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map.copy()
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        pipeline_readers = PipelineReaders(
            mock_spark,  # Use completely mocked spark session
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )
        result = pipeline_readers.read_kafka()

        # Verify that get_kafka_options was called (not get_eventhub_kafka_options)
        mock_get_kafka_options.assert_called_once()
        self.assertIsNotNone(result)
        self.assertEqual(result, mock_selectexpr)

    def test_read_kafka_without_schema_json(self):
        """Test read_kafka without schema JSON to cover line 138 (else branch)."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map.copy()
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)

        # Mock the kafka methods
        with patch.object(PipelineReaders, 'get_kafka_options') as mock_get_kafka_options:
            mock_get_kafka_options.return_value = {"kafka.bootstrap.servers": "localhost:9092"}

            # Mock the readStream operations to avoid actual Kafka connection
            with patch('pyspark.sql.SparkSession.readStream', new_callable=MagicMock) as mock_readstream:
                mock_format = MagicMock()
                mock_options = MagicMock()
                mock_load = MagicMock()
                mock_selectexpr = MagicMock()

                mock_readstream.format.return_value = mock_format
                mock_format.options.return_value = mock_options
                mock_options.load.return_value = mock_load
                mock_load.selectExpr.return_value = mock_selectexpr

                pipeline_readers = PipelineReaders(
                    self.spark,
                    bronze_dataflow_spec.sourceFormat,
                    bronze_dataflow_spec.sourceDetails,
                    bronze_dataflow_spec.readerConfigOptions,
                    None  # No schema to trigger the else branch (line 138)
                )
                result = pipeline_readers.read_kafka()

                # Should return the result of selectExpr (line 138)
                self.assertEqual(result, mock_selectexpr)

    def test_get_kafka_options_with_secrets_for_broker(self):
        """Test get_kafka_options with secrets for broker to cover lines 172-183."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map.copy()
        # Remove direct kafka.bootstrap.servers and add secrets configuration
        source_details = bronze_map['sourceDetails'].copy()
        del source_details["kafka.bootstrap.servers"]
        source_details.update({
            "kafka_source_servers_secrets_scope_key": "kafka.ssl.truststore.secrets.scope",
            "kafka_source_servers_secrets_scope_name": "kafka.ssl.truststore.secrets.key",
            "kafka.ssl.truststore.secrets.key": "kafka.ssl.truststore.secrets.key",
            "kafka.ssl.truststore.secrets.scope": "kafka_scope",
            "kafka.ssl.truststore.location": "tests/resources/kafka/truststore.jks",
            "kafka.ssl.keystore.location": "tests/resources/kafka/keystore.jks",
            "kafka.ssl.keystore.secrets.scope": "kafka_scope",
            "kafka.ssl.keystore.secrets.key": "kafka_key"

        })
        bronze_map['sourceDetails'] = source_details
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)

        # Mock the get_db_utils method and secrets.get
        with patch.object(PipelineReaders, 'get_db_utils') as mock_get_db_utils:
            mock_dbutils = MagicMock()
            mock_dbutils.secrets.get.return_value = "localhost:9092"
            mock_get_db_utils.return_value = mock_dbutils

            pipeline_readers = PipelineReaders(
                self.spark,
                bronze_dataflow_spec.sourceFormat,
                bronze_dataflow_spec.sourceDetails,
                bronze_dataflow_spec.readerConfigOptions,
                bronze_dataflow_spec.schema
            )
            kafka_options = pipeline_readers.get_kafka_options()

            # Verify that secrets.get was called with correct parameters
            self.assertIsNotNone(kafka_options)
            self.assertEqual(kafka_options["kafka.bootstrap.servers"], "localhost:9092")

    def test_get_kafka_options_missing_broker_and_secrets(self):
        """Test get_kafka_options with missing broker and secrets to cover lines 182-185."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map.copy()
        # Remove kafka.bootstrap.servers and don't provide secrets
        source_details = bronze_map['sourceDetails'].copy()
        del source_details["kafka.bootstrap.servers"]
        # Don't add secrets configuration
        bronze_map['sourceDetails'] = source_details
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)

        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )

        with self.assertRaises(Exception) as context:
            pipeline_readers.get_kafka_options()
        self.assertIn("Kafka broker details not found", str(context.exception))

    def test_get_kafka_options_missing_topic(self):
        """Test get_kafka_options with missing topic to cover line 188."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map.copy()
        # Remove subscribe (topic)
        source_details = bronze_map['sourceDetails'].copy()
        del source_details["subscribe"]
        bronze_map['sourceDetails'] = source_details
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)

        pipeline_readers = PipelineReaders(
            self.spark,
            bronze_dataflow_spec.sourceFormat,
            bronze_dataflow_spec.sourceDetails,
            bronze_dataflow_spec.readerConfigOptions,
            bronze_dataflow_spec.schema
        )

        with self.assertRaises(Exception) as context:
            pipeline_readers.get_kafka_options()
        self.assertIn("Kafka topic details not found", str(context.exception))
