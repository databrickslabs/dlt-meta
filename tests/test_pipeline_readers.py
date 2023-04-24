"""Test for pipeline readers."""
from datetime import datetime
import sys
from src.dataflow_spec import BronzeDataflowSpec
from src.pipeline_readers import PipelineReaders
from tests.utils import DLTFrameworkTestCase
from unittest.mock import MagicMock, patch
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
            "eventhub.secretsScopeName": "eventhubs_creds",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL",
            "eventhub.namespace": "ganesh-standard",
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
        customer_df = PipelineReaders.read_dlt_cloud_files(self.spark, bronze_dataflow_spec, schema)
        self.assertIsNotNone(customer_df)

    @patch.object(PipelineReaders, "get_db_utils", return_value=dbutils)
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_get_eventhub_kafka_options(self, get_db_utils, dbutils):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_eventhub_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        kafka_options = PipelineReaders.get_eventhub_kafka_options(self.spark, bronze_dataflow_spec)
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
        with self.assertRaises(Exception):
            PipelineReaders.get_kafka_options(self.spark, bronze_dataflow_spec)

    def test_get_kafka_options_positive(self):
        """Test Get kafka options."""
        bronze_map = PipelineReadersTests.bronze_kafka_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        kafka_options = PipelineReaders.get_kafka_options(self.spark, bronze_dataflow_spec)
        self.assertIsNotNone(kafka_options)

    def test_get_db_utils(self):
        """Test Get kafka options."""
        dbutils = PipelineReaders.get_db_utils(self.spark)
        self.assertIsNotNone(dbutils)

    @patch.object(spark, "readStream", return_value={"called"})
    @patch.object(dbutils, "secrets.get", return_value={"called"})
    def test_kafka_positive(self, spark, dbutils):
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
        customer_df = PipelineReaders.read_kafka(spark, bronze_dataflow_spec, None)
        self.assertIsNotNone(customer_df)

    @patch.object(spark, "readStream", return_value={"called"})
    def test_eventhub_positive(self, spark):
        """Test eventhub read positive."""
        bronze_map = PipelineReadersTests.bronze_eventhub_dataflow_spec_map
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        customer_df = PipelineReaders.read_kafka(spark, bronze_dataflow_spec, None)
        self.assertIsNotNone(customer_df)
