"""Tests for Dataflowpipeline."""
from datetime import datetime
import json
import sys
import tempfile
import copy
from pyspark.sql.functions import lit, expr
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from tests.utils import DLTFrameworkTestCase
from unittest.mock import MagicMock, patch
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
sys.modules["dlt"] = MagicMock()
from src.dataflow_pipeline import DataflowPipeline
from src.onboard_dataflowspec import OnboardDataflowspec
from src.dataflow_spec import DataflowSpecUtils
from src.pipeline_readers import PipelineReaders
dlt = MagicMock()
dlt.expect_all_or_drop = MagicMock()
dlt.apply_changes_from_snapshot = MagicMock()
raw_delta_table_stream = MagicMock()


class DataflowPipelineTests(DLTFrameworkTestCase):
    """Test for Dataflowpipeline."""

    bronze_dataflow_spec_acs_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": """{"keys": ["id"], "scd_type": "2"}""",
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {
            "database": "bronze", "table": "customer_dqe", "path": "tests/localtest/delta/customers_dqe"
        },
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest",
        "clusterBy": [""],
    }

    bronze_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "json",
        "sourceDetails": {"path": "tests/resources/data/customers"},
        "readerConfigOptions": {
        },
        "targetFormat": "delta",
        "targetDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"},
        "tableProperties": {},
        "schema": None,
        "partitionColumns": [""],
        "cdcApplyChanges": None,
        "applyChangesFromSnapshot": None,
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "quarantineTargetDetails": {
            "database": "bronze", "table": "customer_dqe", "path": "tests/localtest/delta/customers_dqe"
        },
        "quarantineTableProperties": {},
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest",
        "clusterBy": [""],
    }
    silver_cdc_apply_changes = {
        "keys": ["id"],
        "sequence_by": "operation_date",
        "scd_type": "1",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": ["operation", "operation_date", "_rescued_data"],
    }
    silver_cdc_apply_changes_scd2 = {
        "keys": ["id"],
        "sequence_by": "operation_date",
        "scd_type": "2",
        "apply_as_deletes": "operation = 'DELETE'",
        "except_column_list": ["operation", "operation_date", "_rescued_data"],
    }
    silver_dataflow_spec_map = {
        "dataFlowId": "1",
        "dataFlowGroup": "A1",
        "sourceFormat": "delta",
        "sourceDetails": {
            "database": "bronze",
            "table": "customer",
            "path": bronze_dataflow_spec_map["targetDetails"]["path"],
        },
        "readerConfigOptions": {},
        "targetFormat": "delta",
        "targetDetails": {"database": "silver", "table": "customer", "path": tempfile.mkdtemp()},
        "tableProperties": {},
        "selectExp": [
            "address",
            "email",
            "firstname",
            "id",
            "lastname",
            "operation_date",
            "operation",
            "_rescued_data",
        ],
        "whereClause": ["id IS NOT NULL", "email is not NULL"],
        "partitionColumns": ["operation_date"],
        "cdcApplyChanges": json.dumps(silver_cdc_apply_changes),
        "dataQualityExpectations": """{
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        }""",
        "appendFlows": [],
        "appendFlowsSchemas": {},
        "version": "v1",
        "createDate": datetime.now,
        "createdBy": "dlt-meta-unittest",
        "updateDate": datetime.now,
        "updatedBy": "dlt-meta-unittest",
        "clusterBy": [""],
    }

    # @classmethod
    # def setUp(self):
    #     """Set up initial resources for unit tests."""
    #     super().setUp()
    #     onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
    #     onboardDataFlowSpecs.onboard_dataflow_specs()

    @patch.object(DataflowPipeline, "run_dlt", return_value={"called"})
    def test_invoke_dlt_pipeline_bronz_positive(self, run_dlt):
        """Test for brozne dlt pipeline."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        database = self.onboarding_bronze_silver_params_map["database"]
        bronze_dataflow_table = self.onboarding_bronze_silver_params_map["bronze_dataflowspec_table"]
        self.spark.conf.set("bronze.group", "A1")
        self.spark.conf.set("layer", "bronze")
        self.spark.conf.set(
            "bronze.dataflowspecTable",
            f"{database}.{bronze_dataflow_table}",
        )

        def custom_transform_func(input_df) -> DataFrame:
            return input_df.withColumn('custom_col', lit('test_value'))

        DataflowPipeline.invoke_dlt_pipeline(self.spark, "bronze", custom_transform_func)
        assert run_dlt.called

    @patch.object(DataflowPipeline, "run_dlt", return_value={"called"})
    def test_invoke_dlt_pipeline_silver_positive(self, run_dlt):
        """Test for brozne dlt pipeline."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        database = self.onboarding_bronze_silver_params_map["database"]
        silver_dataflow_table = self.onboarding_bronze_silver_params_map["silver_dataflowspec_table"]
        self.spark.conf.set("silver.group", "A1")
        self.spark.conf.set("layer", "silver")
        self.spark.conf.set(
            "silver.dataflowspecTable",
            f"{database}.{silver_dataflow_table}",
        )
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customers_cdc")
         )
        transactions_parquet_df = self.spark.read.options(**options).json("tests/resources/data/transactions")
        (transactions_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.transactions_cdc")
         )

        def custom_transform_func(input_df) -> DataFrame:
            return input_df.withColumn('custom_col', lit('test_value'))
        DataflowPipeline.invoke_dlt_pipeline(self.spark, "silver", custom_transform_func)
        assert run_dlt.called

    @patch.object(DataflowPipeline, "read", return_value={"called"})
    def test_run_dlt_pipeline_silver_positive(self, read):
        """Test for silver dlt pipeline."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )

        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        )

        self.assertIsNone(dlt_data_flow.silver_schema)
        dlt_data_flow.run_dlt()
        assert read.called

    def test_dataflow_pipeline_constructor_negative(self):
        """Test dataflowpipelines consturctor with negative values."""
        with self.assertRaises(Exception):
            DataflowPipeline(
                self.spark,
                None,
                "inputView",
                None,
            )

    def test_dataflow_pipeline_read_bronze_negative(self):
        """Test dataflowpipeline reading bronze layer."""
        bronze_map = DataflowPipelineTests.bronze_dataflow_spec_map
        bronze_update_map = {"sourceFormat": "orc"}
        bronze_map.update(bronze_update_map)
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        with self.assertRaises(Exception):
            dlt_data_flow.read_bronze()

    def test_dataflow_pipeline_table_has_expectations_positive(self):
        """Test dataflow pipeline tables expectations."""
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        self.assertIsNotNone(dlt_data_flow.table_has_expectations())

    def test_get_silver_schema_positive(self):
        """Test silver schema."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)

    def test_get_silver_schema_where_clause(self):
        """Test silver schema."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)

        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )

        silver_dataflow_spec.whereClause = None
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)
        silver_dataflow_spec.whereClause = [" "]
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(silver_schema)

    def test_read_silver_positive(self):
        """Test silver reader positive."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

        silver_dataflow_spec.whereClause = None
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)
        silver_dataflow_spec.whereClause = [" "]
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

    @patch.object(DataflowPipeline, "get_silver_schema", return_value={"called"})
    def test_read_silver_with_where(self, get_silver_schema):
        """Test silver reader positive."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        silver_df = dlt_data_flow.read_silver()
        self.assertIsNotNone(silver_df)

    @patch.object(DataflowPipeline, "write_bronze_with_dqe", return_value={"called"})
    @patch.object(dlt, "expect_all_or_drop", return_value={"called"})
    def test_broze_write_dqe(self, expect_all_or_drop, write_bronze_with_dqe):
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputView",
            f"{bronze_dataflow_spec.targetDetails['table']}_inputQView",
        )
        dlt_data_flow.write_bronze()
        assert write_bronze_with_dqe.called

    @patch.object(DataflowPipeline, "cdc_apply_changes", return_value={"called"})
    @patch.object(dlt, "expect_all_or_drop", return_value={"called"})
    def test_broze_write_cdc_apply_changes(self, expect_all_or_drop, cdc_apply_changes):
        bronze_dataflow_spec = BronzeDataflowSpec(**DataflowPipelineTests.bronze_dataflow_spec_map)
        cdc_apply_changes_json = """{
            "keys": [
                "id"
            ],
            "sequence_by": "operation_date",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": [
                "operation",
                "operation_date",
                "_rescued_data"
            ]
        }"""
        bronze_dataflow_spec.cdcApplyChanges = cdc_apply_changes_json
        dlt_data_flow = DataflowPipeline(
            self.spark,
            bronze_dataflow_spec,
            f"{bronze_dataflow_spec.targetDetails['table']}_inputView",
            f"{bronze_dataflow_spec.targetDetails['table']}_inputQView",
        )
        dlt_data_flow.write_bronze()
        assert cdc_apply_changes.called

    @patch.object(DataflowPipeline, "cdc_apply_changes", return_value={"called"})
    def test_cdc_apply_changes_scd_type2(self, cdc_apply_changes):
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        silver_dataflow_spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes_scd2)
        self.spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
        options = {"rescuedDataColumn": "_rescued_data", "inferColumnTypes": "true", "multiline": True}
        customers_parquet_df = self.spark.read.options(**options).json("tests/resources/data/customers")
        (customers_parquet_df.withColumn("_rescued_data", lit("Test")).write.format("delta")
            .mode("overwrite").saveAsTable("bronze.customer")
         )
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        dlt_data_flow.cdc_apply_changes()
        assert cdc_apply_changes.called
        dlt_data_flow.cdcApplyChanges.except_column_list = ["operation_date", "_rescued_data"]
        dlt_data_flow.cdc_apply_changes()
        assert cdc_apply_changes.called
        dlt_data_flow.cdc_apply_changes = None
        with self.assertRaises(Exception):
            dlt_data_flow.cdc_apply_changes()

    @patch('dlt.view', new_callable=MagicMock)
    def test_dlt_view_bronze_call(self, mock_view):
        mock_view.view.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        pipeline.read()
        assert mock_view.called_once_with(
            pipeline.read_bronze,
            name=pipeline.view_name,
            comment=f"input dataset view for {pipeline.view_name}"
        )

    @patch('dlt.view', new_callable=MagicMock)
    def test_dlt_view_silver_call(self, mock_view):
        mock_view.view.return_value = None
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        pipeline.read()
        assert mock_view.called_once_with(
            pipeline.read_silver,
            name=pipeline.view_name,
            comment=f"input dataset view for {pipeline.view_name}"
        )

    @patch('dlt.table', new_callable=MagicMock)
    def test_dlt_write_bronze(self, mock_dlt_table):
        mock_dlt_table.table.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        target_path = None
        pipeline.write_bronze()
        assert mock_dlt_table.called_once_with(
            pipeline.write_to_delta,
            name=f"{bronze_dataflow_spec.targetDetails['table']}",
            partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
            table_properties=bronze_dataflow_spec.tableProperties,
            path=target_path,
            comment=f"bronze dlt table{bronze_dataflow_spec.targetDetails['table']}"
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        target_path = bronze_dataflow_spec.targetDetails["path"]
        pipeline.write_bronze()
        assert mock_dlt_table.called_once_with(
            pipeline.write_to_delta,
            name=f"{bronze_dataflow_spec.targetDetails['table']}",
            partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
            table_properties=bronze_dataflow_spec.tableProperties,
            path=target_path,
            comment=f"bronze dlt table{bronze_dataflow_spec.targetDetails['table']}"
        )

    @patch('dlt.table', new_callable=MagicMock)
    def test_dlt_write_silver(self, mock_dlt_table):
        DataflowPipeline.get_silver_schema = MagicMock
        mock_dlt_table.table.return_value = None
        # Arrange
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        target_path = None
        silver_dataflow_spec.cdcApplyChanges = None
        pipeline.write_silver()
        assert mock_dlt_table.called_once_with(
            pipeline.write_to_delta,
            name=f"{silver_dataflow_spec.targetDetails['table']}",
            partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
            table_properties=silver_dataflow_spec.tableProperties,
            path=target_path,
            comment=f"silver dlt table{silver_dataflow_spec.targetDetails['table']}"
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        target_path = silver_dataflow_spec.targetDetails["path"]
        pipeline.write_silver()
        assert mock_dlt_table.called_once_with(
            pipeline.write_to_delta,
            name=f"{silver_dataflow_spec.targetDetails['table']}",
            partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
            table_properties=silver_dataflow_spec.tableProperties,
            path=target_path,
            comment=f"silver dlt table{silver_dataflow_spec.targetDetails['table']}"
        )

    @patch.object(DataflowPipeline, 'write_silver', new_callable=MagicMock)
    def test_dataflowpipeline_silver_write(self, mock_dfp):
        mock_dfp.write_bronze.return_value = None
        DataflowPipeline.get_silver_schema = MagicMock
        silver_dataflow_spec = SilverDataflowSpec(
            **DataflowPipelineTests.silver_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        silver_dataflow_spec.cdcApplyChanges = None
        pipeline.write()
        assert mock_dfp.called

    @patch.object(DataflowPipeline, 'write_bronze', new_callable=MagicMock)
    def test_dataflowpipeline_bronze_write(self, mock_dfp):
        mock_dfp.write_bronze.return_value = None
        DataflowPipeline.get_silver_schema = MagicMock
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze = MagicMock()
        pipeline.view_name = view_name
        bronze_dataflow_spec.cdcApplyChanges = None
        pipeline.write()
        assert mock_dfp.called

    @patch.object(PipelineReaders, 'read_dlt_cloud_files', mock_cloud_files=MagicMock)
    def test_dataflow_pipeline_read_bronze_cloudfiles(self, mock_cloud_files):
        mock_cloud_files.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "cloudFiles"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_cloud_files.called
        bronze_dataflow_spec.sourceFormat = "delta"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        bronze_dataflow_spec.sourceFormat = "eventhub"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        bronze_dataflow_spec.sourceFormat = "kafka"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)

    @patch.object(PipelineReaders, 'read_dlt_delta', mock_read_dlt_delta=MagicMock)
    def test_dataflow_pipeline_read_bronze_delta(self, mock_read_dlt_delta):
        mock_read_dlt_delta.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "delta"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_read_dlt_delta.called

    @patch.object(PipelineReaders, 'read_kafka', mock_read_kafka=MagicMock)
    def test_dataflow_pipeline_read_bronze_kafka(self, mock_read_kafka):
        mock_read_kafka.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(
            **DataflowPipelineTests.bronze_dataflow_spec_map
        )
        bronze_dataflow_spec.sourceFormat = "kafka"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        bronze_dataflow_spec.cdcApplyChanges = None
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_bronze()
        assert mock_read_kafka.called

    def read_dataflowspec(self, database, table):
        return self.spark.read.table(f"{database}.{table}")

    @patch('dlt.table', new_callable=MagicMock)
    @patch('dlt.expect_all', new_callable=MagicMock)
    @patch('dlt.expect_all_or_fail', new_callable=MagicMock)
    @patch('dlt.expect_all_or_drop', new_callable=MagicMock)
    def test_dataflowpipeline_bronze_dqe(self,
                                         mock_dlt_table,
                                         mock_dlt_expect_all,
                                         mock_dlt_expect_all_or_fail,
                                         mock_dlt_expect_all_or_drop):
        mock_dlt_table.return_value = None
        mock_dlt_expect_all.return_value = None
        mock_dlt_expect_all_or_fail.return_value = None
        mock_dlt_expect_all_or_drop.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        data_quality_expectations_json = json.loads(bronze_dataflow_spec.dataQualityExpectations)
        expect_dict = {}
        if "expect" in data_quality_expectations_json or "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect"])
        if "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect_all"])
        if "expect_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        if "expect_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        target_path = bronze_dataflow_spec.targetDetails["path"]
        ddlSchemaStr = self.spark.read.text(paths="tests/resources/schema/products.ddl",
                                            wholetext=True).collect()[0]["value"]
        struct_schema = T._parse_datatype_string(ddlSchemaStr)
        pipeline.write_bronze()
        assert mock_dlt_table.called_once_with(
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
            table_properties=bronze_dataflowSpec_df.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
            path=target_path,
            schema=struct_schema,
            comment=f"bronze dlt table{bronze_dataflow_spec.targetDetails['table']}",
        )
        assert mock_dlt_expect_all_or_drop.called_once_with(expect_or_drop_dict)
        assert mock_dlt_expect_all_or_fail.called_once_with(expect_or_fail_dict)
        assert mock_dlt_expect_all.called_once_with(expect_dict)
        assert mock_dlt_expect_all_or_drop.expect_all_or_drop(expect_or_quarantine_dict)

    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    @patch('dlt.create_streaming_live_table', new_callable=MagicMock)
    @patch('dlt.apply_changes', new_callable=MagicMock)
    @patch.object(DataflowPipeline, 'get_silver_schema', new_callable=MagicMock)
    def test_dataflowpipeline_silver_cdc_apply_changes(self,
                                                       mock_create_streaming_table,
                                                       mock_create_streaming_live_table,
                                                       mock_apply_changes,
                                                       mock_get_silver_schema):
        mock_create_streaming_table.return_value = None
        mock_create_streaming_live_table.return_value = None
        mock_apply_changes.apply_changes.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_path']
        )
        bronze_df_row = silver_dataflowSpec_df.filter(silver_dataflowSpec_df.dataFlowId == "201").collect()[0]
        silver_dataflow_spec = SilverDataflowSpec(**bronze_df_row.asDict())
        data_quality_expectations_json = json.loads(silver_dataflow_spec.dataQualityExpectations)
        expect_dict = {}
        expect_or_fail_dict = {}
        expect_or_drop_dict = {}
        if "expect" in data_quality_expectations_json or "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect"])
        if "expect_all" in data_quality_expectations_json:
            expect_dict.update(data_quality_expectations_json["expect_all"])
        if "expect_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict.update(data_quality_expectations_json["expect_or_fail"])
        if "expect_all_or_fail" in data_quality_expectations_json:
            expect_or_fail_dict.update(data_quality_expectations_json["expect_all_or_fail"])
        if "expect_all_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict.update(data_quality_expectations_json["expect_all_or_drop"])
        if "expect_or_drop" in data_quality_expectations_json:
            expect_or_drop_dict.update(data_quality_expectations_json["expect_or_drop"])
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        target_path = silver_dataflow_spec.targetDetails["path"]
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(silver_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)
        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        ddlSchemaStr = self.spark.read.text(paths="tests/resources/schema/products.ddl",
                                            wholetext=True).collect()[0]["value"]
        struct_schema = T._parse_datatype_string(ddlSchemaStr)
        mock_get_silver_schema.return_value = json.dumps(struct_schema.jsonValue())
        pipeline.silver_schema = struct_schema
        pipeline.write_silver()
        assert mock_create_streaming_table.called_once_with(
            schema=struct_schema,
            name=f"{silver_dataflowSpec_df.targetDetails['table']}"
        )
        assert mock_create_streaming_live_table.called_once_with(
            name=f"{silver_dataflowSpec_df.targetDetails['table']}",
            table_properties=silver_dataflowSpec_df.tableProperties,
            path=target_path,
            schema=struct_schema,
            expect_all=expect_dict,
            expect_all_or_drop=expect_or_drop_dict,
            expect_all_or_fail=expect_or_fail_dict
        )
        assert mock_apply_changes.called_once_with(
            name=f"{silver_dataflowSpec_df.targetDetails['table']}",
            source=view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=cdc_apply_changes.sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list
        )

    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    @patch('dlt.create_streaming_live_table', new_callable=MagicMock)
    @patch('dlt.apply_changes', new_callable=MagicMock)
    def test_bronze_cdc_apply_changes(self,
                                      mock_create_streaming_table,
                                      mock_create_streaming_live_table,
                                      mock_apply_changes):
        mock_create_streaming_table.return_value = None
        mock_apply_changes.apply_changes.return_value = None
        mock_create_streaming_live_table.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_bronze_type2_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(bronze_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        struct_schema = json.loads(bronze_dataflow_spec.schema)
        pipeline.write_bronze()
        assert mock_create_streaming_table.called_once_with(
            schema=struct_schema,
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}"
        )
        assert mock_create_streaming_live_table.called_once_with(
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
            table_properties=bronze_dataflowSpec_df.tableProperties,
            path=bronze_dataflowSpec_df.targetDetails["path"],
            schema=struct_schema,
            expect_all=None,
            expect_all_or_drop=None,
            expect_all_or_fail=None
        )
        assert mock_apply_changes.called_once_with(
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
            source=view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=cdc_apply_changes.sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list
        )

    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    @patch('dlt.create_streaming_live_table', new_callable=MagicMock)
    @patch('dlt.apply_changes', new_callable=MagicMock)
    def test_bronze_cdc_apply_changes_v7(self,
                                         mock_create_streaming_table,
                                         mock_create_streaming_live_table,
                                         mock_apply_changes):
        mock_create_streaming_table.return_value = None
        mock_apply_changes.apply_changes.return_value = None
        mock_create_streaming_live_table.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_json_v7_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_row_dict = DataflowSpecUtils.populate_additional_df_cols(
            bronze_df_row.asDict(),
            DataflowSpecUtils.additional_bronze_df_columns
        )
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_row_dict)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(bronze_dataflow_spec.cdcApplyChanges)
        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        struct_schema = json.loads(bronze_dataflow_spec.schema)
        pipeline.write_bronze()
        assert mock_create_streaming_table.called_once_with(
            schema=struct_schema,
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}"
        )
        assert mock_create_streaming_live_table.called_once_with(
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
            table_properties=bronze_dataflowSpec_df.tableProperties,
            path=bronze_dataflowSpec_df.targetDetails["path"],
            schema=struct_schema,
            expect_all=None,
            expect_all_or_drop=None,
            expect_all_or_fail=None
        )
        assert mock_apply_changes.called_once_with(
            name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
            source=view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=cdc_apply_changes.sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list
        )

    @patch.object(DataflowPipeline, "create_streaming_table", new_callable=MagicMock)
    @patch.object(DataflowPipeline, "write_to_delta", new_callable=MagicMock)
    @patch('dlt.create_streaming_live_table', new_callable=MagicMock)
    @patch('dlt.append_flow', new_callable=MagicMock)
    def test_bronze_append_flow_positive(self,
                                         mock_create_streaming_table,
                                         mock_write_to_delta,
                                         mock_create_streaming_live_table,
                                         mock_append_flow):
        mock_create_streaming_table.return_value = None
        mock_write_to_delta.return_value = None
        mock_create_streaming_live_table.return_value = None
        mock_append_flow.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_append_flow_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        struct_schema = json.loads(bronze_dataflow_spec.schema)
        append_flows = DataflowSpecUtils.get_append_flows(bronze_dataflow_spec.appendFlows)
        pipeline.write_bronze()
        for append_flow in append_flows:
            assert mock_create_streaming_table.called_once_with(
                schema=struct_schema,
                name=f"{bronze_dataflowSpec_df.targetDetails['table']}"
            )
            assert mock_create_streaming_live_table.called_once_with(
                name=f"{bronze_dataflowSpec_df.targetDetails['table']}",
                table_properties=bronze_dataflowSpec_df.tableProperties,
                path=bronze_dataflowSpec_df.targetDetails["path"],
                schema=struct_schema,
                expect_all=None,
                expect_all_or_drop=None,
                expect_all_or_fail=None
            )
            target_table = bronze_dataflow_spec.targetDetails["table"]
            assert mock_append_flow.called_once_with(
                name=append_flow.name,
                target=target_table,
                comment=f"append_flow={append_flow.name} for target={target_table}",
                spark_conf=append_flow.spark_conf,
                once=append_flow.once
            )(mock_write_to_delta.called_once())

    def test_get_dq_expectations(self):
        o_dfs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = pipeline.get_dq_expectations()
        self.assertIsNotNone(expect_all_or_drop_dict)
        self.assertIsNone(expect_all_or_fail_dict)
        self.assertIsNone(expect_all_dict)

    @patch('dlt.view', new_callable=MagicMock)
    def test_read_append_flows(self, mock_view):
        mock_view.view.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_append_flow_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "100").collect()[0]
        silver_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(silver_dataflow_spec.appendFlows)[0]
        pipeline_reader = PipelineReaders(
            self.spark,
            append_flow.source_format,
            append_flow.source_details,
            append_flow.reader_options
        )
        assert mock_view.called_once_with(
            pipeline_reader.read_dlt_cloud_files,
            name=f"{append_flow.name}_view",
            comment=f"append flow input dataset view for{append_flow.name}_view")

        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "103").collect()[0]
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_df_row.asDict())
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(bronze_dataflow_spec.appendFlows)[0]
        pipeline_reader = PipelineReaders(
            self.spark,
            append_flow.source_format,
            append_flow.source_details,
            append_flow.reader_options
        )
        assert mock_view.called_once_with(
            pipeline_reader.read_kafka,
            name=f"{append_flow.name}_view",
            comment=f"append flow input dataset view for{append_flow.name}_view")

        silver_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_path']
        )
        silver_df_row = silver_dataflowSpec_df.filter(silver_dataflowSpec_df.dataFlowId == "101").collect()[0]
        silver_dataflow_spec = SilverDataflowSpec(**silver_df_row.asDict())
        view_name = f"{silver_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, silver_dataflow_spec, view_name, None)
        pipeline.read_append_flows()
        append_flow = DataflowSpecUtils.get_append_flows(silver_dataflow_spec.appendFlows)[0]
        pipeline_reader = PipelineReaders(
            self.spark,
            append_flow.source_format,
            append_flow.source_details,
            append_flow.reader_options
        )
        assert mock_view.called_once_with(
            pipeline_reader.read_dlt_delta,
            name=f"{append_flow.name}_view",
            comment=f"append flow input dataset view for{append_flow.name}_view")
        bronze_dataflowSpec_df.appendFlows = None
        with self.assertRaises(Exception):
            pipeline = DataflowPipeline(self.spark, bronze_dataflowSpec_df, view_name, None)

    def test_get_dq_expectations_with_expect_all(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboarding_params_map['onboarding_file_path'] = self.onboarding_type2_json_file
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map)
        o_dfs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_path']
        )
        bronze_df_row = bronze_dataflowSpec_df.filter(bronze_dataflowSpec_df.dataFlowId == "201").collect()[0]
        bronze_row_dict = DataflowSpecUtils.populate_additional_df_cols(
            bronze_df_row.asDict(),
            DataflowSpecUtils.additional_bronze_df_columns
        )
        bronze_dataflow_spec = BronzeDataflowSpec(**bronze_row_dict)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = pipeline.get_dq_expectations()
        self.assertIsNotNone(expect_all_dict)
        self.assertIsNotNone(expect_all_or_drop_dict)
        self.assertIsNotNone(expect_all_or_fail_dict)

    @patch('dlt.table', new_callable=MagicMock)
    def test_modify_schema_for_cdc_changes(self, mock_dlt_table):
        mock_dlt_table.table.return_value = None
        cdc_apply_changes_json = """{
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "2",
            "except_column_list": ["operation", "operation_date", "_rescued_data"]
        }"""
        cdc_apply_changes = DataflowSpecUtils.get_cdc_apply_changes(cdc_apply_changes_json)
        bmap = DataflowPipelineTests.bronze_dataflow_spec_map
        ddlSchemaStr = (
            self.spark.read.text(paths="tests/resources/schema/customer_schema.ddl")
            .select("value")
            .collect()[0]["value"]
        )
        schema = T._parse_datatype_string(ddlSchemaStr)
        bronze_dataflow_spec = BronzeDataflowSpec(
            **bmap
        )
        bronze_dataflow_spec.schema = json.dumps(schema.jsonValue())
        bronze_dataflow_spec.cdcApplyChanges = json.dumps(self.silver_cdc_apply_changes_scd2)
        bronze_dataflow_spec.dataQualityExpectations = None
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        expected_schema = T.StructType([
            T.StructField("address", T.StringType()),
            T.StructField("email", T.StringType()),
            T.StructField("firstname", T.StringType()),
            T.StructField("id", T.StringType()),
            T.StructField("lastname", T.StringType()),
            T.StructField("__START_AT", T.StringType()),
            T.StructField("__END_AT", T.StringType())
        ])
        modified_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertEqual(modified_schema, expected_schema)
        pipeline.schema_json = None
        modified_schema = pipeline.modify_schema_for_cdc_changes(cdc_apply_changes)
        self.assertEqual(modified_schema, None)

    @patch.object(dlt, 'create_streaming_table', return_value={"called"})
    @patch.object(dlt, 'apply_changes_from_snapshot', return_value={"called"})
    def test_apply_changes_from_snapshot(self, mock_apply_changes_from_snapshot, mock_create_streaming_table):
        """Test apply_changes_from_snapshot method."""

        def next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
            latest_snapshot_version = latest_snapshot_version or 0
            next_version = latest_snapshot_version + 1
            bronze_dataflow_spec: BronzeDataflowSpec = dataflow_spec
            options = bronze_dataflow_spec.readerConfigOptions
            snapshot_format = bronze_dataflow_spec.sourceDetails["snapshot_format"]
            snapshot_root_path = bronze_dataflow_spec.sourceDetails['path']
            snapshot_path = f"{snapshot_root_path}{next_version}.csv"
            snapshot = self.spark.read.format(snapshot_format).options(**options).load(snapshot_path)
            return (snapshot, next_version)

        mock_create_streaming_table.return_value = None
        mock_apply_changes_from_snapshot.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name,
                                    next_snapshot_and_version=next_snapshot_and_version)
        pipeline.apply_changes_from_snapshot()
        dlt.called

    @patch.object(dlt, 'create_streaming_table', return_value={"called"})
    @patch.object(dlt, 'apply_changes_from_snapshot', return_value={"called"})
    def test_apply_changes_from_snapshot_uc_enabled(self,
                                                    mock_apply_changes_from_snapshot,
                                                    mock_create_streaming_table):
        """Test apply_changes_from_snapshot method with Unity Catalog enabled."""
        def next_snapshot_and_version(latest_snapshot_version, dataflow_spec):
            latest_snapshot_version = latest_snapshot_version or 0
            next_version = latest_snapshot_version + 1
            bronze_dataflow_spec: BronzeDataflowSpec = dataflow_spec
            options = bronze_dataflow_spec.readerConfigOptions
            snapshot_format = bronze_dataflow_spec.sourceDetails["snapshot_format"]
            snapshot_root_path = bronze_dataflow_spec.sourceDetails['path']
            snapshot_path = f"{snapshot_root_path}{next_version}.csv"
            snapshot = self.spark.read.format(snapshot_format).options(**options).load(snapshot_path)
            return (snapshot, next_version)
        mock_create_streaming_table.return_value = None
        mock_apply_changes_from_snapshot.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name,
                                    next_snapshot_and_version=next_snapshot_and_version)
        pipeline.apply_changes_from_snapshot()
        dlt.called

    # @patch('pyspark.sql.SparkSession.readStream')
    # def test_get_silver_schema_uc_enabled(self, mock_read_stream):
    #     """Test get_silver_schema with Unity Catalog enabled."""
    #     silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
    #     source_details = {
    #         "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
    #     }
    #     silver_spec_map.update(source_details)
    #     silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
    #     self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
    #     mock_read_stream.table.return_value.selectExpr.return_value = raw_delta_table_stream
    #     dlt_data_flow = DataflowPipeline(
    #         self.spark,
    #         silver_dataflow_spec,
    #         f"{silver_dataflow_spec.targetDetails['table']}_inputView",
    #         None,
    #     )
    #     schema = dlt_data_flow.get_silver_schema()
    #     self.assertIsNotNone(schema)
        # mock_read_stream.table.assert_called_once_with("bronze.customer")
        # mock_read_stream.table.return_value.selectExpr.assert_called_once_with(*silver_dataflow_spec.selectExp)

    @patch.object(DataflowSpecUtils, 'get_bronze_dataflow_spec', return_value=[MagicMock()])
    @patch.object(DataflowSpecUtils, 'get_silver_dataflow_spec', return_value=[MagicMock()])
    @patch.object(DataflowPipeline, '_launch_dlt_flow', return_value=None)
    def test_invoke_dlt_pipeline_bronze_silver(
        self, mock_launch_dlt_flow, mock_get_silver_dataflow_spec, mock_get_bronze_dataflow_spec
    ):
        """Test invoke_dlt_pipeline for bronze_silver layer."""
        spark = MagicMock()
        bronze_custom_transform_func = MagicMock()
        silver_custom_transform_func = MagicMock()

        DataflowPipeline.invoke_dlt_pipeline(
            spark, "bronze_silver", bronze_custom_transform_func, silver_custom_transform_func
        )

        mock_get_bronze_dataflow_spec.assert_called_once_with(spark)
        mock_get_silver_dataflow_spec.assert_called_once_with(spark)
        mock_launch_dlt_flow.assert_any_call(
            spark, "bronze", mock_get_bronze_dataflow_spec.return_value, bronze_custom_transform_func
        )
        mock_launch_dlt_flow.assert_any_call(
            spark, "silver", mock_get_silver_dataflow_spec.return_value, silver_custom_transform_func
        )

    @patch.object(dlt, 'create_streaming_table', return_value={"called"})
    @patch.object(dlt, 'apply_changes_from_snapshot', return_value={"called"})
    def test_read_unsupported_dataflow(self, mock_apply_changes_from_snapshot, mock_create_streaming_table):
        """Test apply_changes_from_snapshot method."""
        mock_create_streaming_table.return_value = None
        mock_apply_changes_from_snapshot.return_value = None
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name)

        class UnsupportedDataflowSpec:
            pass
        unsupported_dataflow_spec = UnsupportedDataflowSpec()
        pipeline.dataflowSpec = unsupported_dataflow_spec
        with self.assertRaises(Exception) as context:
            pipeline.read()
        self.assertTrue("Dataflow read not supported" in str(context.exception))

    @patch.object(DataflowPipeline, 'apply_changes_from_snapshot', return_value=None)
    def test_write_bronze_snapshot(self, mock_apply_changes_from_snapshot):
        """Test write_bronze with snapshot source format."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_acs_map)
        bronze_dataflow_spec.sourceFormat = "snapshot"
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(
            self.spark, bronze_dataflow_spec, view_name, None, next_snapshot_and_version=MagicMock()
        )
        pipeline.write_bronze()
        assert mock_apply_changes_from_snapshot.called

    @patch.object(DataflowPipeline, 'write_bronze_with_dqe', return_value=None)
    def test_write_bronze_with_dqe(self, mock_write_bronze_with_dqe):
        """Test write_bronze with data quality expectations."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_map)
        bronze_dataflow_spec.dataQualityExpectations = json.dumps({
            "expect_or_drop": {
                "no_rescued_data": "_rescued_data IS NULL",
                "valid_id": "id IS NOT NULL",
                "valid_operation": "operation IN ('APPEND', 'DELETE', 'UPDATE')"
            }
        })
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.write_bronze()
        assert mock_write_bronze_with_dqe.called

    @patch.object(DataflowPipeline, 'cdc_apply_changes', return_value=None)
    def test_write_bronze_cdc_apply_changes(self, mock_cdc_apply_changes):
        """Test write_bronze with CDC apply changes."""
        bronze_dataflow_spec = BronzeDataflowSpec(**self.bronze_dataflow_spec_map)
        bronze_dataflow_spec.cdcApplyChanges = json.dumps({
            "keys": ["id"],
            "sequence_by": "operation_date",
            "scd_type": "1",
            "apply_as_deletes": "operation = 'DELETE'",
            "except_column_list": ["operation", "operation_date", "_rescued_data"]
        })
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, bronze_dataflow_spec, view_name, None)
        pipeline.write_bronze()
        assert mock_cdc_apply_changes.called

    @patch('pyspark.sql.SparkSession.readStream')
    def test_get_silver_schema_uc_enabled(self, mock_read_stream):
        """Test get_silver_schema with Unity Catalog enabled."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        mock_read_stream.table.return_value.selectExpr.return_value = raw_delta_table_stream
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(schema)
        # mock_read_stream.table.assert_called_once_with("bronze.customer")
        # mock_read_stream.table.return_value.selectExpr.assert_called_once_with(*silver_dataflow_spec.selectExp)

    @patch('pyspark.sql.SparkSession.readStream')
    def test_get_silver_schema_uc_disabled(self, mock_read_stream):
        """Test get_silver_schema with Unity Catalog disabled."""
        silver_spec_map = DataflowPipelineTests.silver_dataflow_spec_map
        source_details = {
            "sourceDetails": {"database": "bronze", "table": "customer", "path": "tests/resources/delta/customers"}
        }
        silver_spec_map.update(source_details)
        silver_dataflow_spec = SilverDataflowSpec(**silver_spec_map)
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "False")
        mock_read_stream.load.return_value.selectExpr.return_value = raw_delta_table_stream
        dlt_data_flow = DataflowPipeline(
            self.spark,
            silver_dataflow_spec,
            f"{silver_dataflow_spec.targetDetails['table']}_inputView",
            None,
        )
        schema = dlt_data_flow.get_silver_schema()
        self.assertIsNotNone(schema)
        # mock_read_stream.load.assert_called_once_with(
        #     path=silver_dataflow_spec.sourceDetails["path"],
        #     format="delta"
        # )
        # mock_read_stream.load.return_value.selectExpr.assert_called_once_with(*silver_dataflow_spec.selectExp)
