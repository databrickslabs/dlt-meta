"""Test DataflowSpec script."""
from tests.utils import DLTFrameworkTestCase
from src.dataflow_spec import (
    DataflowSpecUtils,
    CDCApplyChanges,
    BronzeDataflowSpec,
    SilverDataflowSpec,
)
from src.onboard_dataflowspec import OnboardDataflowspec
import copy


class DataFlowSpecTests(DLTFrameworkTestCase):
    """Test DataflowSpec script."""

    def test_checkSparkDataFlowpipelineSparkConfParams_negative(self):
        """Test spark paramters passed from dlt notebook."""
        layer = "bronze"
        with self.assertRaises(Exception):
            DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(self.spark, layer)

        self.spark.conf.set("layer", layer)
        with self.assertRaises(Exception):
            DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(self.spark, layer)

        self.spark.conf.set(f"{layer}.dataflowspecTable", "cdc_dataflowSpec")
        with self.assertRaises(Exception):
            DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(self.spark, layer)
        self.spark.conf.unset("layer")
        self.spark.conf.unset(f"{layer}.dataflowspecTable")

    def test_checkSparkDataFlowpipelineSparkConfParams_positive(self):
        """Test spark paramters passed from dlt notebook."""
        layer = "bronze"
        self.spark.conf.set("layer", layer)
        self.spark.conf.set(f"{layer}.dataflowspecTable", "cdc_dataflowSpec")
        self.spark.conf.set(f"{layer}.group", "A1")
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(self.spark, layer)

        self.spark.conf.unset(f"{layer}.group")
        self.spark.conf.set(f"{layer}.dataflowIds", "1,2")
        DataflowSpecUtils.check_spark_dataflowpipeline_conf_params(self.spark, layer)
        self.spark.conf.unset("layer")
        self.spark.conf.unset(f"{layer}.dataflowspecTable")

    def test_getBronzeDataflowSpec_positive(self):
        """Test Dataflowspec for Bronze layer."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

        bronze_dataflowSpec_path = self.onboarding_spec_paths + "/bronze"
        self.spark.sql("CREATE DATABASE if not exists " + onboarding_params_map["database"])

        bronze_table_name = f"{onboarding_params_map['database']}.{onboarding_params_map['bronze_dataflowspec_table']}"
        self.spark.sql(
            "CREATE TABLE if not exists "
            + bronze_table_name
            + " USING DELTA LOCATION '"
            + bronze_dataflowSpec_path
            + "'"
        )

        self.spark.conf.set("layer", "bronze")
        self.spark.conf.set("bronze.group", "A1")
        self.spark.conf.set("bronze.dataflowspecTable", bronze_table_name)

        dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(self.spark)
        self.assertEqual(len(dataflowspec_list), 2)
        dataflowspec = dataflowspec_list[0]
        self.assertEqual(type(dataflowspec), BronzeDataflowSpec)

        dataflowspec_list = DataflowSpecUtils._get_dataflow_spec(self.spark, "bronze").collect()
        self.assertEqual(len(dataflowspec_list), 2)

        self.spark.conf.unset("layer")
        self.spark.conf.unset("bronze.group")
        self.spark.conf.unset("bronze.dataflowspecTable")

    def test_getSilverDataflowSpec_positive(self):
        """Test silverdataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

        self.spark.sql("CREATE DATABASE if not exists " + onboarding_params_map["database"])

        silver_dataflowSpec_path = self.onboarding_spec_paths + "/silver"
        silver_table_name = f"{onboarding_params_map['database']}.{onboarding_params_map['silver_dataflowspec_table']}"

        self.spark.sql(
            "CREATE TABLE if not exists "
            + silver_table_name
            + " USING DELTA LOCATION '"
            + silver_dataflowSpec_path
            + "'"
        )

        self.spark.conf.set("layer", "silver")
        self.spark.conf.set("silver.group", "A1")
        self.spark.conf.set("silver.dataflowspecTable", silver_table_name)

        dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(self.spark)
        self.assertEqual(len(dataflowspec_list), 2)
        dataflowspec = dataflowspec_list[0]
        self.assertEqual(type(dataflowspec), SilverDataflowSpec)

        dataflowspec_list = DataflowSpecUtils._get_dataflow_spec(self.spark, "silver").collect()
        self.assertEqual(len(dataflowspec_list), 2)

        self.spark.conf.unset("layer")
        self.spark.conf.unset("silver.group")
        self.spark.conf.unset("silver.dataflowspecTable")

    def test_get_partition_cols_negative_values(self):
        """Test partitions cols with negative values."""
        partition_cols_list_of_possible_values = [[""], [], "", "", [""], None]
        for partition_cols in partition_cols_list_of_possible_values:
            self.assertEqual(DataflowSpecUtils.get_partition_cols(partition_cols), None)

    def test_get_partition_cols_positive_values(self):
        """Test partitions cols with negative values."""
        partition_cols_list_of_possible_values = [["col1"], ["col1", "col2"]]
        for partition_cols in partition_cols_list_of_possible_values:
            self.assertEqual(DataflowSpecUtils.get_partition_cols(partition_cols), partition_cols)
        partition_cols_with_empty_col_value = ["col1", "", "", "col2", "", ""]
        self.assertEqual(
            DataflowSpecUtils.get_partition_cols(partition_cols_with_empty_col_value),
            ["col1", "col2"],
        )

    def test_getCdcApplyChanges_negative(self):
        """Test cdcApplychanges dlt api with negative values."""
        silver_cdc_apply_changes = """{"sequence_by" : "sequenceNum", "scd_type" : "1"}"""
        with self.assertRaises(Exception):
            DataflowSpecUtils.get_cdc_apply_changes(silver_cdc_apply_changes)
        silver_cdc_apply_changes = """{"keys" : ["playerId"], "scd_type" : "1"}"""
        with self.assertRaises(Exception):
            DataflowSpecUtils.get_cdc_apply_changes(silver_cdc_apply_changes)
        silver_cdc_apply_changes = """{"keys" : ["playerId"],"sequence_by" : "sequenceNum"}"""
        with self.assertRaises(Exception):
            DataflowSpecUtils.get_cdc_apply_changes(silver_cdc_apply_changes)

    def test_getCdcApplyChanges_positive(self):
        """Test cdcApplychanges dlt api with positive values."""
        silver_cdc_apply_changes = """{"keys" : ["playerId"],"sequence_by" : "sequenceNum", "scd_type" : "1"}"""
        cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(silver_cdc_apply_changes)
        self.assertEqual(type(cdcApplyChanges), CDCApplyChanges)
        self.assertEqual(cdcApplyChanges.keys, ["playerId"])
        self.assertEqual(cdcApplyChanges.sequence_by, "sequenceNum")
        self.assertEqual(cdcApplyChanges.where, None)
        self.assertEqual(cdcApplyChanges.ignore_null_updates, False)
        self.assertEqual(cdcApplyChanges.apply_as_deletes, None)
        self.assertEqual(cdcApplyChanges.apply_as_truncates, None)
        self.assertEqual(cdcApplyChanges.column_list, None)
        self.assertEqual(cdcApplyChanges.except_column_list, None)
        self.assertEqual(cdcApplyChanges.scd_type, "1")

    def test_get_schema_json_positive(self):
        """Test schema json positive."""
        dataflowSpec = BronzeDataflowSpec(
            1,
            "A1",
            "json",
            {"path": "tests/resources/silver_transformations.json"},
            {"cloudFiles.format": "json"},
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        spark_schema = DataflowSpecUtils.get_schema_json(self.spark, dataflowSpec)
        self.assertIsNotNone(spark_schema)
