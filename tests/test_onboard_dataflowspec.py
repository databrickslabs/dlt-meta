"""Test OnboardDataflowSpec class."""
from tests.utils import DLTFrameworkTestCase
import copy
from src.onboard_dataflowspec import OnboardDataflowspec
from src.dataflow_spec import BronzeDataflowSpec


class OnboardDataflowspecTests(DLTFrameworkTestCase):
    """OnboardDataflowSpec Unit Test ."""

    def test_validate_params_for_onboardBronzeDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_bronze_dataflow_spec()

    def test_validate_params_for_onboardSilverDataflowSpec(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]

        for key in onboarding_params_map:
            test_onboarding_params_map = copy.deepcopy(onboarding_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_silver_dataflow_spec()

    def test_validate_params_for_onboardDataFlowSpecs(self):
        """Test for onboardDataflowspec parameters."""
        for key in self.onboarding_bronze_silver_params_map:
            test_onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
            del test_onboarding_params_map[key]
            with self.assertRaises(ValueError):
                OnboardDataflowspec(self.spark, test_onboarding_params_map).onboard_dataflow_specs()

    def test_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboardDataFlowSpecs_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            if bronze_row.dataFlowId in ["100", "101"]:
                self.assertIsNone(bronze_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if bronze_row.dataFlowId == "103":
                self.assertEqual(bronze_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardBronzeDataflowSpec_positive(self):
        """Test for onboardDataflowspec."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

    def test_getOnboardingFileDataframe_for_unsupported_file(self):
        """Test onboardingFiles not supported."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_unsupported_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_onboardSilverDataflowSpec_positive(self):
        """Test Silverdataflowspec positive."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_dataflow_ids_dup_onboard(self):
        """Test dataflow for duplicate ids."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_json_dups
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_validate_mandatory_fields_bronze(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_bronze_dataflow_spec()

    def test_validate_mandatory_fields_silver(self):
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        onboarding_params_map["onboarding_file_path"] = self.onboarding_missing_keys_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, onboarding_params_map)
        with self.assertRaises(Exception):
            onboardDataFlowSpecs.onboard_silver_dataflow_spec()
