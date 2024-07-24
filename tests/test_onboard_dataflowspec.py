"""Test OnboardDataflowSpec class."""
import copy
from tests.utils import DLTFrameworkTestCase
from src.onboard_dataflowspec import OnboardDataflowspec
from src.dataflow_spec import BronzeDataflowSpec
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame


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

    def test_validate_params_for_onboardSilverDataflowSpec_uc(self):
        """Test for onboardDataflowspec parameters."""
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboard_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        print(onboard_dfs.bronze_dict_obj)
        print(onboard_dfs.silver_dict_obj)
        self.assertNotIn('silver_dataflowspec_path', onboard_dfs.bronze_dict_obj)
        self.assertNotIn('bronze_dataflowspec_path', onboard_dfs.silver_dict_obj)

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

    def test_upgrade_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_onboardDataFlowSpecs_positive(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def read_dataflowspec(self, database, table):
        return self.spark.read.table(f"{database}.{table}")

    def test_onboardDataFlowSpecs_with_uc_enabled(self):
        """Test for onboardDataflowspec."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark,
                                                   self.onboarding_bronze_silver_params_uc_map,
                                                   uc_enabled=True)
        self.assertNotIn('bronze_dataflowspec_path', onboardDataFlowSpecs.bronze_dict_obj)
        self.assertNotIn('silver_dataflowspec_path', onboardDataFlowSpecs.silver_dict_obj)

    @patch.object(OnboardDataflowspec, 'onboard_bronze_dataflow_spec', new_callable=MagicMock())
    @patch.object(OnboardDataflowspec, 'onboard_silver_dataflow_spec', new_callable=MagicMock())
    def test_onboardDataFlowSpecs_validate_with_uc_enabled(self, mock_bronze, mock_silver):
        """Test for onboardDataflowspec."""
        mock_bronze.return_value = None
        mock_silver.return_value = None
        bronze_silver_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del bronze_silver_params_map["uc_enabled"]
        OnboardDataflowspec(self.spark,
                            bronze_silver_params_map,
                            uc_enabled=True).onboard_dataflow_specs()
        assert mock_bronze.called
        assert mock_silver.called

    def test_onboardDataFlowSpecs_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            if bronze_row.dataFlowId in ["100", "101"]:
                self.assertIsNone(bronze_row.readerConfigOptions.get("cloudFiles.rescuedDataColumn"))
            if bronze_row.dataFlowId == "103":
                self.assertEqual(bronze_row.readerConfigOptions.get("maxOffsetsPerTrigger"), "60000")

    def test_onboardDataFlowSpecs_with_merge_uc(self):
        """Test for onboardDataflowspec with merge scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        local_params["onboarding_file_path"] = self.onboarding_json_file
        del local_params["uc_enabled"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params, uc_enabled=True)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        bronze_df_rows = bronze_dataflowSpec_df.collect()
        for bronze_df_row in bronze_df_rows:
            bronze_row = BronzeDataflowSpec(**bronze_df_row.asDict())
            if bronze_row.dataFlowId in ["101", "102"]:
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
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
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
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
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

    def test_onboardSilverDataflowSpec_with_merge(self):
        """Test for onboardDataflowspec with merge scenario."""
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["overwrite"] = "False"
        local_params["onboarding_file_path"] = self.onboarding_v2_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_silver_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None

        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["bronze_dataflowspec_table"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        del onboarding_params_map["silver_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_silver_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["silver_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    @patch.object(DataFrame, "write", new_callable=MagicMock)
    def test_bronze_dataflow_spec_dataframe_withuc(self, mock_write):
        """Test for onboardDataflowspec with merge scenario."""
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.return_value = None
        onboarding_params_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        del onboarding_params_map["uc_enabled"]
        del onboarding_params_map["silver_dataflowspec_table"]
        del onboarding_params_map["silver_dataflowspec_path"]
        del onboarding_params_map["bronze_dataflowspec_path"]
        print(onboarding_params_map)
        o_dfs = OnboardDataflowspec(self.spark, onboarding_params_map, uc_enabled=True)
        o_dfs.onboard_bronze_dataflow_spec()
        # Assert
        database = onboarding_params_map["database"]
        table = onboarding_params_map["bronze_dataflowspec_table"]
        mock_write.format.assert_called_once_with("delta")
        mock_write.format.return_value.mode.assert_called_once_with("overwrite")
        mock_write.format.return_value.mode.return_value.option.assert_called_once_with("mergeSchema", "true")
        mock_write.format.return_value.mode.return_value.option.return_value.saveAsTable.assert_called_once_with(
            f"{database}.{table}")

    def test_bronze_dataflow_spec_append_flow(self):
        """Test for onboardDataflowspec with appendflow scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_append_flow_json_file
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        bronze_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['bronze_dataflowspec_table'])
        bronze_dataflowSpec_df.show(truncate=False)
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_silver_fanout_dataflow_spec_dataframe(self):
        """Test for onboardDataflowspec with fanout scenario."""
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_dataflow_specs()
        local_params["onboarding_file_path"] = self.onboarding_silver_fanout_json_file
        del local_params["bronze_dataflowspec_table"]
        del local_params["bronze_dataflowspec_path"]
        local_params["overwrite"] = "False"
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_silver_dataflow_spec()
        silver_dataflowSpec_df = self.read_dataflowspec(
            self.onboarding_bronze_silver_params_map['database'],
            self.onboarding_bronze_silver_params_map['silver_dataflowspec_table'])
        silver_dataflowSpec_df.show(truncate=False)
        self.assertEqual(silver_dataflowSpec_df.count(), 4)
