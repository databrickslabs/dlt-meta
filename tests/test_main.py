"""Test Main class."""
from tests.utils import DLTFrameworkTestCase
import sys
import copy
from src import __main__


class MainTests(DLTFrameworkTestCase):
    """Main Unit Test ."""

    def test_parse_args(self):
        """Parse arguments."""
        bronze_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        bronze_param_map["onboard_layer"] = "bronze"
        list = ["dummy_test"]
        for key in bronze_param_map:
            list.append(f"--{key}={bronze_param_map[key]}")
        sys.argv = list
        args = __main__.parse_args()
        print(args.__dict__.keys())
        print(bronze_param_map.keys())
        self.assertTrue(args.__dict__.keys() == bronze_param_map.keys())

    def test_main_bronze(self):
        """Test bronze onboarding."""
        bronze_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        bronze_param_map["onboard_layer"] = "bronze"
        list = ["dummy_test"]
        for key in bronze_param_map:
            list.append(f"--{key}={bronze_param_map[key]}")
        sys.argv = list
        __main__.main()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

    def test_main_silver(self):
        """Test silver onboarding."""
        silver_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        silver_param_map["onboard_layer"] = "silver"
        list = ["dummy_test"]
        for key in silver_param_map:
            list.append(f"--{key}={silver_param_map[key]}")
        sys.argv = list
        __main__.main()
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_main_bronze_silver(self):
        """Test bronze and silver onboarding."""
        param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        param_map["onboard_layer"] = "bronze_silver"
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        __main__.main()
        bronze_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/bronze")
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        silver_dataflowSpec_df = self.spark.read.format("delta").load(self.onboarding_spec_paths + "/silver")
        self.assertEqual(silver_dataflowSpec_df.count(), 3)

    def test_main_negative(self):
        """Test bronze onboarding."""
        bronze_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        list = ["dummy_test"]
        for key in bronze_param_map:
            list.append(f"--{key}={bronze_param_map[key]}")
        sys.argv = list
        with self.assertRaises(Exception):
            __main__.main()
