"""Test Main class."""
from tests.utils import DLTFrameworkTestCase
import sys
import copy
from src import __main__
from unittest.mock import MagicMock

spark = MagicMock()
OnboardDataflowspec = MagicMock()


class MainTests(DLTFrameworkTestCase):
    """Main Unit Test ."""

    def test_parse_args(self):
        """Parse arguments."""
        bronze_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        bronze_param_map["uc_enabled"] = "False"
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
        bronze_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{bronze_param_map['database']}.{bronze_param_map['bronze_dataflowspec_table']}")
        )
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
        silver_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{silver_param_map['database']}.{silver_param_map['silver_dataflowspec_table']}")
        )
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
        bronze_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{param_map['database']}.{param_map['bronze_dataflowspec_table']}")
        )
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        silver_dataflowSpec_df = (self.spark.read.format("delta") .table(
            f"{param_map['database']}.{param_map['silver_dataflowspec_table']}")
        )
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

    def test_main_bronze_uc(self):
        """Test bronze onboarding."""
        bronze_param_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        bronze_param_map["onboard_layer"] = "bronze"
        list = ["dummy_test"]
        for key in bronze_param_map:
            list.append(f"--{key}={bronze_param_map[key]}")
        sys.argv = list
        __main__.main()
        bronze_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{bronze_param_map['database']}.{bronze_param_map['bronze_dataflowspec_table']}")
        )
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)

    def test_main_layer_missing(self):
        """Test bronze and silver onboarding."""
        param_map = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        with self.assertRaises(Exception):
            __main__.main()

    def test_main_bronze_silver_uc(self):
        """Test bronze and silver onboarding for uc."""
        OnboardDataflowspec.return_value = None
        spark_mock = MagicMock("SparkSession")
        spark.builder.appName("DLT-META_Onboarding_Task").getOrCreate().return_value = spark_mock
        param_map = copy.deepcopy(self.onboarding_bronze_silver_params_uc_map)
        param_map["onboard_layer"] = "bronze_silver"
        list = ["dummy_test"]
        for key in param_map:
            list.append(f"--{key}={param_map[key]}")
        sys.argv = list
        __main__.main()
        bronze_dataflowSpec_df = (self.spark.read.format("delta").table(
            f"{param_map['database']}.{param_map['bronze_dataflowspec_table']}")
        )
        self.assertEqual(bronze_dataflowSpec_df.count(), 3)
        silver_dataflowSpec_df = (self.spark.read.format("delta") .table(
            f"{param_map['database']}.{param_map['silver_dataflowspec_table']}")
        )
        self.assertEqual(silver_dataflowSpec_df.count(), 3)
        del param_map['onboard_layer']
        del param_map['uc_enabled']
        del param_map['bronze_dataflowspec_path']
        del param_map['silver_dataflowspec_path']
        OnboardDataflowspec.called_once_with(spark_mock, param_map, uc_enabled=True)

    def test_onboarding(self):
        mock_onboard_dataflowspec = OnboardDataflowspec
        mock_args = MagicMock()
        mock_args.onboard_layer = 'bronze_silver'
        mock_args.uc_enabled = 'true'
        mock_args.__dict__ = {
            'onboard_layer': 'bronze_silver',
            'uc_enabled': 'true',
            'bronze_dataflowspec_path': 'path/to/bronze_dataflowspec',
            'silver_dataflowspec_path': 'path/to/silver_dataflowspec'
        }

        spark_mock = MagicMock("SparkSession")
        spark.builder.appName("DLT-META_Onboarding_Task").getOrCreate().return_value = spark_mock

        mock_onboard_obj = MagicMock()
        mock_onboard_dataflowspec.return_value = mock_onboard_obj

        # Act
        onboard_layer = mock_args.onboard_layer
        uc_enabled = True if mock_args.uc_enabled and mock_args.uc_enabled.lower() == "true" else False
        onboarding_args_dict = mock_args.__dict__
        del onboarding_args_dict['onboard_layer']
        del onboarding_args_dict['uc_enabled']
        if uc_enabled:
            if 'bronze_dataflowspec_path' in onboarding_args_dict:
                del onboarding_args_dict['bronze_dataflowspec_path']
            if 'silver_dataflowspec_path' in onboarding_args_dict:
                del onboarding_args_dict['silver_dataflowspec_path']
        onboard_obj = mock_onboard_dataflowspec(spark, onboarding_args_dict, uc_enabled=uc_enabled)

        if onboard_layer.lower() == "bronze_silver":
            onboard_obj.onboard_dataflow_specs()
        elif onboard_layer.lower() == "bronze":
            onboard_obj.onboard_bronze_dataflow_spec()
        elif onboard_layer.lower() == "silver":
            onboard_obj.onboard_silver_dataflow_spec()
        else:
            raise Exception("onboard_layer argument missing in commandline")

        # Assert
        mock_onboard_dataflowspec.assert_called_once_with(spark, {}, uc_enabled=True)
        mock_onboard_obj.onboard_dataflow_specs.assert_called_once()
