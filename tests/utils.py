"""Base test class."""


import shutil
import tempfile
import unittest
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from src.metastore_ops import DeltaPipelinesMetaStoreOps, DeltaPipelinesInternalTableOps


class DLTFrameworkTestCase(unittest.TestCase):
    """Test class base that sets up a correctly configured SparkSession for querying Delta tables."""

    @classmethod
    def setUp(self):
        """Set inputs."""
        # Configurations to speed up tests and reduce memory footprint
        builder = (
            SparkSession.builder.appName("DLT-META_UNIT_TESTS")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", "4")
        self.spark.conf.set("spark.app.name", "dlt-meta-unit-tests")
        self.spark.conf.set("spark.master", "local[4]")
        self.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
        self.spark.conf.set("spark.sql.shuffle.partitions", "5")
        self.spark.conf.set("delta.log.cacheSize", "3")
        self.spark.conf.set("spark.databricks.delta.delta.log.cacheSize", "3")
        self.spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        self.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(self.spark)
        self.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(self.spark)
        self.sc = self.spark.sparkContext
        self.onboarding_spec_paths = tempfile.mkdtemp()
        self.temp_delta_tables_path = tempfile.mkdtemp()
        self.onboarding_json_file = "tests/resources/onboarding.json"
        self.onboarding_json_v7_file = "tests/resources/onboarding_v0.0.7.json"
        self.onboarding_json_v8_file = "tests/resources/onboarding_v0.0.8.json"
        self.onboarding_json_v9_file = "tests/resources/onboarding_v0.0.9.json"
        self.onboarding_unsupported_file = "tests/resources/schema.ddl"
        self.onboarding_v2_json_file = "tests/resources/onboarding_v2.json"
        self.onboarding_without_ids_json_file = "tests/resources/onboarding_without_ids.json"
        self.onboarding_invalid_read_options_file = "tests/resources/onboarding_invalid_read_options.json"
        self.onboarding_json_dups = "tests/resources/onboarding_with_dups.json"
        self.onboarding_missing_keys_file = "tests/resources/onboarding_missing_keys.json"
        self.onboarding_type2_json_file = "tests/resources/onboarding_ac_type2.json"
        self.onboarding_bronze_type2_json_file = "tests/resources/onboarding_ac_bronze_type2.json"
        self.onboarding_append_flow_json_file = "tests/resources/onboarding_append_flow.json"
        self.onboarding_silver_fanout_json_file = "tests/resources/onboarding_silverfanout.json"
        self.onboarding_sink_json_file = "tests/resources/onboarding_sink.json"
        self.onboarding_multiple_partitions_file = "tests/resources/onboarding_multiple_partitions.json"
        self.onboarding_apply_changes_from_snapshot_json_file = (
            "tests/resources/onboarding_applychanges_from_snapshot.json"
        )
        self.onboarding_apply_changes_from_snapshot_json__error_file = (
            "tests/resources/onboarding_applychanges_from_snapshot_error.json"
        )
        self.deltaPipelinesMetaStoreOps.drop_database("ravi_dlt_demo")
        self.deltaPipelinesMetaStoreOps.create_database("ravi_dlt_demo", "Unittest")
        self.onboarding_bronze_silver_params_map = {
            "onboarding_file_path": self.onboarding_json_file,
            "database": "ravi_dlt_demo",
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
            "bronze_dataflowspec_path": self.onboarding_spec_paths + "/bronze",
            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
            "silver_dataflowspec_path": self.onboarding_spec_paths + "/silver",
            "overwrite": "True",
            "version": "v1",
            "import_author": "Ravi"
        }
        self.onboarding_bronze_silver_params_uc_map = {
            "onboarding_file_path": self.onboarding_json_file,
            "database": "ravi_dlt_demo",
            "env": "dev",
            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
            "bronze_dataflowspec_path": self.onboarding_spec_paths + "/bronze",
            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
            "silver_dataflowspec_path": self.onboarding_spec_paths + "/silver",
            "overwrite": "True",
            "version": "v1",
            "import_author": "Ravi",
            "uc_enabled": "True"
        }

    @classmethod
    def tearDown(self):
        """Tear down."""
        self.deltaPipelinesMetaStoreOps.drop_database("ravi_dlt_demo")
        self.sc.stop()
        shutil.rmtree(self.onboarding_spec_paths)
        shutil.rmtree(self.temp_delta_tables_path)
