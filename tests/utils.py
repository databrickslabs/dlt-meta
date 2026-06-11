"""Shared base test class for the SDP-META unit-test suite.

Spark startup is expensive (3-5s of JVM boot per ``getOrCreate()``). The
historical version of this file built a fresh ``SparkSession`` in
``setUp`` (once **per test**) and stopped the underlying
``SparkContext`` in ``tearDown``. With ~400 tests subclassing
``SDPFrameworkTestCase`` that paid the cold-start cost once per
test method, dominating the wall-clock cost of the whole suite.

This refactor splits the work into class-scoped vs instance-scoped
hooks:

  * ``setUpClass`` (once per test class):
      - Build the SparkSession + DeltaPipelinesMetaStoreOps /
        DeltaPipelinesInternalTableOps. These are stateless wrt the
        per-test DB / tempdir / fixture-path state below, so sharing
        them across tests in the same class is safe.
      - Bind every onboarding-fixture path string (``onboarding_*``)
        to the class. They never change between tests; binding them
        once is purely a perf win, no semantics change.

  * ``setUp`` (once per test):
      - Create fresh tempdirs (``onboarding_spec_paths``,
        ``temp_delta_tables_path``) so per-test write paths are
        isolated.
      - Drop+recreate the ``ravi_dlt_demo`` test database so each
        test starts from a clean catalog state.
      - Rebuild the ``onboarding_*_params_map`` dicts (they reference
        the per-test ``onboarding_spec_paths``).

  * ``tearDown`` (once per test):
      - Drop the per-test database and remove the per-test tempdirs.
      - **Do NOT call ``sc.stop()``.** Stopping the context in
        ``tearDown`` was the historical reason ``setUp`` had to
        rebuild Spark on every test. Leaving the JVM running lets
        ``getOrCreate()`` return the existing session for the rest
        of the pytest invocation -- effectively one Spark per
        ``pytest`` invocation across all classes that share this
        base.

Test isolation is preserved: every test still sees a fresh database
and a fresh pair of tempdirs. The only thing shared across tests is
the SparkSession itself, which is read-only state for the test code.
"""


import shutil
import tempfile
import unittest
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from databricks.labs.sdp_meta.metastore_ops import DeltaPipelinesMetaStoreOps, DeltaPipelinesInternalTableOps


class SDPFrameworkTestCase(unittest.TestCase):
    """Test class base that sets up a correctly configured SparkSession for querying Delta tables."""

    @classmethod
    def setUpClass(cls):
        """Class-scoped setup -- runs ONCE per test class.

        Builds the SparkSession + Delta-aware ops and binds the
        immutable fixture-path strings to the class. Per-test state
        (database, tempdirs, param dicts) is built in ``setUp`` so
        each test method is still isolated from its peers.
        """
        builder = (
            SparkSession.builder.appName("SDP-META_UNIT_TESTS")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.conf.set("spark.sql.shuffle.partitions", "4")
        cls.spark.conf.set("spark.app.name", "sdp-meta-unit-tests")
        cls.spark.conf.set("spark.master", "local[4]")
        cls.spark.conf.set("spark.databricks.delta.snapshotPartitions", "2")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "5")
        cls.spark.conf.set("delta.log.cacheSize", "3")
        cls.spark.conf.set("spark.databricks.delta.delta.log.cacheSize", "3")
        cls.spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
        cls.deltaPipelinesMetaStoreOps = DeltaPipelinesMetaStoreOps(cls.spark)
        cls.deltaPipelinesInternalTableOps = DeltaPipelinesInternalTableOps(cls.spark)
        cls.sc = cls.spark.sparkContext

        # Onboarding fixture paths are stable across tests -- bind once.
        cls.onboarding_json_file = "tests/resources/onboarding.json"
        cls.onboarding_json_v7_file = "tests/resources/onboarding_v0.0.7.json"
        cls.onboarding_json_v8_file = "tests/resources/onboarding_v0.0.8.json"
        cls.onboarding_json_v9_file = "tests/resources/onboarding_v0.0.9.json"
        cls.onboarding_json_v10_file = "tests/resources/onboarding_v0.0.10.json"
        cls.onboarding_unsupported_file = "tests/resources/schema.ddl"
        cls.onboarding_v2_json_file = "tests/resources/onboarding_v2.json"
        cls.onboarding_without_ids_json_file = "tests/resources/onboarding_without_ids.json"
        cls.onboarding_invalid_read_options_file = "tests/resources/onboarding_invalid_read_options.json"
        cls.onboarding_json_dups = "tests/resources/onboarding_with_dups.json"
        cls.onboarding_missing_keys_file = "tests/resources/onboarding_missing_keys.json"
        cls.onboarding_type2_json_file = "tests/resources/onboarding_ac_type2.json"
        cls.onboarding_bronze_type2_json_file = "tests/resources/onboarding_ac_bronze_type2.json"
        cls.onboarding_append_flow_json_file = "tests/resources/onboarding_append_flow.json"
        cls.onboarding_silver_fanout_json_file = "tests/resources/onboarding_silverfanout.json"
        cls.onboarding_sink_json_file = "tests/resources/onboarding_sink.json"
        cls.onboarding_multiple_partitions_file = "tests/resources/onboarding_multiple_partitions.json"
        cls.onboarding_apply_changes_from_snapshot_json_file = (
            "tests/resources/onboarding_applychanges_from_snapshot.json"
        )
        cls.onboarding_silver_apply_changes_from_snapshot_json_file = (
            "tests/resources/onboarding_silver_acfs.json"
        )
        cls.onboarding_apply_changes_from_snapshot_json__error_file = (
            "tests/resources/onboarding_applychanges_from_snapshot_error.json"
        )
        # Multi-source AUTO CDC (issue #294).
        cls.onboarding_bronze_cdc_flows_json_file = (
            "tests/resources/onboarding_bronze_cdc_flows.json"
        )
        cls.onboarding_silver_cdc_flows_json_file = (
            "tests/resources/onboarding_silver_cdc_flows.json"
        )
        # Mixed file: N bronze-only rows + 1 silver-only row that uses
        # multi-source AUTO CDC (no silver_transformation_json, no
        # bronze fields on the silver row). This is the natural shape
        # users write for the multi-source CDC silver demo (issue #294).
        cls.onboarding_mixed_bronze_silver_cdc_flows_json_file = (
            "tests/resources/onboarding_mixed_bronze_silver_cdc_flows.json"
        )

    @classmethod
    def tearDownClass(cls):
        """Class-scoped teardown.

        Intentionally does **not** call ``cls.sc.stop()``. Stopping
        the SparkContext here is what historically forced ``setUp``
        to spin a fresh JVM on the next test, multiplying Spark
        startup cost by the test count. Letting ``getOrCreate()``
        keep returning the same session across all classes in the
        same pytest invocation is the entire perf win of this
        refactor.
        """

    def setUp(self):
        """Per-test setup -- runs before each test method.

        Builds fresh tempdirs and a clean ``ravi_dlt_demo`` database
        so each test method is isolated from its peers, and
        rebuilds the ``onboarding_*_params_map`` dicts (which embed
        the per-test ``onboarding_spec_paths`` tempdir).
        """
        self.onboarding_spec_paths = tempfile.mkdtemp()
        self.temp_delta_tables_path = tempfile.mkdtemp()
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

    def tearDown(self):
        """Per-test teardown -- runs after each test method.

        Cleans the per-test database + tempdirs. Crucially does NOT
        stop the SparkContext (see ``tearDownClass`` docstring for
        why).

        Test-pollution defense: unconditionally clear any spark.conf
        keys that tests set during execution and that the rest of
        the suite cares about. Historically this base class stopped
        the SparkContext in ``tearDown``, so any conf state died
        with it; now that Spark persists across tests, we have to
        reset shared state explicitly. Tests that set their own
        confs and want fine-grained cleanup still use
        ``self.addCleanup(self.spark.conf.unset, key)`` directly --
        this block is just a belt-and-braces fallback for the older
        tests that pre-date that pattern.
        """
        # Defensive: unset confs the tests are known to mutate. Use
        # try/except so a conf that wasn't set doesn't blow up
        # tearDown for unrelated tests.
        #
        # The dataflow_spec / dataflow_pipeline modules read these
        # keys at runtime (``layer``, ``<layer>.group``,
        # ``<layer>.dataflowIds``, ``<layer>.dataflowspecTable``) and
        # filter the spec dataframe accordingly. Tests that set them
        # without unsetting in their own tearDown leak the filter
        # into sibling tests and cause sporadic ``IndexError`` /
        # missing-row failures depending on collection order.
        leaky_confs = (
            "spark.databricks.unityCatalog.enabled",
            "layer",
            "bronze.group",
            "bronze.dataflowIds",
            "bronze.dataflowspecTable",
            "silver.group",
            "silver.dataflowIds",
            "silver.dataflowspecTable",
        )
        for key in leaky_confs:
            try:
                self.spark.conf.unset(key)
            except Exception:  # noqa: BLE001
                # Conf wasn't set or unset isn't supported on this
                # Spark version's conf object -- fine, nothing to do.
                pass
        self.deltaPipelinesMetaStoreOps.drop_database("ravi_dlt_demo")
        shutil.rmtree(self.onboarding_spec_paths)
        shutil.rmtree(self.temp_delta_tables_path)
