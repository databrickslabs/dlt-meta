"""Tests for the OSS / Lakeflow runtime shim layer.

Exercises:

- Runtime detection (env override + ``pyspark.pipelines`` symbol probe).
- ``filter_table_kwargs`` correctly strips Lakeflow-only kwargs on OSS
  while leaving them intact on Databricks.
- ``wrap_dqe`` injects ``where`` filters for ``or_drop`` on OSS, raises
  via ``raise_error`` for ``or_fail``, and is identity on Databricks.
- ``cdc_not_supported_error`` surfaces the actionable message.
"""
import importlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Mirror tests/test_compat.py: the pyspark.pipelines module is only
# present on Spark 4.1+ runtimes. Register a MagicMock so the shim's
# ``hasattr`` probe still works in CI environments without it.
sys.modules.setdefault("pyspark.pipelines", MagicMock())


def _reload_shim(env_override=None):
    """Reload the shim module so module-level ``RUNTIME`` is recomputed."""
    if env_override is None:
        os.environ.pop("SDP_META_RUNTIME", None)
    else:
        os.environ["SDP_META_RUNTIME"] = env_override
    if "databricks.labs.sdp_meta.oss_pipelines" in sys.modules:
        del sys.modules["databricks.labs.sdp_meta.oss_pipelines"]
    return importlib.import_module("databricks.labs.sdp_meta.oss_pipelines")


class _EnvVarCleanupMixin:
    """Always pop ``SDP_META_RUNTIME`` after each test method.

    The ``_reload_shim`` helper sets the env var but doesn't restore it.
    Without this mixin a leftover ``SDP_META_RUNTIME=oss`` flips
    ``is_oss()`` for every subsequent test file (notably
    ``tests/test_dataflow_pipeline.py``) and breaks any
    ``@patch.object(DataflowPipeline, ...)`` decorator because the
    ``__new__`` factory dispatches to ``OSSDataflowPipeline`` instead.
    """

    def setUp(self):
        super().setUp()
        self._prior_runtime = os.environ.get("SDP_META_RUNTIME")

    def tearDown(self):
        if self._prior_runtime is None:
            os.environ.pop("SDP_META_RUNTIME", None)
        else:
            os.environ["SDP_META_RUNTIME"] = self._prior_runtime
        super().tearDown()


class TestRuntimeDetection(unittest.TestCase):
    """``is_databricks()`` / ``is_oss()`` re-probe on every call.

    The reload-based ``_reload_shim`` helper still exists for tests that
    want to assert the import-time ``RUNTIME`` snapshot, but typical
    callers should be able to flip the env var and call ``is_oss()``
    without forcing a reload — that's what these tests check.
    """

    def setUp(self):
        # Make sure each test starts from a clean env regardless of run order.
        os.environ.pop("SDP_META_RUNTIME", None)
        if "databricks.labs.sdp_meta.oss_pipelines" in sys.modules:
            del sys.modules["databricks.labs.sdp_meta.oss_pipelines"]
        self.m = importlib.import_module("databricks.labs.sdp_meta.oss_pipelines")

    def tearDown(self):
        os.environ.pop("SDP_META_RUNTIME", None)

    def test_env_override_oss_without_reload(self):
        os.environ["SDP_META_RUNTIME"] = "oss"
        self.assertTrue(self.m.is_oss())
        self.assertFalse(self.m.is_databricks())

    def test_env_override_databricks_without_reload(self):
        os.environ["SDP_META_RUNTIME"] = "databricks"
        self.assertTrue(self.m.is_databricks())
        self.assertFalse(self.m.is_oss())

    def test_env_override_flips_live(self):
        # Same module instance: flip the env var and observe both directions.
        os.environ["SDP_META_RUNTIME"] = "oss"
        self.assertTrue(self.m.is_oss())
        os.environ["SDP_META_RUNTIME"] = "databricks"
        self.assertTrue(self.m.is_databricks())
        os.environ["SDP_META_RUNTIME"] = "lakeflow"  # alias
        self.assertTrue(self.m.is_databricks())

    def test_runtime_constant_is_import_time_snapshot(self):
        # ``RUNTIME`` is computed once on import. Changing the env var
        # after import does not change the snapshot — only the live
        # ``is_*()`` predicates pick up the change.
        original = self.m.RUNTIME
        os.environ["SDP_META_RUNTIME"] = "oss" if original == "databricks" else "databricks"
        self.assertEqual(self.m.RUNTIME, original)
        # But the live predicates already moved.
        self.assertEqual(
            self.m.is_oss(),
            os.environ["SDP_META_RUNTIME"].startswith("oss"),
        )

    def test_pyspark_probe_is_memoized(self):
        # With no env override, ``is_*()`` falls through to the
        # ``pyspark.pipelines`` symbol probe. That probe is the expensive
        # part (import + ``hasattr``) and must run only once per process.
        os.environ.pop("SDP_META_RUNTIME", None)
        real = self.m.refresh_runtime()  # clear cache + capture real value
        # Poison the cache with a sentinel the real probe would never
        # return. If the probe re-ran on the next call it would overwrite
        # the sentinel — so observing the sentinel proves it's memoized.
        self.m._probed_pyspark_runtime = "sentinel-not-a-real-runtime"
        self.assertEqual(
            self.m._probe_pyspark_symbols(), "sentinel-not-a-real-runtime"
        )
        # ``refresh_runtime`` discards the cache and re-probes back to real.
        self.assertEqual(self.m.refresh_runtime(), real)

    def test_env_override_short_circuits_before_cached_probe(self):
        # The env override is consulted on every call and wins over the
        # memoized probe — so flipping it takes effect without a refresh
        # even when the pyspark probe is already cached.
        self.m.refresh_runtime()
        self.m._probe_pyspark_symbols()  # ensure the probe is cached
        os.environ["SDP_META_RUNTIME"] = "oss"
        self.assertTrue(self.m.is_oss())
        os.environ["SDP_META_RUNTIME"] = "databricks"
        self.assertTrue(self.m.is_databricks())

    def test_probe_with_lakeflow_symbols_present(self):
        # MagicMock injected into sys.modules already has every attribute,
        # so ``hasattr(_dp, "expect_all")`` is True; detection should
        # land on databricks when no env override is set.
        m = _reload_shim()
        self.assertTrue(m.is_databricks())

    def test_probe_with_oss_only_symbols(self):
        # Replace pyspark.pipelines with an object that only exposes the
        # OSS public API. Detection should land on OSS.
        oss_only = MagicMock(spec=["table", "materialized_view", "temporary_view",
                                   "create_streaming_table", "append_flow",
                                   "create_sink"])
        old = sys.modules.get("pyspark.pipelines")
        sys.modules["pyspark.pipelines"] = oss_only
        try:
            m = _reload_shim()
            self.assertTrue(m.is_oss())
        finally:
            if old is not None:
                sys.modules["pyspark.pipelines"] = old


class TestFilterTableKwargs(_EnvVarCleanupMixin, unittest.TestCase):
    def setUp(self):
        super().setUp()
        # The de-dup set in :func:`filter_table_kwargs` is process-wide;
        # reset it so prior tests (or prior runs of this test) don't
        # suppress the warning we're about to assert on.
        import databricks.labs.sdp_meta.oss_pipelines as m
        m.reset_kwarg_warning_state()

    def test_oss_strips_cluster_by_auto_and_path(self):
        m = _reload_shim("oss")
        out = m.filter_table_kwargs(dict(
            name="t",
            cluster_by_auto=True,
            path="/some/path",
            partition_cols=["a"],
            comment="hi",
        ))
        self.assertNotIn("cluster_by_auto", out)
        self.assertNotIn("path", out)
        self.assertEqual(out["name"], "t")
        self.assertEqual(out["partition_cols"], ["a"])
        self.assertEqual(out["comment"], "hi")

    def test_oss_does_not_warn_on_path_drop(self):
        # ``path`` is honoured by ``ensure_external_delta_table`` before
        # this filter runs; warning here would misleadingly imply data
        # loss. ``cluster_by_auto`` is genuinely dropped → still warns.
        m = _reload_shim("oss")
        m.reset_kwarg_warning_state()
        with patch.object(m, "logger") as mock_logger:
            m.filter_table_kwargs(dict(
                name="t",
                cluster_by_auto=True,
                path="/some/path",
            ))
        warning_args = [c.args for c in mock_logger.warning.call_args_list]
        # Exactly one warning, and it must be about cluster_by_auto, not path.
        self.assertEqual(len(warning_args), 1, warning_args)
        warned_kwarg = warning_args[0][1]
        self.assertEqual(warned_kwarg, "cluster_by_auto")

    def test_oss_dedupes_warnings_across_calls(self):
        """Repeated drops of the same kwarg warn exactly once per process.

        SDP-META can register hundreds of ``dp.table`` calls per run; a
        warning on every repeat floods the operator log without any
        new signal. The de-dup is keyed on kwarg name (not value)
        because the operator action ("remove this kwarg or migrate to
        Lakeflow") is identical regardless of value.
        """
        m = _reload_shim("oss")
        m.reset_kwarg_warning_state()
        with patch.object(m, "logger") as mock_logger:
            for i in range(5):
                m.filter_table_kwargs(dict(
                    name=f"t{i}",
                    cluster_by_auto=True,
                ))
        # Five calls, all dropping ``cluster_by_auto`` — exactly one warning.
        self.assertEqual(len(mock_logger.warning.call_args_list), 1)
        warned_kwarg = mock_logger.warning.call_args_list[0].args[1]
        self.assertEqual(warned_kwarg, "cluster_by_auto")

    def test_oss_dedupes_per_kwarg_not_per_call(self):
        """Each distinct kwarg name gets its own warning slot."""
        m = _reload_shim("oss")
        m.reset_kwarg_warning_state()
        with patch.object(m, "logger") as mock_logger:
            # First call drops cluster_by_auto.
            m.filter_table_kwargs(dict(name="t1", cluster_by_auto=True))
            # Second call drops expect_all (a different kwarg) — must
            # still warn even though one warning already fired.
            m.filter_table_kwargs(
                dict(name="t2", expect_all={"a": "x > 0"}),
                also_drop_dqe=True,
            )
            # Third call repeats cluster_by_auto — suppressed.
            m.filter_table_kwargs(dict(name="t3", cluster_by_auto=True))
        warned_kwargs = sorted(
            c.args[1] for c in mock_logger.warning.call_args_list
        )
        self.assertEqual(warned_kwargs, ["cluster_by_auto", "expect_all"])

    def test_reset_kwarg_warning_state_re_arms(self):
        """``reset_kwarg_warning_state`` clears the de-dup set."""
        m = _reload_shim("oss")
        m.reset_kwarg_warning_state()
        with patch.object(m, "logger") as mock_logger:
            m.filter_table_kwargs(dict(name="t1", cluster_by_auto=True))
            m.filter_table_kwargs(dict(name="t2", cluster_by_auto=True))
            self.assertEqual(len(mock_logger.warning.call_args_list), 1)
            # Reset and fire the same kwarg again — warning re-fires.
            m.reset_kwarg_warning_state()
            m.filter_table_kwargs(dict(name="t3", cluster_by_auto=True))
            self.assertEqual(len(mock_logger.warning.call_args_list), 2)

    def test_oss_strips_dqe_kwargs_when_requested(self):
        m = _reload_shim("oss")
        out = m.filter_table_kwargs(
            dict(
                name="t",
                expect_all={"a": "x > 0"},
                expect_all_or_drop={"b": "y IS NOT NULL"},
                expect_all_or_fail={"c": "z != 'bad'"},
                schema=None,
            ),
            also_drop_dqe=True,
        )
        self.assertNotIn("expect_all", out)
        self.assertNotIn("expect_all_or_drop", out)
        self.assertNotIn("expect_all_or_fail", out)
        self.assertEqual(out["name"], "t")

    def test_oss_keeps_dqe_kwargs_when_not_requested(self):
        m = _reload_shim("oss")
        dqe = {"a": "x > 0"}
        out = m.filter_table_kwargs(
            dict(name="t", expect_all=dqe),
            also_drop_dqe=False,
        )
        self.assertEqual(out.get("expect_all"), dqe)

    def test_databricks_is_identity(self):
        m = _reload_shim("databricks")
        kwargs = dict(
            name="t", cluster_by_auto=True, path="/x", expect_all={"a": "x>0"}
        )
        out = m.filter_table_kwargs(kwargs, also_drop_dqe=True)
        self.assertEqual(out, kwargs)
        self.assertIsNot(out, kwargs)  # defensive copy


class TestWrapDqe(_EnvVarCleanupMixin, unittest.TestCase):
    def test_databricks_is_identity(self):
        m = _reload_shim("databricks")

        def qf():
            return "df"

        wrapped = m.wrap_dqe(
            qf,
            expect_all={"a": "x > 0"},
            expect_all_or_drop={"b": "y IS NOT NULL"},
            expect_all_or_fail={"c": "z != 'bad'"},
        )
        self.assertIs(wrapped, qf)

    def test_oss_or_drop_filters_dataframe(self):
        m = _reload_shim("oss")
        df = MagicMock()
        df.where = MagicMock(return_value="filtered")
        wrapped = m.wrap_dqe(
            lambda: df,
            expect_all_or_drop={"valid": "x > 0", "not_null": "y IS NOT NULL"},
        )
        result = wrapped()
        # Both constraints should be ANDed in the where expression.
        df.where.assert_called_once()
        where_arg = df.where.call_args[0][0]
        self.assertIn("(x > 0)", where_arg)
        self.assertIn("(y IS NOT NULL)", where_arg)
        self.assertIn(" AND ", where_arg)
        self.assertEqual(result, "filtered")

    def test_oss_or_drop_with_no_constraints_passes_through(self):
        m = _reload_shim("oss")

        def qf():
            return "raw"

        wrapped = m.wrap_dqe(qf, expect_all_or_drop=None)
        self.assertIs(wrapped, qf)

    def test_oss_or_fail_uses_where_predicate(self):
        m = _reload_shim("oss")
        df = MagicMock()
        df.where = MagicMock(return_value="asserted")
        wrapped = m.wrap_dqe(
            lambda: df,
            expect_all_or_fail={"valid": "x > 0"},
        )
        # ``expr`` requires an active SparkContext to JVM-evaluate; we just
        # care that the SQL string we built was passed to it once and that
        # the assertion runs inside a ``where`` predicate. A predicate is
        # used (not ``withColumn(...).drop(...)``) because an add-then-drop
        # column is dead code that Catalyst prunes before ``raise_error``
        # ever evaluates — which would silently no-op ``expect_all_or_fail``.
        with patch.object(m, "expr", return_value="<expr-col>") as mock_expr:
            result = wrapped()
        mock_expr.assert_called_once()
        sql = mock_expr.call_args[0][0]
        self.assertIn("raise_error", sql)
        self.assertIn("(x > 0)", sql)
        self.assertIn("valid", sql)  # the description name surfaces in the message
        self.assertIn("THEN true", sql)  # boolean predicate, keeps valid rows
        df.where.assert_called_once_with("<expr-col>")
        self.assertEqual(result, "asserted")

    def test_oss_combined_drop_and_fail(self):
        m = _reload_shim("oss")
        df = MagicMock()
        # First drop wraps qf, then fail wraps that. Order in the helper is:
        #   qf -> _wrap_with_drop -> _wrap_with_fail
        # so calling the final wrapped function calls .where first (drop)
        # and then .where again on the filtered result (fail). Both DQE
        # flavours run inside ``where`` predicates.
        filtered = MagicMock()
        df.where = MagicMock(return_value=filtered)
        filtered.where = MagicMock(return_value="ok")
        wrapped = m.wrap_dqe(
            lambda: df,
            expect_all_or_drop={"a": "x > 0"},
            expect_all_or_fail={"b": "y IS NOT NULL"},
        )
        with patch.object(m, "expr", return_value="<expr-col>"):
            result = wrapped()
        df.where.assert_called_once()
        filtered.where.assert_called_once()
        self.assertEqual(result, "ok")

    def test_oss_expect_all_is_log_only_and_passes_through(self):
        # ``expect_all`` is metrics-only on Lakeflow; OSS has no
        # event-log surface to enforce or surface it, so wrap_dqe must
        # NOT wrap the query function (it returns it unchanged) but must
        # emit a single info log naming the expectation keys so the
        # no-op is visible to operators rather than silent.
        m = _reload_shim("oss")

        def qf():
            return "raw"

        with patch.object(m, "logger") as mock_logger:
            wrapped = m.wrap_dqe(qf, expect_all={"b": "y IS NOT NULL", "a": "x > 0"})
        # No drop/fail wrap → identical function object passes through.
        self.assertIs(wrapped, qf)
        # Exactly one info log, carrying the expectation keys (sorted).
        mock_logger.info.assert_called_once()
        logged_keys = mock_logger.info.call_args[0][1]
        self.assertEqual(logged_keys, ["a", "b"])


class TestCdcNotSupported(_EnvVarCleanupMixin, unittest.TestCase):
    def test_error_class_and_message(self):
        m = _reload_shim("oss")
        err = m.cdc_not_supported_error()
        self.assertIsInstance(err, NotImplementedError)
        self.assertIn("create_auto_cdc_flow", str(err))
        self.assertIn("Lakeflow", str(err))
        self.assertIn("SDP_META_RUNTIME=databricks", str(err))


class TestEnsureExternalDeltaTable(_EnvVarCleanupMixin, unittest.TestCase):
    """``ensure_external_delta_table`` is the side-channel that lets the OSS
    code path honour the per-table ``path`` from the onboarding spec.

    OSS ``pyspark.pipelines.table`` rejects a ``path`` kwarg, so SDP-META
    pre-creates an external Delta table at the requested location and
    lets ``dp.table(name=...)`` resolve to it.
    """

    @staticmethod
    def _fake_spark_with_no_existing_table():
        """Build a Spark mock whose ``DESCRIBE TABLE EXTENDED`` raises (table absent)."""
        spark = MagicMock()
        sql_calls: list[str] = []

        def fake_sql(stmt: str):
            sql_calls.append(stmt)
            result = MagicMock()
            if stmt.strip().upper().startswith("DESCRIBE TABLE EXTENDED"):
                result.collect.side_effect = Exception("Table not found")
            else:
                result.collect.return_value = []
            return result

        spark.sql = MagicMock(side_effect=fake_sql)
        spark._sql_calls = sql_calls
        return spark

    @staticmethod
    def _fake_spark_with_existing_location(location: str):
        spark = MagicMock()
        sql_calls: list[str] = []

        def fake_sql(stmt: str):
            sql_calls.append(stmt)
            result = MagicMock()
            if stmt.strip().upper().startswith("DESCRIBE TABLE EXTENDED"):
                row = MagicMock()
                row.__getitem__ = lambda self, k: {
                    "col_name": "Location",
                    "data_type": location,
                }[k]
                result.collect.return_value = [row]
            else:
                result.collect.return_value = []
            return result

        spark.sql = MagicMock(side_effect=fake_sql)
        spark._sql_calls = sql_calls
        return spark

    def test_no_op_on_databricks(self):
        m = _reload_shim("databricks")
        spark = MagicMock()
        result = m.ensure_external_delta_table(spark, "bronze.customers", "/tmp/x")
        self.assertFalse(result)
        spark.sql.assert_not_called()

    def test_no_op_when_path_is_none(self):
        m = _reload_shim("oss")
        spark = MagicMock()
        result = m.ensure_external_delta_table(spark, "bronze.customers", None)
        self.assertFalse(result)
        spark.sql.assert_not_called()

    def test_no_op_when_path_is_empty(self):
        m = _reload_shim("oss")
        spark = MagicMock()
        result = m.ensure_external_delta_table(spark, "bronze.customers", "")
        self.assertFalse(result)
        spark.sql.assert_not_called()

    def test_creates_schema_and_external_table_when_absent(self):
        m = _reload_shim("oss")
        spark = self._fake_spark_with_no_existing_table()
        result = m.ensure_external_delta_table(
            spark, "bronze.customers", "/tmp/bronze/customers"
        )
        self.assertTrue(result)

        sql_stmts = spark._sql_calls
        # 1) CREATE SCHEMA
        self.assertTrue(
            any(s.startswith("CREATE SCHEMA IF NOT EXISTS `bronze`")
                for s in sql_stmts),
            f"missing CREATE SCHEMA in {sql_stmts}",
        )
        # 2) DESCRIBE check (returned not-found)
        self.assertTrue(
            any(s.startswith("DESCRIBE TABLE EXTENDED `bronze`.`customers`")
                for s in sql_stmts),
            f"missing DESCRIBE in {sql_stmts}",
        )
        # 3) CREATE TABLE with the requested LOCATION
        create_stmt = next(
            (s for s in sql_stmts if s.startswith("CREATE TABLE")), None
        )
        self.assertIsNotNone(create_stmt)
        self.assertIn("`bronze`.`customers`", create_stmt)
        self.assertIn("USING DELTA", create_stmt)
        self.assertIn("LOCATION '/tmp/bronze/customers'", create_stmt)

    def test_three_part_name_includes_catalog(self):
        m = _reload_shim("oss")
        spark = self._fake_spark_with_no_existing_table()
        result = m.ensure_external_delta_table(
            spark, "main.bronze.customers", "/tmp/bronze/customers"
        )
        self.assertTrue(result)

        sql_stmts = spark._sql_calls
        self.assertTrue(
            any(s.startswith("CREATE SCHEMA IF NOT EXISTS `main`.`bronze`")
                for s in sql_stmts),
            f"missing CREATE SCHEMA with catalog in {sql_stmts}",
        )
        create_stmt = next(
            (s for s in sql_stmts if s.startswith("CREATE TABLE")), None
        )
        self.assertIsNotNone(create_stmt)
        self.assertIn("`main`.`bronze`.`customers`", create_stmt)

    def test_existing_table_at_same_location_is_idempotent(self):
        m = _reload_shim("oss")
        spark = self._fake_spark_with_existing_location("/tmp/bronze/customers/")
        result = m.ensure_external_delta_table(
            spark, "bronze.customers", "/tmp/bronze/customers"
        )
        # Already registered at the same location → no CREATE TABLE.
        self.assertFalse(result)
        sql_stmts = spark._sql_calls
        self.assertFalse(
            any(s.startswith("CREATE TABLE") for s in sql_stmts),
            f"unexpected CREATE TABLE in {sql_stmts}",
        )

    def test_existing_table_at_different_location_warns(self):
        m = _reload_shim("oss")
        # Belt-and-braces: the strict env var is also the trigger for
        # the raise branch tested below; make sure it's not leaking from
        # a prior test before exercising the warn branch here.
        os.environ.pop("SDP_META_OSS_REGISTER_STRICT", None)
        spark = self._fake_spark_with_existing_location("/old/path")
        with patch.object(m, "logger") as mock_logger:
            result = m.ensure_external_delta_table(
                spark, "bronze.customers", "/new/path"
            )
        self.assertFalse(result)
        mock_logger.warning.assert_called_once()
        # ``logger.warning`` is called with a format string + positional
        # args (lazy interpolation), so the human-readable mismatch
        # message and the env-var hint are split across the format
        # string and the args. Render the call so the assertions match
        # what an operator would actually see in the log.
        warn_call = mock_logger.warning.call_args
        rendered = warn_call.args[0] % warn_call.args[1:]
        self.assertIn("already registered", rendered)
        self.assertIn("/old/path", rendered)
        self.assertIn("/new/path", rendered)
        # The warning surfaces the strict-mode env var so the user
        # discovers the fail-fast option.
        self.assertIn("SDP_META_OSS_REGISTER_STRICT", rendered)

    def test_existing_table_at_different_location_raises_when_strict(self):
        """``SDP_META_OSS_REGISTER_STRICT=1`` upgrades the warn to a raise.

        Useful in CI so onboarding-spec drift fails the pipeline instead
        of silently writing to the prior location.
        """
        m = _reload_shim("oss")
        os.environ["SDP_META_OSS_REGISTER_STRICT"] = "1"
        try:
            spark = self._fake_spark_with_existing_location("/old/path")
            with self.assertRaises(RuntimeError) as ctx:
                m.ensure_external_delta_table(
                    spark, "bronze.customers", "/new/path"
                )
            msg = str(ctx.exception)
            self.assertIn("already registered", msg)
            self.assertIn("/old/path", msg)
            self.assertIn("/new/path", msg)
            self.assertIn("SDP_META_OSS_REGISTER_STRICT", msg)
        finally:
            os.environ.pop("SDP_META_OSS_REGISTER_STRICT", None)

    def test_strict_mode_does_not_affect_matching_locations(self):
        """Strict mode is a no-op when the registered location matches."""
        m = _reload_shim("oss")
        os.environ["SDP_META_OSS_REGISTER_STRICT"] = "1"
        try:
            spark = self._fake_spark_with_existing_location("/tmp/bronze/customers/")
            # Same location — must NOT raise even in strict mode.
            result = m.ensure_external_delta_table(
                spark, "bronze.customers", "/tmp/bronze/customers"
            )
            self.assertFalse(result)
        finally:
            os.environ.pop("SDP_META_OSS_REGISTER_STRICT", None)

    def test_strict_mode_does_not_affect_first_time_create(self):
        """Strict mode is a no-op on first-time table creation (no prior LOCATION)."""
        m = _reload_shim("oss")
        os.environ["SDP_META_OSS_REGISTER_STRICT"] = "1"
        try:
            spark = self._fake_spark_with_no_existing_table()
            result = m.ensure_external_delta_table(
                spark, "bronze.customers", "/tmp/bronze/customers"
            )
            # First-time create still happens — strict mode only kicks
            # in on mismatched-LOCATION existing tables.
            self.assertTrue(result)
        finally:
            os.environ.pop("SDP_META_OSS_REGISTER_STRICT", None)

    def test_path_with_single_quote_is_escaped(self):
        m = _reload_shim("oss")
        spark = self._fake_spark_with_no_existing_table()
        m.ensure_external_delta_table(
            spark, "bronze.customers", "/tmp/has'quote"
        )
        create_stmt = next(
            s for s in spark._sql_calls if s.startswith("CREATE TABLE")
        )
        self.assertIn("LOCATION '/tmp/has''quote'", create_stmt)

    def test_schema_columns_are_inlined_into_create(self):
        """A supplied ``StructType`` is emitted as columns in the DDL.

        Without this, ``CREATE TABLE ... USING DELTA LOCATION`` at an empty
        location raises ``DELTA_FAILED_INFER_SCHEMA`` because Delta has no
        log to infer from on the first run.
        """
        from pyspark.sql.types import StructType, StructField, StringType, LongType

        m = _reload_shim("oss")
        spark = self._fake_spark_with_no_existing_table()
        schema = StructType([
            StructField("id", LongType()),
            StructField("name", StringType()),
        ])
        result = m.ensure_external_delta_table(
            spark, "bronze.customers", "/tmp/bronze/customers", schema=schema
        )
        self.assertTrue(result)
        create_stmt = next(
            s for s in spark._sql_calls if s.startswith("CREATE TABLE")
        )
        # Columns derived from the StructType land in the DDL.
        self.assertIn("`id`", create_stmt)
        self.assertIn("`name`", create_stmt)
        self.assertIn("USING DELTA", create_stmt)
        self.assertIn("LOCATION '/tmp/bronze/customers'", create_stmt)

    def test_describe_failure_logs_at_debug_not_silent(self):
        """A DESCRIBE failure must surface at DEBUG so operators have a
        breadcrumb when the subsequent CREATE TABLE fails with a
        misleading error.

        The function returns ``None`` (treat as "table absent" and proceed
        to CREATE), but the original Spark exception must be logged at
        DEBUG with the exception type AND message, not silently swallowed.
        """
        m = _reload_shim("oss")
        spark = MagicMock()
        sentinel_error = RuntimeError("catalog connectivity lost")

        def fake_sql(stmt: str):
            result = MagicMock()
            if stmt.strip().upper().startswith("DESCRIBE TABLE EXTENDED"):
                result.collect.side_effect = sentinel_error
            else:
                result.collect.return_value = []
            return result

        spark.sql = MagicMock(side_effect=fake_sql)
        with patch.object(m, "logger") as mock_logger:
            # Use the public helper that drives DESCRIBE — easier than
            # calling the private ``_existing_table_location`` directly.
            result = m.ensure_external_delta_table(
                spark, "bronze.customers", "/tmp/bronze/customers"
            )
        # Helper still creates the table (treats as absent), but the
        # original DESCRIBE error was logged at DEBUG with both the
        # exception type and the message.
        self.assertTrue(result)
        mock_logger.debug.assert_called_once()
        debug_args = mock_logger.debug.call_args.args
        rendered = debug_args[0] % debug_args[1:]
        self.assertIn("DESCRIBE TABLE EXTENDED", rendered)
        self.assertIn("RuntimeError", rendered)
        self.assertIn("catalog connectivity lost", rendered)

    def test_schemaless_create_failure_degrades_to_warning(self):
        """A failing schema-less create warns and returns False, never raises.

        On a fresh location with no schema, Delta raises
        ``DELTA_FAILED_INFER_SCHEMA``. The helper must not abort the whole
        pipeline run over a path it cannot pre-bind.
        """
        m = _reload_shim("oss")
        spark = MagicMock()
        sql_calls: list[str] = []

        def fake_sql(stmt: str):
            sql_calls.append(stmt)
            result = MagicMock()
            if stmt.strip().upper().startswith("DESCRIBE TABLE EXTENDED"):
                result.collect.side_effect = Exception("Table not found")
            elif stmt.strip().upper().startswith("CREATE TABLE"):
                raise Exception("DELTA_FAILED_INFER_SCHEMA: unable to infer schema")
            else:
                result.collect.return_value = []
            return result

        spark.sql = MagicMock(side_effect=fake_sql)
        with patch.object(m, "logger") as mock_logger:
            result = m.ensure_external_delta_table(
                spark, "bronze.customers", "/tmp/bronze/customers"
            )
        # No crash; degraded to a warning, path left unbound for this run.
        self.assertFalse(result)
        mock_logger.warning.assert_called_once()
        rendered = (
            mock_logger.warning.call_args.args[0]
            % mock_logger.warning.call_args.args[1:]
        )
        self.assertIn("could not pre-register", rendered)


if __name__ == "__main__":
    unittest.main()
