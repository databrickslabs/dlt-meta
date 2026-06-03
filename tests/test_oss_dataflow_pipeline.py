"""Tests for the dedicated :class:`OSSDataflowPipeline` subclass.

Exercises:

- ``DataflowPipeline(...)`` factory dispatch returns an
  ``OSSDataflowPipeline`` when the runtime probe lands on OSS, and a
  plain ``DataflowPipeline`` on Lakeflow.
- ``OSSDataflowPipeline.cdc_apply_changes`` and
  ``OSSDataflowPipeline.apply_changes_from_snapshot`` raise
  ``NotImplementedError`` with the canonical actionable message.
- ``OSSDataflowPipeline._register_table_with_dqe`` inlines DQE via
  ``oss_pipelines.wrap_dqe``, strips Lakeflow-only kwargs via
  ``oss_pipelines.filter_table_kwargs``, AND honours the per-table
  ``path`` kwarg by pre-creating an external Delta table at that
  location via ``oss_pipelines.ensure_external_delta_table``.
- ``OSSDataflowPipeline.create_streaming_table`` filters Lakeflow-only
  kwargs (``cluster_by_auto``, ``path``, ``expect_*``) before calling
  ``dp.create_streaming_table`` and pre-creates an external Delta
  table at ``target_path`` (when set).

Like ``tests/test_oss_pipelines.py``, this module mocks
``pyspark.pipelines`` so the imports succeed in environments where
Spark 4.1 isn't installed. The runtime-detection predicates
(``is_oss()`` / ``is_databricks()``) re-probe the environment on every
call, so flipping ``SDP_META_RUNTIME`` between tests is sufficient —
we deliberately do NOT pop sub-modules from ``sys.modules`` because
that would invalidate cached module references that other test files
(notably ``tests/test_dataflow_pipeline.py``) rely on through
``@patch('databricks.labs.sdp_meta.dataflow_pipeline.dp')`` decorators
captured at import time.
"""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# ``pyspark.pipelines`` is only present on Spark 4.1+ runtimes. Register a
# MagicMock so the production imports succeed in CI without it.
sys.modules.setdefault("pyspark.pipelines", MagicMock())

# Eagerly import the modules under test once at module load. We share
# these references across every test so we don't have to clear
# ``sys.modules`` (which would break ``test_dataflow_pipeline.py``'s
# patch.dp decorators). ``oss_dpl_mod`` is the patch target for the
# ``dp`` / ``oss_dp`` / ``DataflowSpecUtils`` aliases that the OSS
# subclass binds at import time.
from databricks.labs.sdp_meta import oss_dataflow_pipeline as oss_dpl_mod  # noqa: E402
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: E402
from databricks.labs.sdp_meta.oss_dataflow_pipeline import OSSDataflowPipeline  # noqa: E402


class _FakeDataflowSpec:
    """Minimal stand-in for a :class:`BronzeDataflowSpec` / :class:`SilverDataflowSpec`.

    ``OSSDataflowPipeline`` inherits its ``__init__`` from
    :class:`DataflowPipeline`, which type-checks the spec. We construct a
    real ``BronzeDataflowSpec`` for the dispatch / instantiation tests
    only; the override unit tests bypass ``__init__`` and stitch the
    minimal fields the methods actually read.
    """

    def __init__(self):
        self.targetDetails = {
            "database": "test_db",
            "table": "test_table",
        }
        self.tableProperties = {}
        self.partitionColumns = [""]
        self.clusterBy = [""]
        self.clusterByAuto = False
        self.dataQualityExpectations = None
        self.cdcApplyChanges = None
        self.applyChangesFromSnapshot = None


class _OSSRuntimeMixin:
    """Set ``SDP_META_RUNTIME`` for the duration of a single test method.

    Re-probing in ``oss_pipelines.is_oss()`` happens on every call, so
    flipping the env var alone is enough to redirect the factory and
    behaviour predicates without touching ``sys.modules``.
    """

    _runtime: str = "oss"

    def setUp(self):
        super().setUp()
        self._prior_runtime = os.environ.get("SDP_META_RUNTIME")
        os.environ["SDP_META_RUNTIME"] = self._runtime

    def tearDown(self):
        if self._prior_runtime is None:
            os.environ.pop("SDP_META_RUNTIME", None)
        else:
            os.environ["SDP_META_RUNTIME"] = self._prior_runtime
        super().tearDown()


class TestFactoryDispatch(unittest.TestCase):
    """``DataflowPipeline(...)`` returns the right class based on runtime."""

    def setUp(self):
        self._prior_runtime = os.environ.get("SDP_META_RUNTIME")

    def tearDown(self):
        if self._prior_runtime is None:
            os.environ.pop("SDP_META_RUNTIME", None)
        else:
            os.environ["SDP_META_RUNTIME"] = self._prior_runtime

    def test_oss_runtime_returns_oss_subclass(self):
        os.environ["SDP_META_RUNTIME"] = "oss"
        # Bypass __init__ — we only want to verify that __new__ dispatched
        # to the right concrete class.
        with patch.object(DataflowPipeline, "__init__", return_value=None):
            instance = DataflowPipeline(
                spark=MagicMock(),
                dataflow_spec=MagicMock(),
                view_name="v",
            )
        self.assertIsInstance(instance, OSSDataflowPipeline)

    def test_databricks_runtime_returns_base_class(self):
        os.environ["SDP_META_RUNTIME"] = "databricks"
        with patch.object(DataflowPipeline, "__init__", return_value=None):
            instance = DataflowPipeline(
                spark=MagicMock(),
                dataflow_spec=MagicMock(),
                view_name="v",
            )
        # ``OSSDataflowPipeline`` is a subclass of ``DataflowPipeline`` so
        # ``isinstance(instance, DataflowPipeline)`` is true for both.
        # Pin the concrete type instead.
        self.assertIs(type(instance), DataflowPipeline)
        self.assertNotIsInstance(instance, OSSDataflowPipeline)

    def test_explicit_oss_subclass_construction_bypasses_factory_dispatch(self):
        """``__new__`` factory dispatch is keyed on ``cls is DataflowPipeline``.

        Direct ``OSSDataflowPipeline(...)`` must still produce an
        ``OSSDataflowPipeline`` regardless of runtime — the class
        identity check in ``DataflowPipeline.__new__`` short-circuits
        the factory. (The runtime-mismatch guard added in
        ``OSSDataflowPipeline.__init__`` is exercised separately by
        ``test_explicit_oss_construction_on_lakeflow_raises``; here we
        patch ``__init__`` so we're only asserting the ``__new__`` path.)
        """
        os.environ["SDP_META_RUNTIME"] = "databricks"
        with patch.object(OSSDataflowPipeline, "__init__", return_value=None):
            instance = OSSDataflowPipeline(
                spark=MagicMock(),
                dataflow_spec=MagicMock(),
                view_name="v",
            )
        self.assertIs(type(instance), OSSDataflowPipeline)

    def test_explicit_oss_construction_on_lakeflow_raises(self):
        """``OSSDataflowPipeline(...)`` on Lakeflow fails fast in ``__init__``.

        Forcing the OSS subclass on a Databricks runtime is almost
        always a programming error — the OSS code path side-channels
        external-table registration in a way that would compete with
        Lakeflow's native registration. The guard raises with an
        actionable message that points at the factory and the env var.
        """
        os.environ["SDP_META_RUNTIME"] = "databricks"
        with self.assertRaises(RuntimeError) as ctx:
            OSSDataflowPipeline(
                spark=MagicMock(),
                dataflow_spec=MagicMock(),
                view_name="v",
            )
        msg = str(ctx.exception)
        self.assertIn("OSSDataflowPipeline", msg)
        self.assertIn("Lakeflow", msg)
        self.assertIn("DataflowPipeline(...)", msg)
        self.assertIn("SDP_META_RUNTIME=oss", msg)


class TestOSSCDCRaises(_OSSRuntimeMixin, unittest.TestCase):
    """``cdc_apply_changes`` and ``apply_changes_from_snapshot`` raise on OSS."""

    @staticmethod
    def _make_instance():
        # Bypass __init__: the override methods we test do not read any
        # state set up by __initialize_dataflow_pipeline.
        return OSSDataflowPipeline.__new__(OSSDataflowPipeline)

    def test_cdc_apply_changes_raises(self):
        instance = self._make_instance()
        with self.assertRaises(NotImplementedError) as ctx:
            instance.cdc_apply_changes()
        msg = str(ctx.exception)
        self.assertIn("create_auto_cdc_flow", msg)
        self.assertIn("Lakeflow", msg)
        self.assertIn("SDP_META_RUNTIME=databricks", msg)

    def test_apply_changes_from_snapshot_raises(self):
        instance = self._make_instance()
        with self.assertRaises(NotImplementedError) as ctx:
            instance.apply_changes_from_snapshot()
        msg = str(ctx.exception)
        # Both AutoCDC entry points share the same canonical error
        # (``oss_pipelines.cdc_not_supported_error``); pin all the
        # remediation hints here too so a regression in either path is
        # caught.
        self.assertIn("create_auto_cdc_flow", msg)
        self.assertIn("Lakeflow", msg)
        self.assertIn("SDP_META_RUNTIME=databricks", msg)


class TestOSSRegisterTableWithDqe(_OSSRuntimeMixin, unittest.TestCase):
    """``_register_table_with_dqe`` on OSS routes through the shim."""

    def test_register_strips_lakeflow_only_kwargs(self):
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.spark = MagicMock()

        def qf():
            return MagicMock()

        with patch.object(oss_dpl_mod, "dp") as mock_dp, \
                patch.object(oss_dpl_mod, "oss_dp") as mock_oss_dp:
            mock_oss_dp.wrap_dqe = MagicMock(return_value=qf)
            mock_oss_dp.filter_table_kwargs = MagicMock(
                side_effect=lambda kw: {k: v for k, v in kw.items()
                                        if k not in ("cluster_by_auto", "path")}
            )
            mock_oss_dp.ensure_external_delta_table = MagicMock(return_value=True)
            mock_dp.table = MagicMock(return_value="decorated")
            result = instance._register_table_with_dqe(
                qf,
                name="bronze.customers",
                expect_all={"a": "x > 0"},
                cluster_by_auto=True,
                path="/tmp/bronze/customers",
                comment="hi",
            )

        # wrap_dqe was invoked with all three DQE flavours.
        mock_oss_dp.wrap_dqe.assert_called_once()
        wrap_kwargs = mock_oss_dp.wrap_dqe.call_args.kwargs
        self.assertEqual(wrap_kwargs["expect_all"], {"a": "x > 0"})
        self.assertIsNone(wrap_kwargs["expect_all_or_drop"])
        self.assertIsNone(wrap_kwargs["expect_all_or_fail"])

        # The external Delta table at the requested path was pre-created
        # so ``dp.table(name=...)`` resolves to that location.
        mock_oss_dp.ensure_external_delta_table.assert_called_once_with(
            instance.spark, "bronze.customers", "/tmp/bronze/customers", schema=None
        )

        # filter_table_kwargs was called with the table-level kwargs only.
        mock_oss_dp.filter_table_kwargs.assert_called_once()
        filter_arg = mock_oss_dp.filter_table_kwargs.call_args.args[0]
        self.assertIn("cluster_by_auto", filter_arg)
        self.assertIn("path", filter_arg)
        self.assertIn("comment", filter_arg)

        # ``dp.table`` saw the filtered kwargs (no cluster_by_auto / path).
        mock_dp.table.assert_called_once()
        dp_kwargs = mock_dp.table.call_args.kwargs
        self.assertEqual(dp_kwargs["name"], "bronze.customers")
        self.assertNotIn("cluster_by_auto", dp_kwargs)
        self.assertNotIn("path", dp_kwargs)
        self.assertEqual(dp_kwargs["comment"], "hi")
        self.assertEqual(result, "decorated")

    def test_register_without_path_skips_pre_creation(self):
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.spark = MagicMock()

        def qf():
            return MagicMock()

        with patch.object(oss_dpl_mod, "dp") as mock_dp, \
                patch.object(oss_dpl_mod, "oss_dp") as mock_oss_dp:
            mock_oss_dp.wrap_dqe = MagicMock(return_value=qf)
            mock_oss_dp.filter_table_kwargs = MagicMock(side_effect=lambda kw: dict(kw))
            mock_oss_dp.ensure_external_delta_table = MagicMock(return_value=False)
            mock_dp.table = MagicMock(return_value="decorated")
            instance._register_table_with_dqe(qf, name="t", comment="hi")

        # The helper is still called (it no-ops on path=None) — that's
        # what keeps the OSS code path uniform regardless of whether the
        # onboarding spec set a per-table path.
        mock_oss_dp.ensure_external_delta_table.assert_called_once_with(
            instance.spark, "t", None, schema=None
        )


class TestOSSStructSchema(_OSSRuntimeMixin, unittest.TestCase):
    """``_oss_struct_schema`` converts ``self.schema_json`` to a StructType.

    The result feeds ``ensure_external_delta_table`` so its
    ``CREATE TABLE ... USING DELTA LOCATION`` carries real columns and
    succeeds at an empty location. Bronze specs carry ``schema_json``;
    silver specs don't (→ ``None``).
    """

    def test_returns_structtype_from_schema_json(self):
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.schema_json = {
            "type": "struct",
            "fields": [
                {"name": "id", "type": "string", "nullable": True, "metadata": {}},
                {"name": "amount", "type": "long", "nullable": True, "metadata": {}},
            ],
        }
        schema = instance._oss_struct_schema()
        self.assertIsInstance(schema, oss_dpl_mod.StructType)
        self.assertEqual([f.name for f in schema.fields], ["id", "amount"])

    def test_returns_none_when_schema_json_absent(self):
        # Silver specs carry no source schema_json — path-binding then
        # falls back to the warn-and-continue branch in
        # ensure_external_delta_table.
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.schema_json = None
        self.assertIsNone(instance._oss_struct_schema())

    def test_returns_none_on_unparseable_schema_json(self):
        # A malformed schema_json must degrade to None (best-effort), not
        # raise — the path-binding then warns rather than aborting the run.
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.schema_json = {"not": "a valid struct"}
        self.assertIsNone(instance._oss_struct_schema())


class TestOSSCreateStreamingTable(_OSSRuntimeMixin, unittest.TestCase):
    """``create_streaming_table`` on OSS strips Lakeflow-only kwargs."""

    def test_streaming_table_with_dqe_raises_not_silently_drops(self):
        """If a future refactor adds a non-CDC caller of
        ``create_streaming_table`` that supplies DQE on a streaming
        table, the OSS subclass must fail loudly rather than silently
        drop the constraints.

        Today this guard is unreachable — the only callers
        (``cdc_apply_changes`` / ``apply_changes_from_snapshot``) raise
        ``NotImplementedError`` upstream on OSS. This test pins the
        invariant so a future change that introduces a new caller
        without per-``append_flow`` DQE rewriting fails CI instead of
        silently dropping data-quality enforcement.
        """
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.dataflowSpec = _FakeDataflowSpec()
        instance.spark = MagicMock()
        # Stub get_dq_expectations to return non-empty DQE — that's the
        # state the guard must catch.
        instance.get_dq_expectations = MagicMock(
            return_value=({"a": "x > 0"}, None, None)
        )

        with self.assertRaises(NotImplementedError) as ctx:
            instance.create_streaming_table(struct_schema=None, target_path="/tmp/x")
        msg = str(ctx.exception)
        self.assertIn("create_streaming_table", msg)
        self.assertIn("DQE", msg)
        # Message must point at the two viable workarounds:
        # per-append_flow inlining and the env-var override.
        self.assertIn("append_flow", msg)
        self.assertIn("SDP_META_RUNTIME=databricks", msg)

    def test_streaming_table_without_dqe_does_not_raise(self):
        """The DQE guard must NOT fire when expectations are empty —
        otherwise it'd break every existing OSS-path streaming table
        construction.
        """
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.dataflowSpec = _FakeDataflowSpec()
        instance.spark = MagicMock()
        instance.get_dq_expectations = MagicMock(return_value=(None, None, None))

        with patch.object(oss_dpl_mod, "dp") as mock_dp, \
                patch.object(oss_dpl_mod, "oss_dp") as mock_oss_dp, \
                patch.object(oss_dpl_mod, "DataflowSpecUtils") as mock_utils:
            mock_utils.get_partition_cols = MagicMock(return_value=None)
            mock_oss_dp.filter_table_kwargs = MagicMock(side_effect=lambda kw, **_: dict(kw))
            mock_oss_dp.ensure_external_delta_table = MagicMock(return_value=True)
            mock_dp.create_streaming_table = MagicMock()
            # Must not raise.
            instance.create_streaming_table(struct_schema=None, target_path="/tmp/x")
        mock_dp.create_streaming_table.assert_called_once()

    def test_filters_lakeflow_only_kwargs(self):
        instance = OSSDataflowPipeline.__new__(OSSDataflowPipeline)
        instance.dataflowSpec = _FakeDataflowSpec()
        instance.spark = MagicMock()
        # No DQE on the spec — guard must not fire. Stub
        # ``get_dq_expectations`` so the test works whether or not the
        # method exists on the base class.
        instance.get_dq_expectations = MagicMock(return_value=(None, None, None))

        with patch.object(oss_dpl_mod, "dp") as mock_dp, \
                patch.object(oss_dpl_mod, "oss_dp") as mock_oss_dp, \
                patch.object(oss_dpl_mod, "DataflowSpecUtils") as mock_utils:
            mock_utils.get_partition_cols = MagicMock(return_value=None)
            captured_filter_kwargs = {}

            def fake_filter(kw, **kwargs):
                captured_filter_kwargs.update(kwargs)
                return {k: v for k, v in kw.items()
                        if k not in ("cluster_by_auto", "path",
                                     "expect_all", "expect_all_or_drop",
                                     "expect_all_or_fail")}

            mock_oss_dp.filter_table_kwargs = MagicMock(side_effect=fake_filter)
            mock_oss_dp.ensure_external_delta_table = MagicMock(return_value=True)
            mock_dp.create_streaming_table = MagicMock()

            instance.create_streaming_table(struct_schema=None, target_path="/tmp/x")

        # ``also_drop_dqe`` must be set so DQE kwargs (if added by a
        # future caller) get stripped on OSS.
        self.assertTrue(captured_filter_kwargs.get("also_drop_dqe"))
        # The streaming table's path is honoured via the side-channel
        # external-table pre-creation.
        mock_oss_dp.ensure_external_delta_table.assert_called_once_with(
            instance.spark, "test_db.test_table", "/tmp/x", schema=None
        )
        mock_dp.create_streaming_table.assert_called_once()
        st_kwargs = mock_dp.create_streaming_table.call_args.kwargs
        self.assertEqual(st_kwargs["name"], "test_db.test_table")
        self.assertNotIn("cluster_by_auto", st_kwargs)
        self.assertNotIn("path", st_kwargs)
        self.assertNotIn("expect_all", st_kwargs)


if __name__ == "__main__":
    unittest.main()
