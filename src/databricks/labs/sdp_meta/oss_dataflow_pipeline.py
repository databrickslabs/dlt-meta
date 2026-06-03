"""OSS Apache Spark subclass of :class:`DataflowPipeline`.

The base :class:`DataflowPipeline` targets the Databricks Lakeflow runtime
(the superset of ``pyspark.pipelines`` that ships ``expect_all`` /
``create_auto_cdc_flow`` / Lakeflow-only kwargs like ``cluster_by_auto``
and ``path``). On plain Apache Spark 4.1+ ``pyspark.pipelines`` (the public
OSS subset) those Lakeflow extensions are unavailable. ``OSSDataflowPipeline``
overrides exactly the methods that diverge so the OSS-supported subset of
SDP-META runs natively while the Lakeflow-only paths raise a clear,
actionable :class:`NotImplementedError`.

Methods overridden here:

- :meth:`_register_table_with_dqe` — DQE expectations are inlined into the
  query function (``df.where`` for ``or_drop``, ``raise_error`` for
  ``or_fail``, log-only for ``expect_all``) instead of stacking
  Lakeflow-only ``dp.expect_*`` decorators on top of ``dp.table``. The
  ``path`` kwarg is honoured by side-channel: an external Delta table
  is pre-created at that path so the subsequent ``dp.table`` writes
  into the configured location (``pyspark.pipelines.table`` itself
  rejects a ``path`` kwarg).
- :meth:`create_streaming_table` — the kwargs accepted by
  ``pyspark.pipelines.create_streaming_table`` on OSS are a strict subset
  of Lakeflow's. ``cluster_by_auto`` and the ``expect_*`` kwargs are
  filtered out with a one-shot warning per dropped kwarg; ``path`` is
  honoured via the same external-table side channel as
  ``_register_table_with_dqe``.
- :meth:`cdc_apply_changes` and :meth:`apply_changes_from_snapshot` —
  ``create_auto_cdc_flow`` / ``create_auto_cdc_from_snapshot_flow`` are
  Databricks-only; both raise :class:`NotImplementedError` with a pointer
  to either remove the CDC section from the dataflow spec or run on
  Databricks Lakeflow.

Selection is automatic. ``DataflowPipeline(...)`` returns an
``OSSDataflowPipeline`` instance when the runtime probe lands on OSS (see
:mod:`databricks.labs.sdp_meta.oss_pipelines`); existing call sites do not
need to change. Callers that want to force the OSS class explicitly can
instantiate ``OSSDataflowPipeline`` directly.
"""
from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql.types import StructType

from databricks.labs.sdp_meta import oss_pipelines as oss_dp
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils


class OSSDataflowPipeline(DataflowPipeline):
    """OSS Apache Spark 4.1+ flavour of :class:`DataflowPipeline`.

    Overrides Lakeflow-specific methods with implementations that use
    only the public ``pyspark.pipelines`` API surface. Unsupported paths
    (``create_auto_cdc_flow`` and ``create_auto_cdc_from_snapshot_flow``)
    raise :class:`NotImplementedError` with an actionable message.
    """

    def __init__(self, *args, **kwargs):
        """Initialize and double-check that we're really on OSS Spark.

        The :meth:`DataflowPipeline.__new__` factory routes plain
        ``DataflowPipeline(...)`` constructions here when the runtime
        probe lands on OSS, but a caller can also instantiate
        ``OSSDataflowPipeline`` explicitly (e.g. in tests). When that
        explicit construction collides with a Lakeflow runtime, fail
        loudly with an actionable message rather than silently writing
        Lakeflow tables through the OSS-only ``ensure_external_delta_table``
        side channel — which on Lakeflow would either no-op (correct on
        Databricks but surprising) or compete with Lakeflow's own table
        registration.
        """
        if oss_dp.is_databricks():
            raise RuntimeError(
                "OSSDataflowPipeline was constructed on a Databricks "
                "Lakeflow runtime. Use DataflowPipeline(...) instead — the "
                "factory dispatches to the right concrete class based on "
                "the runtime probe. Or set SDP_META_RUNTIME=oss to force "
                "the OSS code path on Lakeflow (only useful in tests)."
            )
        super().__init__(*args, **kwargs)

    def _assert_no_streaming_table_dqe(self) -> None:
        """Fail loudly if a caller reached :meth:`create_streaming_table`
        with DQE expectations set on the streaming table.

        On Lakeflow, ``dp.create_streaming_table`` accepts
        ``expect_all`` / ``expect_all_or_drop`` / ``expect_all_or_fail``
        kwargs that enforce constraints at the streaming-table level
        across all bound ``append_flow`` queries. On OSS,
        ``pyspark.pipelines.create_streaming_table`` rejects those
        kwargs and SDP-META currently has no per-``append_flow`` DQE
        rewriting on OSS, so silently dropping the constraints would
        be a data-quality footgun.

        Today this guard is unreachable in practice — the only callers
        of ``create_streaming_table`` (the AutoCDC entry points) raise
        :class:`NotImplementedError` upstream on OSS. The guard exists
        so a future refactor that adds a non-CDC caller (e.g. an
        ``append_flow``-only pipeline that pre-declares its target
        streaming table) fails fast with an actionable message
        instead of silently dropping the DQE on the floor.
        """
        get_dq = getattr(self, "get_dq_expectations", None)
        if get_dq is None:
            return
        try:
            expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = get_dq()
        except Exception:
            # ``get_dq_expectations`` reads
            # ``self.dataflowSpec.dataQualityExpectations``; if it isn't
            # set / parseable we can't tell whether DQE is present,
            # so default to permissive (the runtime behaviour was
            # silently-drop before this guard existed; preserve that
            # rather than introduce a new failure mode).
            return
        if expect_all_dict or expect_all_or_drop_dict or expect_all_or_fail_dict:
            raise NotImplementedError(
                "OSSDataflowPipeline.create_streaming_table reached with "
                "DQE expectations set on the streaming table "
                "(expect_all / expect_all_or_drop / expect_all_or_fail). "
                "OSS pyspark.pipelines.create_streaming_table does not "
                "accept these kwargs, and SDP-META has no per-append_flow "
                "DQE rewriting on OSS yet. Either move the constraints "
                "onto each individual append_flow query "
                "(@_register_table_with_dqe inlines them via wrap_dqe), "
                "remove them from the streaming-table spec, or run on "
                "Databricks Lakeflow. Set SDP_META_RUNTIME=databricks "
                "if runtime detection is incorrect."
            )

    def _oss_struct_schema(self):
        """Best-effort ``StructType`` for the target table, or ``None``.

        Passed to :func:`oss_pipelines.ensure_external_delta_table` so its
        ``CREATE TABLE ... USING DELTA LOCATION`` carries real columns and
        succeeds at an empty location (a schema-less create raises
        ``DELTA_FAILED_INFER_SCHEMA``). Bronze specs carry the source schema
        in ``self.schema_json``; silver specs don't, so this returns ``None``
        there and the path-binding falls back to the warn-and-continue path
        in ``ensure_external_delta_table``.
        """
        schema_json = getattr(self, "schema_json", None)
        if not schema_json:
            return None
        try:
            return StructType.fromJson(schema_json)
        except Exception:
            return None

    def _register_table_with_dqe(
        self,
        query_function,
        *,
        name,
        expect_all=None,
        expect_all_or_drop=None,
        expect_all_or_fail=None,
        **table_kwargs,
    ):
        """Register a table on OSS with DQE constraints inlined.

        Lakeflow stacks ``dp.expect_all`` / ``dp.expect_all_or_drop`` /
        ``dp.expect_all_or_fail`` decorators on top of ``dp.table``. Those
        decorators do not exist on OSS, so the constraints are inlined
        into the query function instead:

        - ``expect_all_or_drop`` → ``df.where(constraint AND ...)``.
        - ``expect_all_or_fail`` → synthetic column whose CASE branch
          calls ``raise_error`` on a violating row (fails the update at
          execution time on the first violation).
        - ``expect_all`` → logged once at registration time. OSS SDP has
          no event-log surface for per-expectation metrics, so there is
          nothing to enforce or surface — this matches Lakeflow's
          "include in target dataset, log metrics" semantics minus the
          metrics.

        ``cluster_by_auto`` is stripped from ``table_kwargs`` because
        OSS ``pyspark.pipelines.table`` rejects it. ``path`` is also
        stripped (OSS ``dp.table`` rejects it too) but the value is
        honoured by side-channel: :func:`oss_pipelines.ensure_external_delta_table`
        pre-creates an external Delta table at the configured location
        with the same name, so when the OSS planner materialises
        ``dp.table(name=...)`` it writes into the path the onboarding
        spec asked for.
        """
        path = table_kwargs.get("path")
        oss_dp.ensure_external_delta_table(
            self.spark, name, path, schema=self._oss_struct_schema()
        )
        wrapped_qf = oss_dp.wrap_dqe(
            query_function,
            expect_all=expect_all,
            expect_all_or_drop=expect_all_or_drop,
            expect_all_or_fail=expect_all_or_fail,
        )
        return dp.table(
            wrapped_qf,
            name=name,
            **oss_dp.filter_table_kwargs(table_kwargs),
        )

    def create_streaming_table(self, struct_schema, target_path=None):
        """Create a streaming table on OSS, filtering Lakeflow-only kwargs.

        ``pyspark.pipelines.create_streaming_table`` on OSS does not
        accept ``cluster_by_auto``, ``path``, or any of the ``expect_*``
        kwargs that Lakeflow accepts. The shim filters those out with a
        de-duplicated warning per dropped kwarg name; ``target_path``
        is honoured via :func:`oss_pipelines.ensure_external_delta_table`
        (an external Delta table is pre-created at the requested
        location so the subsequent ``dp.create_streaming_table``
        resolves to that path-backed table).

        ``target_path`` flows through a single channel: it's bound once
        to the local ``path`` variable, side-channelled into
        ``ensure_external_delta_table``, and passed by-name into the
        kwargs dict that ``filter_table_kwargs`` strips. The reader
        only needs to track one source of truth.

        DQE on a streaming table is unreachable on this code path
        *today* because the AutoCDC entry points (the only callers of
        ``create_streaming_table`` that pass DQE on Lakeflow) raise
        :class:`NotImplementedError` further upstream. The
        ``_assert_no_streaming_table_dqe`` pre-flight below makes that
        invariant explicit so a future refactor that adds a non-CDC
        caller can't silently drop DQE constraints — streaming-table
        DQE on OSS would require per-``append_flow``-query rewriting
        (a separate, larger change) and is currently unsupported.
        """
        self._assert_no_streaming_table_dqe()
        target_cl = self.dataflowSpec.targetDetails.get("catalog", None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ""
        target_db_name = self.dataflowSpec.targetDetails["database"]
        target_table_name = self.dataflowSpec.targetDetails["table"]
        target_table = f"{target_cl_name}{target_db_name}.{target_table_name}"

        # Single source of truth for the per-table location.
        path = target_path

        oss_dp.ensure_external_delta_table(
            self.spark, target_table, path, schema=struct_schema
        )

        cluster_by_auto = (
            self.dataflowSpec.clusterByAuto
            if hasattr(self.dataflowSpec, "clusterByAuto")
            and self.dataflowSpec.clusterByAuto is not None
            else False
        )

        st_kwargs = oss_dp.filter_table_kwargs(
            dict(
                table_properties=self.dataflowSpec.tableProperties,
                partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
                cluster_by_auto=cluster_by_auto,
                path=path,
                schema=struct_schema,
            ),
            also_drop_dqe=True,
        )
        dp.create_streaming_table(name=target_table, **st_kwargs)

    def cdc_apply_changes(self):
        """``create_auto_cdc_flow`` is Databricks-only; raise.

        See :func:`oss_pipelines.cdc_not_supported_error` for the full
        message including remediation guidance.
        """
        raise oss_dp.cdc_not_supported_error()

    def apply_changes_from_snapshot(self):
        """``create_auto_cdc_from_snapshot_flow`` is Databricks-only; raise.

        See :func:`oss_pipelines.cdc_not_supported_error` for the full
        message including remediation guidance.
        """
        raise oss_dp.cdc_not_supported_error()


__all__ = ["OSSDataflowPipeline"]
