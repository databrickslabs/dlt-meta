"""Runtime detection + OSS shim layer for ``pyspark.pipelines``.

The repo's runtime calls fall into two categories:

1. APIs in the OSS public ``pyspark.pipelines`` surface (Apache Spark 4.1+):
   ``table``, ``materialized_view``, ``temporary_view``,
   ``create_streaming_table``, ``append_flow``, ``create_sink``.
   These work identically on OSS Spark and Databricks Lakeflow.

2. Databricks-only extensions that the Lakeflow runtime grafts onto
   ``pyspark.pipelines``:

   - ``expect_all`` / ``expect_all_or_drop`` / ``expect_all_or_fail``
   - ``create_auto_cdc_flow``
   - ``create_auto_cdc_from_snapshot_flow``

   These do not exist in the OSS public API. Lakeflow also accepts a few
   extra kwargs on OSS-public APIs (``cluster_by_auto`` and ``path`` on
   ``table`` / ``create_streaming_table``) which the OSS signatures reject.

This module:

- detects which runtime is active (Lakeflow vs. plain OSS Spark),
- exposes ``wrap_dqe(...)`` which injects DQE enforcement directly into a
  query function on OSS (drop / fail / log-only behaviours),
- exposes ``filter_table_kwargs(...)`` which strips kwargs that OSS
  ``pyspark.pipelines`` does not accept,
- exposes ``cdc_not_supported_error()`` which raises a clear, actionable
  NotImplementedError for AutoCDC paths on OSS.

Detection order (the ``SDP_META_RUNTIME`` env override is consulted on
every ``is_databricks()`` / ``is_oss()`` call so it stays live for the
process; the ``pyspark.pipelines`` symbol probe is memoized after the
first call — see :func:`refresh_runtime` to reset it):

1. ``SDP_META_RUNTIME`` env var (``databricks`` / ``lakeflow`` / ``oss`` /
   ``oss-spark``).
2. Probe for the Lakeflow extension symbols on ``pyspark.pipelines``
   (computed once per process, then cached).
3. Default to OSS.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable, Mapping, Optional
from urllib.parse import urlparse

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType

logger = logging.getLogger("databricks.labs.sdp_meta.oss_pipelines")

# Public env override for the runtime detection. Set to ``databricks`` when
# running under Lakeflow if probing somehow fails, or ``oss`` to force the
# OSS-Spark code path even on Databricks (useful in tests).
_RUNTIME_ENV_VAR = "SDP_META_RUNTIME"

# When set (truthy), :func:`ensure_external_delta_table` raises a
# ``RuntimeError`` instead of logging a WARNING when the requested path
# disagrees with the LOCATION of an already-registered table. Useful in CI
# for OSS users who iterate frequently on onboarding-spec paths and want
# drift to fail loudly. Accepts ``1``/``true``/``yes``/``on`` (case-
# insensitive). Default off — preserves the warn-and-proceed behavior so
# accidental flips on the spec don't refuse to start the pipeline.
_REGISTER_STRICT_ENV_VAR = "SDP_META_OSS_REGISTER_STRICT"


def _strict_external_register() -> bool:
    """True when ``SDP_META_OSS_REGISTER_STRICT`` is set to a truthy value."""
    return os.environ.get(_REGISTER_STRICT_ENV_VAR, "").strip().lower() in (
        "1", "true", "yes", "on",
    )


# Kwargs accepted by Lakeflow's ``dp.table`` / ``dp.create_streaming_table``
# but NOT by OSS Spark 4.1's ``pyspark.pipelines``. The shim strips these on
# the OSS path and emits a single warning per kwarg.
_LAKEFLOW_ONLY_TABLE_KWARGS = ("cluster_by_auto", "path")

# Lakeflow-only DQE kwargs accepted by ``dp.create_streaming_table`` on
# Lakeflow only.
_LAKEFLOW_ONLY_DQE_KWARGS = ("expect_all", "expect_all_or_drop", "expect_all_or_fail")

# Process-wide set of (kwarg_name) we've already warned about in
# :func:`filter_table_kwargs`. SDP-META can register hundreds of
# ``dp.table`` calls per run (one per dataflow spec row × each DQE
# stack), and re-warning on every call floods the operator log without
# adding signal. We de-dup by kwarg name only (not by value) — the
# operator action is "remove this kwarg from the spec or migrate to
# Lakeflow", which doesn't change based on the value. Tests reset
# this via :func:`reset_kwarg_warning_state` rather than reach into
# the private name.
_warned_kwargs: set = set()

# DQE constraint dict type alias.
ConstraintDict = Optional[Mapping[str, str]]


# Memoized result of the ``pyspark.pipelines`` symbol probe. The probe
# imports ``pyspark.pipelines`` and inspects it for the Lakeflow-only
# ``create_auto_cdc_flow`` symbol. That answer cannot change within a
# running process — a live Spark session does not swap its pipelines
# module mid-run — so it is computed once (effectively when the first
# ``DataflowPipeline(...)`` is built against the session) and reused for
# every later call. ``None`` means "not probed yet". Reset with
# :func:`refresh_runtime` (used by tests that swap the pipelines module).
_probed_pyspark_runtime: Optional[str] = None


def _probe_pyspark_symbols() -> str:
    """Probe ``pyspark.pipelines`` for Lakeflow-only symbols (memoized).

    Returns ``"databricks"`` if ``pyspark.pipelines`` exposes
    ``create_auto_cdc_flow`` (a Lakeflow extension that has no OSS
    counterpart and is the narrowest single signal), otherwise ``"oss"``.

    The import + ``hasattr`` reflection runs only on the first call and
    the result is cached in ``_probed_pyspark_runtime``; subsequent calls
    return the cached value. SDP-META can register hundreds of tables per
    run, each routing through ``filter_table_kwargs`` / ``wrap_dqe`` /
    ``ensure_external_delta_table`` which gate on the runtime, so this
    keeps the probe off the hot path. Call :func:`refresh_runtime` to
    force a re-probe.

    Why probe a single symbol instead of an AND of two: the prior
    ``hasattr(expect_all) AND hasattr(create_auto_cdc_flow)`` check
    drifted unsafely if either OSS started shipping a stub for one
    symbol or Lakeflow shipped a release with only one of the two.
    ``create_auto_cdc_flow`` is the more recent Lakeflow extension and
    the one that actually has no OSS counterpart, so it's the right
    single signal to key on.
    """
    global _probed_pyspark_runtime
    if _probed_pyspark_runtime is None:
        try:
            from pyspark import pipelines as _dp  # type: ignore[import-not-found]
        except ImportError:
            _probed_pyspark_runtime = "oss"
        else:
            _probed_pyspark_runtime = (
                "databricks" if hasattr(_dp, "create_auto_cdc_flow") else "oss"
            )
    return _probed_pyspark_runtime


def _probe_runtime() -> str:
    """Resolve the active runtime: env override first, then the cached probe.

    The ``SDP_META_RUNTIME`` env override is honoured on every call (a
    cheap ``os.environ`` lookup) so it stays live for the process — a
    CLI flag or test can force the runtime without a restart. Only when
    no override is set do we fall back to the memoized
    :func:`_probe_pyspark_symbols` result.
    """
    override = os.environ.get(_RUNTIME_ENV_VAR, "").strip().lower()
    if override in ("databricks", "lakeflow"):
        return "databricks"
    if override in ("oss", "oss-spark", "spark"):
        return "oss"
    return _probe_pyspark_symbols()


def refresh_runtime() -> str:
    """Discard the cached ``pyspark.pipelines`` probe and re-evaluate.

    The pyspark symbol probe is memoized after its first call (see
    :func:`_probe_pyspark_symbols`). Call this only if the underlying
    ``pyspark.pipelines`` module is swapped mid-process — chiefly tests
    that inject an OSS-only or Lakeflow stub. Returns the freshly
    resolved runtime string for convenience.
    """
    global _probed_pyspark_runtime
    _probed_pyspark_runtime = None
    return _probe_runtime()


# Snapshot of the runtime at import time. ``is_databricks()`` / ``is_oss()``
# consult the ``SDP_META_RUNTIME`` env override live on every call but reuse
# the memoized ``pyspark.pipelines`` probe, so an override set after import
# still takes effect immediately while the costly symbol reflection runs only
# once per process.
RUNTIME: str = _probe_runtime()


def is_databricks() -> bool:
    """True when running on a Databricks Lakeflow runtime.

    Consults the ``SDP_META_RUNTIME`` env override on every call (cheap)
    but reuses the memoized ``pyspark.pipelines`` symbol probe, so the
    expensive reflection runs only once per process. A runtime override
    set after import is still honoured immediately; call
    :func:`refresh_runtime` to force the symbol probe itself to re-run.
    """
    return _probe_runtime() == "databricks"


def is_oss() -> bool:
    """True when running on plain Apache Spark 4.1+ ``pyspark.pipelines``.

    Consults the ``SDP_META_RUNTIME`` env override on every call (cheap)
    but reuses the memoized ``pyspark.pipelines`` symbol probe, so the
    expensive reflection runs only once per process. A runtime override
    set after import is still honoured immediately; call
    :func:`refresh_runtime` to force the symbol probe itself to re-run.
    """
    return _probe_runtime() == "oss"


def reset_kwarg_warning_state() -> None:
    """Clear the de-dup set used by :func:`filter_table_kwargs`.

    Tests that want to assert a warning fires (not just that it would
    fire) should call this in setUp / tearDown so the previous test's
    state doesn't suppress the warning under test.
    """
    _warned_kwargs.clear()


def filter_table_kwargs(kwargs: dict, *, also_drop_dqe: bool = False) -> dict:
    """Remove kwargs that OSS ``pyspark.pipelines`` does not accept.

    Returns a new dict suitable for passing to ``dp.table`` /
    ``dp.create_streaming_table`` on OSS. Warnings for genuinely-dropped
    kwargs are de-duplicated *across the process* by kwarg name so a
    pipeline that registers hundreds of tables doesn't flood the
    operator log — the actionable signal ("remove this kwarg from the
    spec or migrate to Lakeflow") is identical on every repeat.

    ``path`` is NOT warned about because :class:`OSSDataflowPipeline`
    honours it via the external-Delta-table side channel in
    :func:`ensure_external_delta_table` before this filter runs — by
    the time we strip it from the kwargs dict, the configured location
    has already been registered with the catalog under the same name
    SDP-META is about to pass to ``dp.table``. A warning would
    misleadingly imply data loss.

    On Databricks this is the identity function (no kwargs are stripped).
    """
    if is_databricks():
        return dict(kwargs)
    out = {k: v for k, v in kwargs.items() if k not in _LAKEFLOW_ONLY_TABLE_KWARGS}
    if also_drop_dqe:
        out = {k: v for k, v in out.items() if k not in _LAKEFLOW_ONLY_DQE_KWARGS}
    dropped = sorted(set(kwargs.keys()) - set(out.keys()))
    for k in dropped:
        v = kwargs.get(k)
        if v is None or v is False:
            continue
        if k == "path":
            # Side-channelled by ensure_external_delta_table (an external
            # Delta table at this path is already registered under the
            # same name); no data loss to warn about.
            continue
        if k in _warned_kwargs:
            # Already surfaced this kwarg name once in this process;
            # subsequent repeats add noise without signal.
            continue
        _warned_kwargs.add(k)
        logger.warning(
            "[oss_shim] dropping Lakeflow-only kwarg %s=%r (not supported "
            "by OSS pyspark.pipelines). Warning will be suppressed for "
            "subsequent occurrences of this kwarg in the current process.",
            k, v,
        )
    return out


def _combine_constraints(constraints: Mapping[str, str]) -> str:
    """AND together the SQL constraint expressions in a DQE dict."""
    return " AND ".join(f"({c})" for c in constraints.values())


def _wrap_with_drop(
    query_function: Callable[..., DataFrame],
    constraints: Mapping[str, str],
) -> Callable[..., DataFrame]:
    """Wrap ``query_function`` so its output filters out failing rows.

    Mirrors Lakeflow's ``expect_all_or_drop`` semantics: invalid records are
    silently dropped before being written to the target dataset.
    """
    if not constraints:
        return query_function
    where_expr = _combine_constraints(constraints)

    def _wrapped(*args: Any, **kwargs: Any) -> DataFrame:
        df = query_function(*args, **kwargs)
        return df.where(where_expr)

    _wrapped.__name__ = getattr(query_function, "__name__", "_wrapped")
    _wrapped.__doc__ = getattr(query_function, "__doc__", None)
    return _wrapped


def _wrap_with_fail(
    query_function: Callable[..., DataFrame],
    constraints: Mapping[str, str],
) -> Callable[..., DataFrame]:
    """Wrap ``query_function`` so a failing constraint aborts the pipeline.

    This is a best-effort port of Lakeflow's ``expect_all_or_fail`` to OSS.
    The wrapped query function evaluates a CASE expression inside a ``where``
    predicate; the violating branch calls ``raise_error``, which Spark only
    evaluates as rows are pulled through the plan. A filter predicate is used
    rather than ``withColumn(...).drop(...)`` because an add-then-drop column
    is dead code that Catalyst's column pruning removes *before* ``raise_error``
    is ever evaluated — which would silently turn this into a no-op. A filter,
    by contrast, must be evaluated for every row to decide inclusion, so the
    assertion reliably fires.

    **Semantics gap with Lakeflow — read before migrating.**

    Lakeflow's ``expect_all_or_fail`` aborts the *update* before any write
    happens — Lakeflow's planner runs an expectation pre-flight, so no
    violating row ever lands in the target table and no commit is made.

    The OSS shim has no equivalent pre-flight surface, so it raises
    *during streaming execution* on the first violating row pulled
    through the plan. By the time the exception propagates:

    - Some rows from the same micro-batch may already have been emitted
      to downstream operators (`append_flow` consumers, sinks, etc.).
    - The failing micro-batch will be retried on resume — Spark's
      structured-streaming engine treats it as a transient task failure
      and re-executes from the same offsets. If the violation is
      data-driven (the same row will violate again), the stream stays
      in a crash loop until the bad row ages out of the source or the
      checkpoint is bumped past it.
    - The target table commit for the failing micro-batch is rolled
      back by Spark's atomic-commit semantics, but commits for prior
      micro-batches in the same update are NOT rolled back — they
      already succeeded.

    Concretely: don't migrate a Lakeflow spec that depends on the
    "no violating row ever reaches the target" guarantee without
    re-validating against this OSS behaviour. For workloads where the
    pre-write guarantee matters, stay on Lakeflow.
    """
    if not constraints:
        return query_function
    cond = _combine_constraints(constraints)
    descs = ", ".join(constraints.keys()).replace("'", "''")
    # Valid rows return ``true`` (kept by ``where``); violating rows hit
    # ``raise_error`` in the ELSE branch, which aborts before the predicate
    # can return. The CASE result type is boolean (the ELSE never yields a
    # value), so this is a well-formed filter predicate.
    assert_sql = (
        f"CASE WHEN ({cond}) THEN true "
        f"ELSE raise_error(concat('SDP-META expect_or_fail violated: ', '{descs}')) "
        "END"
    )

    def _wrapped(*args: Any, **kwargs: Any) -> DataFrame:
        df = query_function(*args, **kwargs)
        return df.where(expr(assert_sql))

    _wrapped.__name__ = getattr(query_function, "__name__", "_wrapped")
    _wrapped.__doc__ = getattr(query_function, "__doc__", None)
    return _wrapped


def wrap_dqe(
    query_function: Callable[..., DataFrame],
    *,
    expect_all: ConstraintDict = None,
    expect_all_or_drop: ConstraintDict = None,
    expect_all_or_fail: ConstraintDict = None,
) -> Callable[..., DataFrame]:
    """Inject DQE constraints into a query function on OSS Spark.

    On Databricks this is the identity (the call site stacks the native
    ``dp.expect_*`` decorators around ``dp.table(...)`` instead).

    On OSS Spark:

    - ``expect_all_or_drop``: violating rows are filtered out via ``where``.
    - ``expect_all_or_fail``: violating rows raise via ``raise_error`` at
      execution time.
    - ``expect_all``: logged once at registration time. OSS SDP has no
      event-log surface for per-expectation metrics, so there is nothing to
      enforce or surface — this matches Lakeflow's "include in target
      dataset, log metrics" semantics minus the metrics.
    """
    if is_databricks():
        return query_function
    qf = query_function
    if expect_all_or_drop:
        qf = _wrap_with_drop(qf, expect_all_or_drop)
    if expect_all_or_fail:
        qf = _wrap_with_fail(qf, expect_all_or_fail)
    if expect_all:
        logger.info(
            "[oss_shim] expect_all (metrics-only on OSS, no enforcement): %s",
            sorted(expect_all.keys()),
        )
    return qf


_OSS_CDC_NOT_SUPPORTED = (
    "create_auto_cdc_flow / create_auto_cdc_from_snapshot_flow are not "
    "available in OSS Spark Declarative Pipelines (Apache Spark 4.1+). "
    "These are Databricks Lakeflow extensions that implement streaming "
    "MERGE / SCD semantics not present in OSS pyspark.pipelines. To run "
    "this dataflow spec on OSS Spark, remove the cdcApplyChanges / "
    "applyChangesFromSnapshot section, or run on Databricks Lakeflow. "
    "Set SDP_META_RUNTIME=databricks if detection is incorrect."
)


def cdc_not_supported_error() -> NotImplementedError:
    """Build the canonical AutoCDC-not-on-OSS error.

    Used by the ``cdc_apply_changes`` and ``apply_changes_from_snapshot``
    code paths in ``dataflow_pipeline.py``.
    """
    return NotImplementedError(_OSS_CDC_NOT_SUPPORTED)


def _split_qualified_name(full_name: str) -> tuple[Optional[str], str, str]:
    """Split ``catalog.schema.table`` / ``schema.table`` / ``table``.

    Returns ``(catalog, schema, table)`` with ``catalog`` set to ``None``
    when only two parts are supplied. A single-part name is treated as
    ``default.<table>`` to match Spark's bare-name resolution.
    """
    parts = [p for p in full_name.split(".") if p]
    if len(parts) == 1:
        return None, "default", parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[-3], parts[-2], parts[-1]


def _quote(ident: str) -> str:
    """Backtick-quote a single identifier component, escaping embedded backticks."""
    return "`" + ident.replace("`", "``") + "`"


def _quote_qualified(catalog: Optional[str], schema: str, table: Optional[str] = None) -> str:
    """Build a backtick-quoted qualified identifier."""
    parts = []
    if catalog:
        parts.append(_quote(catalog))
    parts.append(_quote(schema))
    if table is not None:
        parts.append(_quote(table))
    return ".".join(parts)


def _existing_table_location(spark: Any, qualified_name: str) -> Optional[str]:
    """Return the LOCATION of an existing table, or ``None`` if not present.

    Falls back to ``DESCRIBE TABLE EXTENDED`` because ``DESCRIBE DETAIL``
    requires the table to already be Delta and the catalog API surface
    differs across Spark / catalog implementations.

    On *any* failure of the DESCRIBE call we return ``None`` (i.e. treat
    as "table absent" and let the caller proceed to ``CREATE TABLE
    IF NOT EXISTS``). The most common failure is the genuinely-absent
    case — Spark raises an ``AnalysisException`` (``TABLE_OR_VIEW_NOT_FOUND``)
    — but transient errors (catalog connectivity, permission denied,
    unparseable ``qualified_name``) also land here. To keep the caller's
    diagnostics meaningful we log the underlying exception at DEBUG so
    operators running with ``--verbose`` have a breadcrumb when a
    subsequent ``CREATE TABLE`` fails with a misleading error.
    """
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qualified_name}").collect()
    except Exception as exc:
        # Don't swallow silently — log at DEBUG so the original error
        # surfaces under ``--verbose`` if the subsequent CREATE TABLE
        # fails and operators need to trace the root cause.
        logger.debug(
            "[oss_shim] DESCRIBE TABLE EXTENDED %s failed (%s: %s); "
            "treating as table-absent",
            qualified_name, type(exc).__name__, exc,
        )
        return None
    for r in rows:
        try:
            col = (r["col_name"] or "").strip().lower()
        except Exception:
            continue
        if col == "location":
            try:
                return r["data_type"]
            except Exception:
                return None
    return None


def _normalize_path(path: str) -> str:
    """Normalize a path to a canonical form for comparison.

    Reconciles the two shapes the same location takes on either side of
    the comparison in :func:`ensure_external_delta_table`:

    - ``DESCRIBE TABLE EXTENDED`` reports ``LOCATION`` as a URI, e.g.
      ``file:/abs/path`` or ``file:///abs/path``;
    - the onboarding spec carries a bare path, e.g. ``/abs/path``.

    Without reconciliation ``file:/abs/path != /abs/path`` and every run
    after the first reports a spurious location mismatch (a warning, or a
    hard ``RuntimeError`` under ``SDP_META_OSS_REGISTER_STRICT=1``). This
    collapses the ``file:`` scheme (and a bare local path) to the path
    component, lowercases remote schemes / hosts, and strips trailing
    slashes. Does not resolve symlinks.
    """
    parsed = urlparse(path.strip())
    scheme = parsed.scheme.lower()
    if scheme in ("", "file"):
        # ``/x``, ``file:/x`` and ``file:///x`` all canonicalise to the
        # local path component.
        local = (parsed.path or path.strip()).rstrip("/")
        return local or "/"
    # Remote schemes (s3, abfss, gcs, hdfs, ...): canonicalise
    # scheme + host, keep the path verbatim modulo trailing slashes.
    return f"{scheme}://{parsed.netloc.lower()}{parsed.path}".rstrip("/")


def ensure_external_delta_table(
    spark: Any,
    qualified_name: str,
    path: Optional[str],
    schema: Optional[StructType] = None,
) -> bool:
    """Ensure an external Delta table at ``path`` is registered as ``qualified_name``.

    OSS ``pyspark.pipelines.table`` does not accept a ``path`` kwarg, so
    SDP-META cannot let ``dp.table(name=..., path=...)`` materialise the
    location for it (the way it does on Lakeflow). Instead, the OSS code
    path side-channels the per-table path from the onboarding spec
    (``bronze_table_path_dev`` / ``silver_table_path_dev``) by
    pre-creating an external Delta table at that location *before* the
    SDP graph builder registers the same name. The subsequent
    ``dp.table(name=qualified_name, ...)`` call resolves to the existing
    external Delta table and writes into the configured path.

    Idempotent. No-ops on Lakeflow (where ``path`` flows through to
    ``dp.table`` directly), or when ``path`` is empty / None. By default,
    logs a one-shot warning when the requested path disagrees with an
    already-registered location for the same name (Spark will continue
    writing to the registered location; user must drop the table or
    align the spec). Set ``SDP_META_OSS_REGISTER_STRICT=1`` to raise
    ``RuntimeError`` on mismatch instead of warning — recommended in CI
    so onboarding-spec drift fails loudly.

    When ``schema`` (a ``StructType``) is supplied, its columns are written
    into the ``CREATE TABLE`` DDL so the create succeeds even at an empty
    location. A schema-less ``CREATE TABLE ... USING DELTA LOCATION`` at a
    location that has no Delta log yet raises ``DELTA_FAILED_INFER_SCHEMA``;
    when no schema is available and the create fails for that reason, the
    helper degrades to a one-shot warning rather than aborting the run — the
    pipeline then creates the table itself, but the configured ``path`` is
    not bound on that first run.

    Returns ``True`` when the helper performed a ``CREATE TABLE`` on
    behalf of the caller, ``False`` otherwise. Useful for tests / logs
    that want to count side-effects.
    """
    if is_databricks():
        return False
    if not path:
        return False

    # NB: do not bind to ``schema`` here — that name holds the optional
    # ``StructType`` parameter whose columns are inlined into the CREATE
    # TABLE DDL below. Shadowing it would silently drop the schema and
    # re-break the empty-location create (DELTA_FAILED_INFER_SCHEMA).
    catalog, schema_name, table = _split_qualified_name(qualified_name)
    qualified = _quote_qualified(catalog, schema_name, table)
    schema_ident = _quote_qualified(catalog, schema_name)

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_ident}")

    existing_loc = _existing_table_location(spark, qualified)
    if existing_loc is not None:
        if _normalize_path(existing_loc) != _normalize_path(path):
            mismatch_msg = (
                f"{qualified_name} is already registered at LOCATION="
                f"{existing_loc}; onboarding spec requested {path}. "
                f"Drop the table (DROP TABLE {qualified_name}) or align "
                "the onboarding spec to resolve."
            )
            if _strict_external_register():
                raise RuntimeError(
                    f"[oss_shim] {mismatch_msg} Strict-register mode "
                    f"({_REGISTER_STRICT_ENV_VAR}) is on; refusing to "
                    "proceed with mismatched location."
                )
            logger.warning(
                "[oss_shim] %s Spark will continue writing to the "
                "registered location. Set %s=1 to fail-fast on mismatch.",
                mismatch_msg, _REGISTER_STRICT_ENV_VAR,
            )
        return False

    escaped_path = path.replace("'", "''")
    # Build the column list explicitly rather than via ``StructType.toDDL``
    # (only present on Spark 3.4+) so identifiers are backtick-quoted and
    # the DDL is portable across the Spark versions SDP-META runs under.
    fields = getattr(schema, "fields", None) if schema is not None else None
    columns_ddl = (
        " (" + ", ".join(f"{_quote(f.name)} {f.dataType.simpleString()}" for f in fields) + ")"
        if fields
        else ""
    )
    try:
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {qualified}{columns_ddl} "
            f"USING DELTA LOCATION '{escaped_path}'"
        )
    except Exception as exc:
        # A schema-less create at a location with no Delta log raises
        # DELTA_FAILED_INFER_SCHEMA. Don't abort the run over a path we
        # can't pre-bind — warn and let the pipeline create the table.
        logger.warning(
            "[oss_shim] could not pre-register external Delta table %s at "
            "LOCATION=%s (%s). The pipeline will create the table; the "
            "configured path is not bound on this run. Supply a schema in "
            "the onboarding spec, or pre-create the Delta table at this "
            "location, to bind the path.",
            qualified_name, path, exc,
        )
        return False
    logger.info(
        "[oss_shim] registered external Delta table %s at LOCATION=%s",
        qualified_name, path,
    )
    return True


__all__ = [
    "RUNTIME",
    "is_databricks",
    "is_oss",
    "refresh_runtime",
    "filter_table_kwargs",
    "reset_kwarg_warning_state",
    "wrap_dqe",
    "cdc_not_supported_error",
    "ensure_external_delta_table",
]
