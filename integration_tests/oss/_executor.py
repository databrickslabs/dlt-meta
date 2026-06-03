"""Replay SDP-META's recorded ``dp.table`` calls against real Spark + Delta.

On real OSS Apache Spark 4.1+, ``pyspark.pipelines.table`` registers a
query function with the planner; the planner later invokes the query
function, writes its output to the table's catalog-registered location,
and commits. SDP-META's ``OSSDataflowPipeline._register_table_with_dqe``
also pre-registers an external Delta table at the per-table path from
the onboarding spec (the side-channel for the ``path`` kwarg OSS
``dp.table`` rejects), so the planner's write lands at the configured
location.

This module reproduces that planner behavior on plain Spark 3.5+ so the
integration suite can validate row counts in the resulting Delta tables
without requiring Spark 4.1+ on every dev machine. The executor walks
the recorded ``(api, args, kwargs)`` tuples in registration order, calls
the query function for each ``dp.table`` registration, and writes the
output via ``writeStream(availableNow=True)`` (streaming sources, which
is what SDP-META's bronze readers return) or ``write.saveAsTable``
(batch sources). The target table name is the one SDP-META passed to
``dp.table(name=...)`` — same name the side-channelled external Delta
table is already registered under, so writes land at the configured
path.

The exact code path under test (``OSSDataflowPipeline`` → DQE inlining
via ``wrap_dqe`` → kwarg filtering via ``filter_table_kwargs`` →
``ensure_external_delta_table``) is unchanged from what runs on real
OSS Spark. Only the execution step is local-Spark-shaped instead of
``spark-pipelines run``-shaped. That keeps the tests fast and portable
while still exercising the OSS subclass end-to-end.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def _location_to_local_path(location: str) -> str:
    """Convert a Delta LOCATION URI to a local filesystem path.

    Spark's ``DESCRIBE TABLE EXTENDED`` returns LOCATION as a URI
    (``file:/a/b``, ``file:///a/b``) or a bare path (``/a/b``) depending
    on Spark version. ``urlparse`` handles every form correctly,
    returning the path component for ``file:`` URIs and the original
    string when there's no scheme. Returns the empty string for
    non-``file:`` URIs (s3://, abfss://, ...) — those aren't reachable
    from an integration-test JVM, so the caller should treat empty as
    "not a local cleanup target".
    """
    parsed = urlparse(location)
    if parsed.scheme in ("", "file"):
        return parsed.path or location
    return ""


@dataclass
class _ExecutedTable:
    """Result row for one ``dp.table(qf, name=...)`` replay."""

    name: str
    row_count: int
    streaming: bool


@dataclass
class FakeOSSPipelineExecutorResult:
    """Aggregate result of an executor run.

    Tests assert on ``executed_by_name`` for per-table row counts and
    on ``failures`` for any qf that raised during replay (we surface
    the table name + exception verbatim so an integration failure
    points at the offending bronze/silver registration, not at the
    executor).
    """

    executed: list[_ExecutedTable] = field(default_factory=list)
    failures: list[tuple[str, Exception]] = field(default_factory=list)

    @property
    def executed_by_name(self) -> dict[str, _ExecutedTable]:
        return {e.name: e for e in self.executed}

    @property
    def row_counts(self) -> dict[str, int]:
        return {e.name: e.row_count for e in self.executed}


class FakeOSSPipelineExecutor:
    """Replays recorded ``dp.table`` registrations against real Spark + Delta.

    Usage::

        executor = FakeOSSPipelineExecutor(spark, recorder, checkpoint_dir)
        recorder.clear()
        DataflowPipeline.invoke_pipeline(spark, "bronze")
        bronze_result = executor.execute()
        recorder.clear()
        DataflowPipeline.invoke_pipeline(spark, "silver")
        silver_result = executor.execute()

    The recorder is cleared between layers because both bronze and
    silver register against the same global recorder (SDP-META's
    ``dp`` binding is module-level). Clearing keeps each layer's
    executor result focused on the layer's tables; without it,
    ``executor.execute()`` after silver would re-run every bronze qf
    too, doubling the bronze row counts.
    """

    def __init__(self, spark: Any, recorder: Any, checkpoint_dir: Any) -> None:
        self.spark = spark
        self.recorder = recorder
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def execute(self) -> FakeOSSPipelineExecutorResult:
        """Replay recorded registrations in two passes: views, then tables.

        SDP-META wires the bronze / silver flow as:

          1. ``dp.temporary_view(read_bronze, name="bronze_<table>_bronze_inputview")``
             — registers the source read as a named temp view.
          2. ``dp.table(qf, name="bronze.<table>", ...)`` — defines the
             target table; ``qf`` reads from the temp view by name.

        On real OSS Spark ``pyspark.pipelines`` materialises (1) before
        (2). We do the same here: first pass executes every
        ``temporary_view`` qf and creates a session-local temp view of
        the same name; second pass executes every ``table`` qf and
        writes to the side-channelled Delta location.

        Per-table failures are captured (not re-raised) so a single
        bronze table's qf blowing up doesn't mask whether the rest of
        the layer registered correctly — the test gets the full
        picture in one assertion.
        """
        result = FakeOSSPipelineExecutorResult()

        # Pass 1: temporary_view — materialise every named input view
        # so the subsequent ``table`` qfs can reference them via
        # ``spark.read.table(<view_name>)``.
        for api, args, kwargs in list(self.recorder.calls):
            if api != "temporary_view":
                continue
            if not args or not callable(args[0]):
                continue
            name = kwargs.get("name")
            if not name:
                continue
            try:
                self._materialise_view(name, args[0])
            except Exception as exc:  # noqa: BLE001
                result.failures.append((f"view:{name}", exc))

        # Pass 2: table — execute qfs that reference the views above
        # and write to the side-channelled Delta tables.
        for api, args, kwargs in list(self.recorder.calls):
            if api != "table":
                continue
            if not args or not callable(args[0]):
                continue
            name = kwargs.get("name")
            if not name:
                continue
            try:
                self._execute_one(name, args[0], result)
            except Exception as exc:  # noqa: BLE001 — surface verbatim
                result.failures.append((name, exc))
        return result

    def _materialise_view(self, view_name: str, qf: Any) -> None:
        """Register the qf output as a session-local view named ``view_name``.

        SDP-META's bronze ``read_bronze`` returns a *streaming*
        DataFrame (Spark file-stream source). ``createOrReplaceTempView``
        works for streaming DataFrames too — the resulting view is a
        streaming source that downstream readers can pick up via
        ``spark.readStream.table(view_name)`` or, equivalently in this
        executor, by having the downstream qf reference it through the
        same ``dp.read`` /  Spark catalog name resolution.
        """
        df = qf()
        df.createOrReplaceTempView(view_name)

    def _execute_one(
        self,
        name: str,
        qf: Any,
        result: FakeOSSPipelineExecutorResult,
    ) -> None:
        df = qf()
        # SDP-META's ``_register_table_with_dqe`` pre-creates the
        # target table via ``ensure_external_delta_table`` so the
        # write lands at the configured per-table path. For silver
        # specs (no ``schema_json`` on the pipeline instance) that
        # pre-create lands as a schema-less Delta table — and the
        # subsequent write fails with a Delta "schema mismatch" error.
        # Real OSS ``pyspark.pipelines`` doesn't have this gap because
        # its planner has the qf's output schema at planning time and
        # creates the table accordingly. Reproduce that here by
        # re-aligning the pre-created table's schema to the qf's
        # before writing.
        self._align_external_table_schema(name, df.schema)
        if df.isStreaming:
            chk = self.checkpoint_dir / name.replace(".", "_") / "_chk"
            chk.mkdir(parents=True, exist_ok=True)
            # ``trigger(availableNow=True)`` processes all currently-
            # available source data in one batch and stops — exactly
            # the integration-test "process source then validate
            # counts" semantic. ``toTable(name)`` resolves to the
            # external Delta table at the side-channelled location.
            query = (
                df.writeStream
                .format("delta")
                .outputMode("append")
                .trigger(availableNow=True)
                .option("checkpointLocation", str(chk))
                .toTable(name)
            )
            query.awaitTermination()
            streaming = True
        else:
            df.write.format("delta").mode("append").saveAsTable(name)
            streaming = False
        # SELECT COUNT(*) after the write so the integration assertion
        # sees the persisted row count, not the source row count
        # (which would mask a silently-empty write).
        row_count = self.spark.read.format("delta").table(name).count()
        result.executed.append(
            _ExecutedTable(name=name, row_count=row_count, streaming=streaming)
        )

    def _align_external_table_schema(self, name: str, target_schema: Any) -> None:
        """Re-create the pre-bound external table with ``target_schema``.

        Reads the current LOCATION via ``DESCRIBE TABLE EXTENDED``,
        clears the on-disk Delta log at that location (so the next
        ``CREATE TABLE`` is at a truly empty path and doesn't surface
        ``DELTA_FAILED_INFER_SCHEMA``), drops the catalog registration,
        then re-creates an external Delta table at the same path with
        the columns of ``target_schema``. The end state is
        catalog-name → same path → schema-matching-the-qf, which is
        exactly what real OSS ``pyspark.pipelines`` plans before its
        first write.

        No-op if the catalog doesn't know the table (it'll be created
        by the subsequent write at the default warehouse location —
        the path-binding contract requires SDP-META's pre-create to
        have run, so the no-op only triggers if SDP-META didn't
        register the side-channelled location, which is itself a bug
        the row-count assertion will catch).
        """
        try:
            rows = self.spark.sql(f"DESCRIBE TABLE EXTENDED {name}").collect()
        except Exception:
            return
        location: str | None = None
        for r in rows:
            try:
                col = (r["col_name"] or "").strip().lower()
            except Exception:
                continue
            if col == "location":
                location = r["data_type"]
                break
        if not location:
            return
        # Drop + clear the Delta log so the next CREATE TABLE binds
        # the same path under a fresh schema.
        self.spark.sql(f"DROP TABLE IF EXISTS {name}")
        local_path = _location_to_local_path(location)
        if local_path:
            try:
                from shutil import rmtree

                rmtree(local_path, ignore_errors=True)
            except Exception:
                # Remote URIs (s3://, abfss://, ...) aren't reachable
                # from an integration test — but those paths are also
                # fresh per run via the per-test workdir, so re-CREATE
                # works without clearing the prior data.
                pass
        # Build a column list from the qf's schema so the create
        # succeeds without DELTA_FAILED_INFER_SCHEMA.
        cols = ", ".join(
            f"`{f.name}` {f.dataType.simpleString()}" for f in target_schema.fields
        )
        self.spark.sql(
            f"CREATE TABLE {name} ({cols}) USING DELTA LOCATION '{location}'"
        )
