"""DP recorder + sys.modules install — shared by pytest conftest AND runner.

Background: SDP-META's OSS code path imports ``from pyspark import
pipelines as dp`` at module top. Once that binding is taken, the
recorder MUST already be installed into ``sys.modules["pyspark.pipelines"]``
or the binding will resolve to the real Spark 4.1+ module (when present)
or fail to import (when not).

This module exposes the recorder install as a single import-time side
effect so BOTH consumers — the pytest conftest under
``integration_tests/oss/conftest.py`` AND the standalone runner under
``integration_tests/run_oss_integration_tests.py`` — get the same
recorder singleton without one having to import the other.

Importing this module guarantees:
  - ``SDP_META_RUNTIME=oss`` is set (unless caller already pinned it).
  - ``sys.modules["pyspark.pipelines"]`` resolves to a ``_DPRecorder``
    instance that records every ``dp.table`` / ``dp.temporary_view`` /
    ``dp.create_streaming_table`` / ``dp.create_sink`` call SDP-META
    makes on the OSS code path.
  - The recorder singleton is exposed as :data:`DP_RECORDER` for tests
    and runners that need to inspect or clear the call log.

Critical ordering rule: this module must be imported BEFORE any
``databricks.labs.sdp_meta`` import. The pytest conftest enforces this
by importing this module at the top of ``conftest.py`` (which pytest
loads before any test module). The runner enforces it by importing
this module before any sdp-meta import.
"""
from __future__ import annotations

import os
import sys
from typing import Any, Callable


os.environ.setdefault("SDP_META_RUNTIME", "oss")


class _DPRecorder:
    """Records every ``pyspark.pipelines`` call SDP-META makes on the OSS path.

    Stands in for ``pyspark.pipelines`` (installed into
    ``sys.modules["pyspark.pipelines"]`` so SDP-META's
    ``from pyspark import pipelines as dp`` resolves to this object).
    Every ``dp.table`` / ``dp.create_streaming_table`` / etc. call
    appends a ``(api_name, args, kwargs)`` tuple to ``self.calls``.

    Critically, the Lakeflow-only extension symbols (``expect_all``,
    ``create_auto_cdc_flow``, ``create_auto_cdc_from_snapshot_flow``)
    are NOT defined here — so the runtime probe in
    ``oss_pipelines._probe_runtime`` (which checks
    ``hasattr(dp, "create_auto_cdc_flow")``) lands on OSS even if a
    test forgets the env override.

    The ``dp.table(qf, name=..., ...)`` shape SDP-META actually uses
    (qf as first positional arg) is detected by the factory and
    additionally records a ``"table.applied"`` entry pointing at the
    decorated function's name.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def clear(self) -> None:
        self.calls.clear()

    def call_names(self) -> list[str]:
        return [api for api, _args, _kwargs in self.calls]

    def kwargs_for(self, api: str) -> list[dict]:
        return [kwargs for name, _args, kwargs in self.calls if name == api]

    def _factory(self, api: str) -> Callable[..., Any]:
        recorder = self

        def factory(*args: Any, **kwargs: Any):
            recorder.calls.append((api, args, kwargs))

            def decorator(fn: Callable[..., Any]):
                recorder.calls.append((
                    f"{api}.applied",
                    (getattr(fn, "__name__", repr(fn)),),
                    {},
                ))
                return fn

            if args and callable(args[0]):
                fn = args[0]
                recorder.calls.append((
                    f"{api}.applied",
                    (getattr(fn, "__name__", repr(fn)),),
                    {},
                ))
                return fn
            return decorator

        return factory

    @property
    def table(self):
        return self._factory("table")

    @property
    def temporary_view(self):
        return self._factory("temporary_view")

    @property
    def append_flow(self):
        return self._factory("append_flow")

    @property
    def materialized_view(self):
        return self._factory("materialized_view")

    def create_streaming_table(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("create_streaming_table", args, kwargs))

    def create_sink(self, *args: Any, **kwargs: Any) -> None:
        self.calls.append(("create_sink", args, kwargs))


DP_RECORDER = _DPRecorder()
sys.modules["pyspark.pipelines"] = DP_RECORDER  # type: ignore[assignment]
