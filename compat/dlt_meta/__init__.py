"""DLT-META Compatibility Package.

DEPRECATED: This package is a compatibility wrapper for databricks-labs-sdp-meta.
Please migrate to using databricks.labs.sdp_meta directly.

All imports from this package are re-exported from databricks.labs.sdp_meta with deprecation warnings.
"""
import warnings

# Issue deprecation warning on import
warnings.warn(
    "The 'dlt_meta' package is deprecated and will be removed in a future version. "
    "Please migrate to 'databricks.labs.sdp_meta' (pip install databricks-labs-sdp-meta). "
    "See https://databrickslabs.github.io/sdp-meta/ for migration guide.",
    DeprecationWarning,
    stacklevel=2
)

from databricks.labs.sdp_meta import *  # noqa: F401, F403, E402
from databricks.labs.sdp_meta.cli import (  # noqa: F401, E402
    SDPMeta as DLTMeta,
    OnboardCommand,
    DeployCommand,
    SDP_META_RUNNER_NOTEBOOK as DLT_META_RUNNER_NOTEBOOK,
    onboard,
    deploy,
    main,
)
from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: F401, E402
from databricks.labs.sdp_meta.config import WorkspaceConfig  # noqa: F401, E402


def _optional_runtime_import_error(exc):
    message = str(exc)
    return "pyspark" in message or "pipelines" in message or "delta" in message


def _make_stub(name, error):
    def _raise(*_a, **_kw):
        raise ImportError(
            f"{name} requires pyspark>=4.1.0 (for pyspark.pipelines), "
            f"or use it only inside Databricks runtime. "
            f"Original error: {error}"
        )
    return _raise


def _stub_missing(names, error):
    for name in names:
        globals()[name] = _make_stub(name, error)


try:
    from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec  # noqa: F401
except ImportError as _exc:
    if not _optional_runtime_import_error(_exc):
        raise
    _stub_missing(("BronzeDataflowSpec", "SilverDataflowSpec"), str(_exc))

try:
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec  # noqa: F401
except ImportError as _exc:
    if not _optional_runtime_import_error(_exc):
        raise
    _stub_missing(("OnboardDataflowspec",), str(_exc))

try:
    from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders  # noqa: F401
except ImportError as _exc:
    if not _optional_runtime_import_error(_exc):
        raise
    _stub_missing(("PipelineReaders",), str(_exc))

try:
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: F401
    from databricks.labs.sdp_meta.pipeline_writers import AppendFlowWriter, DLTSinkWriter  # noqa: F401
except ImportError as _exc:
    if not _optional_runtime_import_error(_exc):
        raise
    _stub_missing(("DataflowPipeline", "AppendFlowWriter", "DLTSinkWriter"), str(_exc))


def _deprecated_wrapper(func, old_name, new_name):
    """Wrapper that adds deprecation warning to functions."""
    def wrapper(*args, **kwargs):
        warnings.warn(
            f"'{old_name}' is deprecated, use '{new_name}' instead.",
            DeprecationWarning,
            stacklevel=2
        )
        return func(*args, **kwargs)
    return wrapper
