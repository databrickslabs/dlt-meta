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

# Re-export everything from databricks.labs.sdp_meta
# This maintains full backwards compatibility
try:
    from databricks.labs.sdp_meta import *  # noqa: F401, F403
    from databricks.labs.sdp_meta.cli import (  # noqa: F401
        SDPMeta as DLTMeta,  # Alias for backwards compatibility
        OnboardCommand,
        DeployCommand,
        SDP_META_RUNNER_NOTEBOOK as DLT_META_RUNNER_NOTEBOOK,
        onboard,
        deploy,
        main,
    )
    from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline  # noqa: F401
    from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec  # noqa: F401
    from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec  # noqa: F401
    from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders  # noqa: F401
    from databricks.labs.sdp_meta.pipeline_writers import AppendFlowWriter, DLTSinkWriter  # noqa: F401
    from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: F401
    from databricks.labs.sdp_meta.config import WorkspaceConfig  # noqa: F401
except ImportError:
    # If databricks.labs.sdp_meta module not available, this is being run standalone
    pass


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
