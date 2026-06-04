"""Tests for the dlt-meta backward compatibility layer.

These tests verify that the old dlt_meta package (compat/) properly
re-exports symbols from databricks.labs.sdp_meta with deprecation warnings.
"""
import os
import subprocess
import sys
import unittest
import warnings
from unittest.mock import MagicMock

# Mock the pyspark.pipelines module (and parent + sibling submodules used by
# the runtime classes) before importing them. `from pyspark import pipelines`
# requires pyspark itself in sys.modules as the parent package.
sys.modules['pyspark'] = MagicMock()
sys.modules['pyspark.pipelines'] = MagicMock()
sys.modules['pyspark.sql'] = MagicMock()
sys.modules['pyspark.sql.functions'] = MagicMock()
sys.modules['pyspark.sql.types'] = MagicMock()
sys.modules['pyspark.sql.session'] = MagicMock()
sys.modules['pyspark.sql.window'] = MagicMock()
sys.modules['delta'] = MagicMock()
sys.modules['delta.tables'] = MagicMock()

# Ensure the compat directory is on the Python path so `import dlt_meta` works
_compat_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "compat")
if _compat_dir not in sys.path:
    sys.path.insert(0, _compat_dir)


class TestCompatDeprecationWarning(unittest.TestCase):
    """Test that importing dlt_meta emits a DeprecationWarning."""

    def test_import_dlt_meta_emits_deprecation_warning(self):
        """Importing dlt_meta should emit a DeprecationWarning."""
        # Remove from cache to trigger fresh import
        modules_to_remove = [k for k in sys.modules if k.startswith('dlt_meta')]
        for mod in modules_to_remove:
            del sys.modules[mod]

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import dlt_meta  # noqa: F401
            deprecation_warnings = [
                x for x in w if issubclass(x.category, DeprecationWarning)
            ]
            self.assertTrue(
                len(deprecation_warnings) > 0,
                "Importing dlt_meta should emit at least one DeprecationWarning"
            )


class TestCompatReExports(unittest.TestCase):
    """Test that old dlt_meta imports correctly re-export from sdp_meta."""

    def test_dltmeta_alias_is_sdp_meta(self):
        """DLTMeta class should be an alias for SDPMeta."""
        from dlt_meta import DLTMeta
        from databricks.labs.sdp_meta.cli import SDPMeta
        self.assertIs(DLTMeta, SDPMeta)

    def test_dataflow_pipeline_reexport(self):
        """DataflowPipeline should be re-exported from dlt_meta."""
        from dlt_meta import DataflowPipeline
        from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline as Original
        self.assertIs(DataflowPipeline, Original)

    def test_dataflow_spec_reexport(self):
        """BronzeDataflowSpec and SilverDataflowSpec should be re-exported."""
        from dlt_meta import BronzeDataflowSpec, SilverDataflowSpec
        from databricks.labs.sdp_meta.dataflow_spec import (
            BronzeDataflowSpec as OrigBronze,
            SilverDataflowSpec as OrigSilver,
        )
        self.assertIs(BronzeDataflowSpec, OrigBronze)
        self.assertIs(SilverDataflowSpec, OrigSilver)

    def test_onboard_command_reexport(self):
        """OnboardCommand should be re-exported from dlt_meta."""
        from dlt_meta import OnboardCommand
        from databricks.labs.sdp_meta.cli import OnboardCommand as Original
        self.assertIs(OnboardCommand, Original)

    def test_deploy_command_reexport(self):
        """DeployCommand should be re-exported from dlt_meta."""
        from dlt_meta import DeployCommand
        from databricks.labs.sdp_meta.cli import DeployCommand as Original
        self.assertIs(DeployCommand, Original)

    def test_runner_notebook_alias(self):
        """DLT_META_RUNNER_NOTEBOOK should alias SDP_META_RUNNER_NOTEBOOK."""
        from dlt_meta import DLT_META_RUNNER_NOTEBOOK
        from databricks.labs.sdp_meta.cli import SDP_META_RUNNER_NOTEBOOK
        self.assertEqual(DLT_META_RUNNER_NOTEBOOK, SDP_META_RUNNER_NOTEBOOK)

    def test_pipeline_readers_reexport(self):
        """PipelineReaders should be re-exported from dlt_meta."""
        from dlt_meta import PipelineReaders
        from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders as Original
        self.assertIs(PipelineReaders, Original)

    def test_pipeline_writers_reexport(self):
        """AppendFlowWriter and DLTSinkWriter should be re-exported from dlt_meta."""
        from dlt_meta import AppendFlowWriter, DLTSinkWriter
        from databricks.labs.sdp_meta.pipeline_writers import (
            AppendFlowWriter as OrigAppend,
            DLTSinkWriter as OrigSink,
        )
        self.assertIs(AppendFlowWriter, OrigAppend)
        self.assertIs(DLTSinkWriter, OrigSink)

    def test_pyspark_pipeline_stubs_do_not_hide_non_pipeline_exports(self):
        """Missing pyspark.pipelines should only stub symbols that require it."""
        repo_root = os.path.dirname(os.path.dirname(__file__))
        script = r"""
import importlib.abc
import os
import sys
import warnings

repo_root = os.environ["REPO_ROOT"]
sys.path.insert(0, os.path.join(repo_root, "src"))
sys.path.insert(0, os.path.join(repo_root, "compat"))

class BlockPipelines(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "pyspark.pipelines":
            raise ImportError("cannot import name 'pipelines' from 'pyspark'")
        return None

sys.meta_path.insert(0, BlockPipelines())
sys.modules.pop("pyspark.pipelines", None)
import pyspark
if hasattr(pyspark, "pipelines"):
    delattr(pyspark, "pipelines")

with warnings.catch_warnings():
    warnings.simplefilter("ignore", DeprecationWarning)
    import dlt_meta

from databricks.labs.sdp_meta.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec
from databricks.labs.sdp_meta.onboard_dataflowspec import OnboardDataflowspec
from databricks.labs.sdp_meta.pipeline_readers import PipelineReaders

assert dlt_meta.BronzeDataflowSpec is BronzeDataflowSpec
assert dlt_meta.SilverDataflowSpec is SilverDataflowSpec
assert dlt_meta.OnboardDataflowspec is OnboardDataflowspec
assert dlt_meta.PipelineReaders is PipelineReaders

try:
    dlt_meta.DataflowPipeline()
except ImportError as exc:
    assert "pyspark>=4.1.0" in str(exc), str(exc)
else:
    raise AssertionError("DataflowPipeline should be stubbed when pyspark.pipelines is missing")
"""
        env = os.environ.copy()
        env["REPO_ROOT"] = repo_root
        result = subprocess.run(
            [sys.executable, "-c", script],
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)


if __name__ == '__main__':
    unittest.main()
