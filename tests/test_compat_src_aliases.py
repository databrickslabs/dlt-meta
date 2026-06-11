"""Tests for the v0.0.10 ``src.*`` import compatibility shim.

The shim's job: a v0.0.10 customer notebook with ``from src.X import Y``
lines must keep working on v0.0.11 (with a ``DeprecationWarning``)
instead of failing with ``ModuleNotFoundError: No module named 'src'``.

This file covers four independent surfaces:

1. **Module-level alias resolution** — every v0.0.10 ``src.<sub>`` module
   resolves to the canonical ``databricks.labs.sdp_meta.<sub>``.
2. **Symbol-level access** — ``DLTMeta`` (renamed to ``SDPMeta`` in
   v0.0.11), ``DLT_META_RUNNER_NOTEBOOK`` (renamed to
   ``SDP_META_RUNNER_NOTEBOOK``), and the unchanged classes
   (``DataflowPipeline``, ``BronzeDataflowSpec``, ``DLTSink``…) all
   resolve through the alias.
3. **Deprecation warning behaviour** — fires once per alias per process
   on first attribute access, not on the eager registration walk, and
   not on subsequent accesses.
4. **Optional-runtime stub** — when ``pyspark.pipelines`` is missing,
   accessing a stub'd module raises a clear ``Lakeflow SDP runtime``
   error rather than ``ModuleNotFoundError: src`` or the historical
   silent ``cannot import name`` failure mode.

A separate fresh-interpreter ``subprocess`` test
(``test_pth_loads_dlt_meta_at_startup``) exercises the ``.pth``
auto-load path — the actual customer experience after
``pip install dlt-meta``.
"""
import os
import subprocess
import sys
import textwrap
import unittest
import warnings
from unittest.mock import MagicMock

# Mock pyspark.pipelines BEFORE importing the shim or anything from
# databricks.labs.sdp_meta — the test runner doesn't ship a Spark
# version with pyspark.pipelines yet. (Mirrors tests/test_compat.py.)
sys.modules.setdefault("pyspark.pipelines", MagicMock())

# Make the compat package importable without installing it as a wheel.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_COMPAT_DIR = os.path.join(_REPO_ROOT, "compat")
if _COMPAT_DIR not in sys.path:
    sys.path.insert(0, _COMPAT_DIR)


def _purge_dlt_meta_and_src() -> None:
    """Remove cached compat modules so the next ``import`` is fresh.

    Each test that exercises the registration path needs a clean
    ``sys.modules`` so module-level state (``_WARNED_ALIASES``, the
    ``src`` alias entries) is rebuilt rather than reused from a prior
    test's import.
    """
    for key in list(sys.modules):
        if key == "dlt_meta" or key.startswith("dlt_meta."):
            del sys.modules[key]
        elif key == "src" or key.startswith("src."):
            del sys.modules[key]


class TestSubmoduleAliasMapMatchesV0_0_10(unittest.TestCase):
    """The hand-pinned ``_SRC_SUBMODULES`` must match the v0.0.10 publication."""

    # Verbatim from ``git ls-tree v0.0.10 -- 'src/*.py' --name-only``
    # minus ``__init__.py`` (which is the package itself, registered
    # under the ``src`` top-level alias rather than ``src.__init__``).
    EXPECTED_V0_0_10_SUBMODULES = frozenset({
        "__about__",
        "__main__",
        "cli",
        "config",
        "dataflow_pipeline",
        "dataflow_spec",
        "install",
        "metastore_ops",
        "onboard_dataflowspec",
        "pipeline_readers",
        "pipeline_writers",
        "uninstall",
    })

    def test_pinned_alias_set_matches_v0_0_10(self):
        _purge_dlt_meta_and_src()
        import dlt_meta  # noqa: F401  (fresh import to register aliases)

        from dlt_meta import _SRC_SUBMODULES
        self.assertEqual(
            frozenset(_SRC_SUBMODULES),
            self.EXPECTED_V0_0_10_SUBMODULES,
            "_SRC_SUBMODULES drifted from the v0.0.10 src/*.py set. If you "
            "intend the change, also update the v0.0.10 ground-truth list "
            "in this test.",
        )


class TestSrcModuleAliasing(unittest.TestCase):
    """`from src.X import Y` resolves to ``databricks.labs.sdp_meta.X.Y``."""

    def setUp(self):
        _purge_dlt_meta_and_src()
        import dlt_meta  # noqa: F401  (triggers alias registration)

    def test_top_level_src_alias_resolves(self):
        import src
        self.assertIs(sys.modules["src"], sys.modules["dlt_meta"])
        # ``__version__`` is a flat re-export on dlt_meta itself, so
        # accessing it via the ``src`` alias proves the package alias
        # is wired.
        self.assertTrue(hasattr(src, "__version__") or hasattr(src, "DataflowPipeline"))

    def test_dataflow_pipeline_alias_resolves(self):
        from src.dataflow_pipeline import DataflowPipeline
        from databricks.labs.sdp_meta.dataflow_pipeline import (
            DataflowPipeline as Canonical,
        )
        self.assertIs(DataflowPipeline, Canonical)

    def test_dataflow_spec_aliases_resolve(self):
        from src.dataflow_spec import (
            BronzeDataflowSpec,
            SilverDataflowSpec,
            DataflowSpecUtils,
            DLTSink,
        )
        from databricks.labs.sdp_meta.dataflow_spec import (
            BronzeDataflowSpec as B,
            SilverDataflowSpec as S,
            DataflowSpecUtils as U,
            DLTSink as D,
        )
        self.assertIs(BronzeDataflowSpec, B)
        self.assertIs(SilverDataflowSpec, S)
        self.assertIs(DataflowSpecUtils, U)
        self.assertIs(DLTSink, D)

    def test_pipeline_writers_aliases_resolve(self):
        from src.pipeline_writers import AppendFlowWriter, DLTSinkWriter
        from databricks.labs.sdp_meta.pipeline_writers import (
            AppendFlowWriter as A,
            DLTSinkWriter as D,
        )
        self.assertIs(AppendFlowWriter, A)
        self.assertIs(DLTSinkWriter, D)

    def test_metastore_ops_aliases_resolve(self):
        from src.metastore_ops import (
            DeltaPipelinesMetaStoreOps,
            DeltaPipelinesInternalTableOps,
        )
        from databricks.labs.sdp_meta.metastore_ops import (
            DeltaPipelinesMetaStoreOps as M,
            DeltaPipelinesInternalTableOps as I_,
        )
        self.assertIs(DeltaPipelinesMetaStoreOps, M)
        self.assertIs(DeltaPipelinesInternalTableOps, I_)

    def test_onboard_dataflowspec_alias_resolves(self):
        from src.onboard_dataflowspec import OnboardDataflowspec
        from databricks.labs.sdp_meta.onboard_dataflowspec import (
            OnboardDataflowspec as Canonical,
        )
        self.assertIs(OnboardDataflowspec, Canonical)

    def test_pipeline_readers_alias_resolves(self):
        from src.pipeline_readers import PipelineReaders
        from databricks.labs.sdp_meta.pipeline_readers import (
            PipelineReaders as P,
        )
        self.assertIs(PipelineReaders, P)

    def test_install_alias_resolves(self):
        from src.install import WorkspaceInstaller
        from databricks.labs.sdp_meta.install import WorkspaceInstaller as W
        self.assertIs(WorkspaceInstaller, W)

    def test_config_alias_resolves(self):
        from src.config import WorkspaceConfig
        from databricks.labs.sdp_meta.config import WorkspaceConfig as W
        self.assertIs(WorkspaceConfig, W)

    def test_about_alias_resolves(self):
        from src.__about__ import __version__
        from databricks.labs.sdp_meta.__about__ import __version__ as canonical
        self.assertEqual(__version__, canonical)


class TestRenamedSymbolAliasing(unittest.TestCase):
    """v0.0.10 → v0.0.11 symbol renames must resolve via ``src.cli``.

    This is the C1 review point: ``DLTMeta`` was renamed to ``SDPMeta``
    in v0.0.11. Module-level aliasing alone (``src.cli`` →
    ``databricks.labs.sdp_meta.cli``) doesn't fix this — the rebind
    ``DLTMeta = SDPMeta`` in ``cli.py`` does.
    """

    def setUp(self):
        _purge_dlt_meta_and_src()
        import dlt_meta  # noqa: F401

    def test_dltmeta_resolves_via_src_cli_alias(self):
        from src.cli import DLTMeta
        from databricks.labs.sdp_meta.cli import SDPMeta
        self.assertIs(
            DLTMeta, SDPMeta,
            "DLTMeta must be a rebind of SDPMeta in cli.py for the "
            "src.cli alias to work — see C1 in the review.",
        )

    def test_runner_notebook_constant_resolves_via_src_cli_alias(self):
        from src.cli import DLT_META_RUNNER_NOTEBOOK
        from databricks.labs.sdp_meta.cli import SDP_META_RUNNER_NOTEBOOK
        self.assertEqual(DLT_META_RUNNER_NOTEBOOK, SDP_META_RUNNER_NOTEBOOK)

    def test_other_cli_symbols_resolve(self):
        from src.cli import (
            OnboardCommand,
            DeployCommand,
            onboard,
            deploy,
            main,
        )
        from databricks.labs.sdp_meta.cli import (
            OnboardCommand as OC,
            DeployCommand as DC,
            onboard as on,
            deploy as dep,
            main as mn,
        )
        self.assertIs(OnboardCommand, OC)
        self.assertIs(DeployCommand, DC)
        self.assertIs(onboard, on)
        self.assertIs(deploy, dep)
        self.assertIs(main, mn)


class TestDeprecationWarningBehaviour(unittest.TestCase):
    """Warning fires once per alias on first ATTRIBUTE access, not on registration."""

    def setUp(self):
        _purge_dlt_meta_and_src()

    def test_no_warning_on_eager_registration(self):
        """Importing ``dlt_meta`` itself must not fire src.* alias warnings.

        The ``.pth`` runs ``import dlt_meta`` at every interpreter
        startup. If the registration walk eagerly emitted warnings, a
        customer who's already migrated would get N warnings every
        startup for no reason.
        """
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            import dlt_meta  # noqa: F401

        src_alias_warnings = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "is a v0.0.10 compatibility alias" in str(x.message)
        ]
        self.assertEqual(
            src_alias_warnings, [],
            "Eager registration emitted src.* alias warnings; they "
            "should fire on first attribute access only.",
        )

    def test_warning_fires_on_first_attribute_access(self):
        import dlt_meta  # noqa: F401

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            from src.dataflow_pipeline import DataflowPipeline  # noqa: F401

        relevant = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "src.dataflow_pipeline" in str(x.message)
        ]
        self.assertEqual(
            len(relevant), 1,
            f"Expected exactly one DeprecationWarning for src.dataflow_pipeline, "
            f"got {len(relevant)}: {[str(x.message) for x in relevant]}",
        )

    def test_warning_fires_only_once_per_alias(self):
        import dlt_meta  # noqa: F401
        # First access emits one warning.
        from src.dataflow_pipeline import DataflowPipeline  # noqa: F401

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            from src.dataflow_pipeline import DataflowPipeline as _Again  # noqa: F401, F811

        relevant = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "src.dataflow_pipeline" in str(x.message)
        ]
        self.assertEqual(
            relevant, [],
            "DeprecationWarning re-fired on second access; alias dedup "
            "is broken (check _WARNED_ALIASES).",
        )

    def test_warning_message_points_at_canonical_path(self):
        import dlt_meta  # noqa: F401

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            from src.cli import DLTMeta  # noqa: F401

        relevant = [
            x for x in w
            if issubclass(x.category, DeprecationWarning)
            and "src.cli" in str(x.message)
        ]
        self.assertEqual(len(relevant), 1)
        msg = str(relevant[0].message)
        self.assertIn("databricks.labs.sdp_meta.cli", msg)
        self.assertIn("v0.1.0", msg, "Removal version must be in the message")


class TestOptOutEnvVar(unittest.TestCase):
    """``SDP_META_DISABLE_SRC_ALIAS=1`` skips registration and the warning.

    Subprocess-isolated because the env var must be set BEFORE the
    shim is imported, and our other tests have already loaded it.
    """

    def _run_in_clean_subprocess(self, env_overrides, code):
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
            **env_overrides,
        }
        return subprocess.run(
            [sys.executable, "-W", "default::DeprecationWarning", "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_opt_out_env_var_skips_src_aliasing(self):
        code = textwrap.dedent("""
            import sys
            from unittest.mock import MagicMock
            sys.modules['pyspark.pipelines'] = MagicMock()
            import dlt_meta  # noqa: F401

            assert 'src' not in sys.modules, (
                "Opt-out env var must skip the src alias entirely"
            )
            assert 'src.dataflow_pipeline' not in sys.modules, (
                "Opt-out env var must skip every src.<sub> alias"
            )
            print('OK')
        """)
        result = self._run_in_clean_subprocess(
            {"SDP_META_DISABLE_SRC_ALIAS": "1"}, code,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "OK")

    def test_opt_out_env_var_suppresses_package_warning(self):
        code = textwrap.dedent("""
            import sys, warnings
            from unittest.mock import MagicMock
            sys.modules['pyspark.pipelines'] = MagicMock()

            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter('always')
                import dlt_meta  # noqa: F401

            messages = [str(x.message) for x in w
                        if issubclass(x.category, DeprecationWarning)]
            assert all(
                "'dlt_meta' package is deprecated" not in m
                for m in messages
            ), f"Package warning fired under opt-out: {messages}"
            print('OK')
        """)
        result = self._run_in_clean_subprocess(
            {"SDP_META_DISABLE_SRC_ALIAS": "1"}, code,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "OK")


class TestPthFileFreshInterpreter(unittest.TestCase):
    """Fresh-interpreter ``.pth`` exec — the actual customer experience.

    The unit tests above all run after ``import dlt_meta`` has already
    been triggered (either directly or transitively). The customer's
    actual failure mode is a notebook whose first line is
    ``from src.dataflow_pipeline import DataflowPipeline`` with no
    explicit ``import dlt_meta``. The ``.pth`` is what saves them.

    Rather than build a wheel and install it (slow, env-dependent),
    we simulate the ``.pth`` exec by running the same line via
    ``python -c`` AFTER it. This proves the .pth's exec model
    (CPython site.py only runs lines starting with ``import``)
    accepts our particular ``import os; …`` line.
    """

    def test_pth_line_is_well_formed(self):
        """Confirm the .pth line is one-line, starts with ``import``, no syntax error."""
        pth_path = os.path.join(_COMPAT_DIR, "dlt_meta.pth")
        with open(pth_path) as f:
            lines = [ln for ln in f.read().splitlines() if ln.strip()]
        self.assertEqual(
            len(lines), 1,
            f".pth must be exactly one non-empty line; got {len(lines)}",
        )
        self.assertTrue(
            lines[0].startswith("import "),
            "CPython site.py only execs .pth lines starting with 'import '. "
            f"Line was: {lines[0]!r}",
        )
        # The line must compile.
        compile(lines[0], "dlt_meta.pth", "exec")

    def test_pth_line_loads_dlt_meta_when_env_var_unset(self):
        """Executing the .pth line must populate sys.modules['dlt_meta']."""
        pth_path = os.path.join(_COMPAT_DIR, "dlt_meta.pth")
        with open(pth_path) as f:
            pth_line = next(ln for ln in f.read().splitlines() if ln.strip())

        code = textwrap.dedent(f"""
            import sys
            from unittest.mock import MagicMock
            sys.modules['pyspark.pipelines'] = MagicMock()
            # Simulate site.py exec'ing the .pth line at startup.
            {pth_line}
            assert 'dlt_meta' in sys.modules, "dlt_meta not loaded by .pth"
            assert 'src' in sys.modules, "src alias not registered after .pth"
            from src.dataflow_pipeline import DataflowPipeline
            print(DataflowPipeline.__module__)
        """)
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
        }
        env.pop("SDP_META_DISABLE_SRC_ALIAS", None)
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            "databricks.labs.sdp_meta.dataflow_pipeline",
        )

    def test_pth_line_skips_when_env_var_set(self):
        pth_path = os.path.join(_COMPAT_DIR, "dlt_meta.pth")
        with open(pth_path) as f:
            pth_line = next(ln for ln in f.read().splitlines() if ln.strip())

        code = textwrap.dedent(f"""
            import sys
            from unittest.mock import MagicMock
            sys.modules['pyspark.pipelines'] = MagicMock()
            {pth_line}
            assert 'dlt_meta' not in sys.modules, (
                "Opt-out env var ignored by .pth line"
            )
            print('OK')
        """)
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
            "SDP_META_DISABLE_SRC_ALIAS": "1",
        }
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertEqual(result.stdout.strip(), "OK")


class TestOptionalRuntimeStub(unittest.TestCase):
    """When ``pyspark.pipelines`` is unavailable, errors are actionable.

    This was the silent-swallow bug review-point C3: the previous
    shim did ``except ImportError: pass`` and customers saw confusing
    ``cannot import name 'DataflowPipeline'`` messages instead of
    ``Lakeflow SDP runtime not installed``.

    Subprocess-isolated to actually exercise the missing-runtime
    path (the in-process tests have ``pyspark.pipelines`` mocked).
    """

    def test_missing_pyspark_pipelines_raises_actionable_error(self):
        code = textwrap.dedent("""
            import sys

            # Real pyspark (3.5.x in this dev env) is a working Spark
            # install but doesn't ship the Lakeflow SDP submodule, so
            # ``from pyspark import pipelines`` naturally raises
            # ``ImportError: cannot import name 'pipelines' from
            # 'pyspark'`` — exactly the failure mode customers see on
            # legacy DBR runtimes. We do NOT pre-mock pyspark.pipelines
            # here; the absence is the test condition.
            assert 'pyspark.pipelines' not in sys.modules, (
                'Test contract violated: pyspark.pipelines pre-installed'
            )

            # Importing dlt_meta must not raise — it falls back to stubs.
            import dlt_meta  # noqa: F401

            # And the flat-reexport surface raises actionably:
            try:
                from dlt_meta import DataflowPipeline
            except ImportError as exc:
                msg = str(exc)
                assert 'Lakeflow SDP runtime' in msg, (
                    f"Flat re-export error must mention Lakeflow SDP: {msg!r}"
                )
            else:
                raise AssertionError(
                    'Expected ImportError on dlt_meta.DataflowPipeline '
                    'when pyspark.pipelines is unavailable'
                )

            # And the src.* stub surface raises actionably too:
            try:
                from src.dataflow_pipeline import DataflowPipeline
            except ImportError as exc:
                msg = str(exc)
                assert 'Lakeflow SDP runtime' in msg, (
                    f"Stub error must mention Lakeflow SDP: {msg!r}"
                )
                assert 'pyspark.pipelines' in msg, (
                    f"Stub error must mention pyspark.pipelines: {msg!r}"
                )
                print('OK')
            else:
                raise AssertionError(
                    'Expected ImportError on src.dataflow_pipeline access '
                    'when pyspark.pipelines is unavailable'
                )
        """)
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
        }
        env.pop("SDP_META_DISABLE_SRC_ALIAS", None)
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(
            result.returncode, 0,
            msg=f"stderr: {result.stderr}\nstdout: {result.stdout}",
        )
        self.assertEqual(result.stdout.strip(), "OK")


class TestSrcFirstImportPath(unittest.TestCase):
    """The real customer path: ``from src.X import`` with NO prior ``import dlt_meta``.

    Every other test in this file imports ``dlt_meta`` first, which
    claims ``sys.modules['src']`` via ``setdefault`` so ``compat/src``
    never runs. But a v0.0.10 notebook's first line is literally
    ``from src.dataflow_pipeline import …`` -- ``compat/src/__init__``
    is what runs, and it must bootstrap ``dlt_meta`` so the aliases
    (and the actionable missing-runtime stubs) get wired identically
    to the dlt_meta-first path.

    Regression guard for the two consolidated shim bugs:
      * ``compat/src`` used to silently *skip* a submodule whose
        canonical target failed to import, so on a non-SDP runtime the
        access raised ``ModuleNotFoundError: No module named
        'src.dataflow_pipeline'`` instead of the actionable
        "Lakeflow SDP runtime" error.
      * ``compat/src`` carried a shorter submodule list than
        ``dlt_meta`` (it dropped ``uninstall`` / ``__main__`` /
        ``__about__``).

    Subprocess-isolated -- a clean interpreter is the only way to make
    ``compat/src`` (rather than the dlt_meta ``src`` alias) run.
    """

    def _run(self, code, env_overrides=None):
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
        }
        env.pop("SDP_META_DISABLE_SRC_ALIAS", None)
        if env_overrides:
            env.update(env_overrides)
        return subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )

    def test_src_first_missing_runtime_raises_actionable_error(self):
        # No ``import dlt_meta``, no mocked pyspark.pipelines: a legacy
        # notebook's first line on a non-SDP runtime. The fix
        # (compat/src delegates to dlt_meta) makes this raise the
        # actionable error instead of ModuleNotFoundError.
        code = textwrap.dedent("""
            import sys
            assert 'dlt_meta' not in sys.modules, 'dlt_meta pre-imported'
            assert 'pyspark.pipelines' not in sys.modules, (
                'Test contract violated: pyspark.pipelines pre-installed'
            )
            try:
                from src.dataflow_pipeline import DataflowPipeline
            except ImportError as exc:
                msg = str(exc)
                assert 'Lakeflow SDP runtime' in msg, (
                    f'src-first error must mention Lakeflow SDP: {msg!r}'
                )
                assert 'No module named' not in msg, (
                    f'Regressed to ModuleNotFoundError silent-skip: {msg!r}'
                )
                print('OK')
            else:
                raise AssertionError(
                    'Expected ImportError on src.dataflow_pipeline '
                    'when pyspark.pipelines is unavailable'
                )
        """)
        result = self._run(code)
        self.assertEqual(
            result.returncode, 0,
            msg=f"stderr: {result.stderr}\nstdout: {result.stdout}",
        )
        self.assertEqual(result.stdout.strip(), "OK")

    def test_src_first_resolves_full_v0_0_10_surface(self):
        # With the runtime present (mocked), the src-first path resolves
        # to the canonical class, and every v0.0.10 submodule is
        # registered -- including ``uninstall``, which the old
        # compat/src list dropped (the submodule-list drift bug).
        code = textwrap.dedent("""
            import sys
            from unittest.mock import MagicMock
            sys.modules['pyspark.pipelines'] = MagicMock()
            assert 'dlt_meta' not in sys.modules, 'dlt_meta pre-imported'

            from src.dataflow_pipeline import DataflowPipeline
            from databricks.labs.sdp_meta.dataflow_pipeline import (
                DataflowPipeline as Canonical,
            )
            assert DataflowPipeline is Canonical, 'src-first did not resolve to canonical'

            # ``uninstall`` was absent from the old compat/src submodule
            # list; the consolidated single list (dlt_meta) includes it.
            assert 'src.uninstall' in sys.modules, (
                'src.uninstall not registered -- submodule-list drift regressed'
            )
            import databricks.labs.sdp_meta.uninstall as canon_uninstall
            assert sys.modules['src.uninstall'].__file__ == canon_uninstall.__file__, (
                'src.uninstall alias does not point at the canonical module'
            )
            print('OK')
        """)
        result = self._run(code)
        self.assertEqual(
            result.returncode, 0,
            msg=f"stderr: {result.stderr}\nstdout: {result.stdout}",
        )
        self.assertEqual(result.stdout.strip(), "OK")


class TestStubModuleDunderTolerance(unittest.TestCase):
    """Stub modules must survive dunder introspection without raising.

    Tools that walk ``sys.modules`` (IPython's
    ``AutoreloadReliabilityHook``, ``inspect.getmodule``, importlib
    metadata scanners, traceback formatters) routinely do
    ``getattr(mod, "__file__", "")`` and similar dunder probes on every
    loaded module. The original stub raised ``ImportError`` on ANY
    attribute access -- including ``__file__`` -- which on serverless
    DLT poisoned IPython's traceback formatter (which itself probes
    dunders during exception display) and produced the multi-page
    "Unexpected exception formatting exception. Falling back to
    standard exception" cascade after every cell of
    ``validate_phase2.py``.

    The contract these tests pin down:

    1. ``mod.__file__`` is set to a self-describing sentinel string
       (so the most common probe never even calls ``__getattr__``).
    2. ``__getattr__`` raises ``AttributeError`` for dunders (PEP 562
       compliant) -- ``getattr(mod, dunder, default)`` returns the
       default and ``hasattr(mod, dunder)`` returns False, so
       introspection tools fall back cleanly.
    3. Real public attribute access STILL raises the actionable
       ``ImportError`` so customers writing
       ``from src.dataflow_pipeline import DataflowPipeline`` see the
       Lakeflow-SDP-missing message, not a silent ``AttributeError``.
    """

    def setUp(self) -> None:
        # Importing the helper directly is fine -- it's a pure
        # function that builds and returns a fresh ModuleType. None
        # of the side-effecting registration walks fire.
        from dlt_meta import _make_stub_module
        self._make_stub_module = _make_stub_module
        self.stub = _make_stub_module(
            "src.dataflow_pipeline",
            "cannot import name 'pipelines' from 'pyspark'",
        )

    def test_dunder_file_is_set_to_sentinel(self):
        """The most common probe path: ``getattr(mod, '__file__', '')``."""
        self.assertTrue(hasattr(self.stub, "__file__"))
        self.assertIn("sdp-meta", self.stub.__file__)
        self.assertIn("src.dataflow_pipeline", self.stub.__file__)

    def test_getattr_with_default_returns_default_for_unset_dunders(self):
        """``__path__`` is not set; getattr-with-default must NOT raise."""
        sentinel = object()
        result = getattr(self.stub, "__path__", sentinel)
        self.assertIs(result, sentinel)

    def test_hasattr_returns_false_for_unset_dunders(self):
        """``hasattr`` on an unset dunder must return False, not crash."""
        self.assertFalse(hasattr(self.stub, "__path__"))
        self.assertFalse(hasattr(self.stub, "__all__"))
        self.assertFalse(hasattr(self.stub, "__version__"))

    def test_inspect_getmodule_does_not_raise(self):
        """``inspect.getmodule`` is what IPython's tb formatter calls."""
        import inspect
        # ``inspect.getmodule`` walks ``__file__`` to locate modules.
        # We don't care what it returns for a synthetic stub -- only
        # that it doesn't propagate an ImportError.
        try:
            inspect.getmodule(self.stub)
        except ImportError as exc:
            self.fail(f"inspect.getmodule raised ImportError: {exc}")

    def test_dunder_getattr_raises_attribute_error_not_import_error(self):
        """PEP 562 contract: ``__getattr__`` for unknown dunders -> AttributeError."""
        with self.assertRaises(AttributeError):
            self.stub.__nonexistent_dunder__  # noqa: B018

    def test_public_attribute_still_raises_actionable_import_error(self):
        """Real customer-facing access must still get the Lakeflow SDP msg."""
        with self.assertRaises(ImportError) as cm:
            self.stub.DataflowPipeline  # noqa: B018
        msg = str(cm.exception)
        self.assertIn("Lakeflow SDP runtime", msg)
        self.assertIn("pyspark.pipelines", msg)
        self.assertIn("src.dataflow_pipeline", msg)

    def test_iterating_sys_modules_safe(self):
        """The IPython autoreload hook walks ``sys.modules`` and
        probes dunders on every entry. Simulate that iteration to
        prove the stub is hook-safe.
        """
        import sys
        try:
            sys.modules["__sdp_meta_stub_test__"] = self.stub
            for name, mod in list(sys.modules.items()):
                if mod is None:
                    continue
                # This is exactly what file_module_utils.get_module_file_name
                # does on serverless DLT.
                _ = getattr(mod, "__file__", "") or ""
                # And what inspect.getmodule does:
                _ = hasattr(mod, "__file__")
        finally:
            sys.modules.pop("__sdp_meta_stub_test__", None)


class TestPackageGetattrDunderTolerance(unittest.TestCase):
    """The dlt_meta package-level ``__getattr__`` (set when the SDP
    runtime is missing) must follow the same dunder-tolerance contract
    as the stub modules.

    Same failure mode applies: IPython's autoreload hook probes
    ``dlt_meta.__file__`` etc., and a raised ``ImportError`` would
    cascade through the traceback formatter the same way.

    This is exercised in a fresh subprocess (no ``pyspark.pipelines``
    pre-mocked) because the in-process tests have ``pyspark.pipelines``
    mocked to a ``MagicMock``, which means ``_flat_reexport_or_stub``
    succeeds normally and never installs the package-level fallback
    ``__getattr__`` we want to test.
    """

    def test_package_getattr_dunder_tolerance(self):
        code = textwrap.dedent("""
            import sys

            # Same precondition as TestOptionalRuntimeStub: real
            # pyspark, no pyspark.pipelines, so the shim takes the
            # missing-runtime path and installs the package-level
            # ``__getattr__`` fallback on dlt_meta.
            assert 'pyspark.pipelines' not in sys.modules

            import dlt_meta

            # IPython's autoreload reliability hook probe.
            # MUST NOT raise ImportError.
            assert getattr(dlt_meta, '__file__', None) is not None, (
                'dlt_meta.__file__ should resolve to the package init'
            )

            # ``hasattr`` on an unknown dunder must return False
            # (i.e. ``__getattr__`` raised AttributeError, not
            # ImportError).
            assert hasattr(dlt_meta, '__nonexistent__') is False, (
                'hasattr on unknown dunder must return False'
            )

            # ``getattr(..., default)`` on an unknown dunder must
            # return the default.
            sentinel = object()
            assert getattr(dlt_meta, '__nonexistent__', sentinel) is sentinel

            # And public-name access still gets the actionable
            # ImportError, not silently masked.
            try:
                dlt_meta.DataflowPipeline
            except ImportError as exc:
                assert 'Lakeflow SDP runtime' in str(exc)
            else:
                raise AssertionError(
                    'Public attr access should still raise ImportError'
                )

            print('OK')
        """)
        env = {
            **os.environ,
            "PYTHONPATH": os.pathsep.join([_COMPAT_DIR, _REPO_ROOT]),
        }
        env.pop("SDP_META_DISABLE_SRC_ALIAS", None)
        result = subprocess.run(
            [sys.executable, "-c", code],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(
            result.returncode, 0,
            msg=f"stderr: {result.stderr}\nstdout: {result.stdout}",
        )
        self.assertEqual(result.stdout.strip(), "OK")


if __name__ == "__main__":
    unittest.main()
