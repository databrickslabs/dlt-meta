"""Unit tests for the sdp-meta MCP server (issue #231).

These tests exercise the synchronous tool/resource dispatchers directly
(``call_tool`` / ``read_resource``) so we don't have to spin up the asyncio
stdio plumbing. Live workspace calls and the actual ``databricks bundle ...``
subprocesses are mocked at the bundle.py boundary.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

try:
    from databricks.labs.sdp_meta.mcp import server as mcp_server  # noqa: F401
    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover - skip path
    _MCP_AVAILABLE = False


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class PathResolutionTests(unittest.TestCase):
    """Environment overrides resolve consistently across CI platforms."""

    def test_examples_dir_uses_valid_environment_override(self):
        with tempfile.TemporaryDirectory() as examples_dir:
            with patch.dict(
                os.environ,
                {"SDP_META_EXAMPLES_DIR": examples_dir},
            ):
                self.assertEqual(
                    mcp_server._locate_examples_dir(),
                    Path(examples_dir).resolve(),
                )

    def test_invalid_mcp_root_falls_back_to_working_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_root = str(Path(temp_dir) / "missing")
            with patch.dict(
                os.environ,
                {"SDP_META_MCP_ROOT": missing_root},
            ):
                self.assertEqual(mcp_server._mcp_root(), Path.cwd().resolve())


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class ListToolsTests(unittest.TestCase):
    """Lock-in: the v0 tool surface."""

    def test_lists_v0_tools(self):
        names = {t.name for t in mcp_server.list_tools()}
        self.assertEqual(
            names,
            {
                "sdp_meta_bundle_init",
                "sdp_meta_bundle_validate",
                "sdp_meta_bundle_add_flow",
                "sdp_meta_list_templates",
                "sdp_meta_get_onboarding_template",
            },
        )

    def test_every_tool_has_schema_and_description(self):
        for tool in mcp_server.list_tools():
            self.assertTrue(tool.description, f"{tool.name} missing description")
            self.assertEqual(tool.inputSchema.get("type"), "object")

    def test_unknown_tool_raises(self):
        with self.assertRaisesRegex(ValueError, "Unknown tool"):
            mcp_server.call_tool("does_not_exist", {})


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class TemplateToolTests(unittest.TestCase):
    """list_templates + get_onboarding_template hit real packaged files."""

    def test_list_templates_returns_packaged_names(self):
        result = mcp_server.call_tool("sdp_meta_list_templates", {})
        payload = json.loads(result[0].text)
        names = payload["templates"]
        self.assertIn("json/cloudfiles-onboarding.template", names)
        self.assertIn("yml/cloudfiles-onboarding.template.yml", names)
        # Both formats should be represented.
        self.assertTrue(any(n.startswith("json/") for n in names))
        self.assertTrue(any(n.startswith("yml/") for n in names))

    def test_get_onboarding_template_returns_content(self):
        result = mcp_server.call_tool(
            "sdp_meta_get_onboarding_template",
            {"name": "json/cloudfiles-onboarding.template"},
        )
        payload = json.loads(result[0].text)
        self.assertEqual(payload["name"], "json/cloudfiles-onboarding.template")
        # Content is real JSON template text, not empty.
        self.assertGreater(len(payload["content"]), 100)

    def test_get_onboarding_template_unknown_name_raises(self):
        with self.assertRaisesRegex(FileNotFoundError, "Template 'no/such.json' not found"):
            mcp_server.call_tool(
                "sdp_meta_get_onboarding_template", {"name": "no/such.json"}
            )

    def test_get_onboarding_template_requires_name(self):
        with self.assertRaisesRegex(ValueError, "`name` is required"):
            mcp_server.call_tool("sdp_meta_get_onboarding_template", {})


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleInitToolTests(unittest.TestCase):
    """bundle_init delegates to bundle.bundle_init with a quickstart config."""

    def setUp(self):
        # Sandbox root for this test; caller-supplied paths must live here.
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    @patch("databricks.labs.sdp_meta.bundle.write_quickstart_config_file")
    def test_quickstart_writes_config_and_calls_bundle_init(
        self, mock_write_cfg, mock_bundle_init
    ):
        out_dir = self.root / "qs"
        cfg = out_dir / "config.json"
        mock_write_cfg.return_value = str(cfg)
        mock_bundle_init.return_value = 0

        result = mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {"output_dir": str(out_dir), "quickstart": True},
        )
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)
        # write_quickstart_config_file is called with the resolved Path.
        mock_write_cfg.assert_called_once()
        # bundle_init received a BundleInitCommand pointing at our config,
        # with the sandbox-resolved output_dir.
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, str(cfg))
        self.assertEqual(cmd.output_dir, str(out_dir))

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    @patch("databricks.labs.sdp_meta.bundle.write_quickstart_config_file")
    def test_quickstart_forwards_overrides(self, mock_write_cfg, mock_bundle_init):
        out_dir = self.root / "qs"
        mock_write_cfg.return_value = str(out_dir / "config.json")
        mock_bundle_init.return_value = 0

        mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {
                "output_dir": str(out_dir),
                "quickstart": True,
                "overrides": {"uc_catalog_name": "acme_prod"},
            },
        )
        # The overrides dict is threaded into write_quickstart_config_file.
        _, kwargs = mock_write_cfg.call_args
        self.assertEqual(kwargs.get("overrides"), {"uc_catalog_name": "acme_prod"})

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_quickstart_invalid_override_is_rejected(self, mock_bundle_init):
        # A hyphenated catalog must fail before bundle_init runs. Here we do
        # NOT mock write_quickstart_config_file, so its real validation fires.
        out_dir = self.root / "qs2"
        with self.assertRaisesRegex(ValueError, "uc_catalog_name"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {
                    "output_dir": str(out_dir),
                    "quickstart": True,
                    "overrides": {"uc_catalog_name": "bad-catalog"},
                },
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_non_quickstart_requires_config_file(self, mock_bundle_init):
        with self.assertRaisesRegex(ValueError, "config_file is required"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": ".", "quickstart": False},
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_non_quickstart_with_config_file_calls_bundle_init(self, mock_bundle_init):
        mock_bundle_init.return_value = 0
        out_dir = self.root / "x"
        cfg = out_dir / "cfg.json"
        mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {
                "output_dir": str(out_dir),
                "quickstart": False,
                "config_file": str(cfg),
                "profile": "myprofile",
            },
        )
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, str(cfg))
        self.assertEqual(cmd.profile, "myprofile")

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_output_dir_outside_root_is_rejected(self, mock_bundle_init):
        # A path that escapes the sandbox root (parent traversal) must be
        # rejected before bundle_init is ever called.
        with self.assertRaisesRegex(ValueError, "outside the MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": str(self.root / ".." / "escape"), "quickstart": True},
            )
        mock_bundle_init.assert_not_called()

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    def test_sibling_root_prefix_is_not_bypass(self, mock_bundle_init):
        # A sibling dir whose name merely starts with the root name must
        # not be treated as inside the root (trailing-separator boundary).
        sibling = str(self.root) + "_evil"
        with self.assertRaisesRegex(ValueError, "outside the MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_init",
                {"output_dir": sibling, "quickstart": True},
            )
        mock_bundle_init.assert_not_called()


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleValidateToolTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_forwards_args_and_returns_rc(self, mock_validate):
        mock_validate.return_value = 0
        bundle_dir = self.root / "b"
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate",
            {"bundle_dir": str(bundle_dir), "target": "dev", "profile": "p"},
        )
        cmd = mock_validate.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, str(bundle_dir))
        self.assertEqual(cmd.target, "dev")
        self.assertEqual(cmd.profile, "p")
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_propagates_non_zero_rc(self, mock_validate):
        mock_validate.return_value = 2
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate", {"bundle_dir": str(self.root / "b")}
        )
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 2)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_bundle_dir_outside_root_is_rejected(self, mock_validate):
        # ``/etc`` is a symlink to ``/private/etc`` on macOS, so this may be
        # rejected by either the plain out-of-root branch ("outside the MCP
        # filesystem root") or the symlink-escape branch ("escapes the MCP
        # filesystem root"). Match the substring common to both.
        with self.assertRaisesRegex(ValueError, "MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_validate",
                {"bundle_dir": "/etc"},
            )
        mock_validate.assert_not_called()


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleAddFlowToolTests(unittest.TestCase):

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="sdp_mcp_root_")).resolve()
        self._env = patch.dict(
            os.environ, {"SDP_META_MCP_ROOT": str(self.root)}
        )
        self._env.start()

    def tearDown(self):
        self._env.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_builds_flow_specs_from_dicts(self, mock_add):
        mock_add.return_value = 0
        bundle_dir = self.root / "b"
        flows_in = [
            {
                "source_format": "cloudFiles",
                "source_path": "/Volumes/c/s/v/in",
                "bronze_table": "raw_orders",
                "data_flow_group": "demo",
            }
        ]
        result = mcp_server.call_tool(
            "sdp_meta_bundle_add_flow",
            {"bundle_dir": str(bundle_dir), "flows": flows_in, "dry_run": True},
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, str(bundle_dir))
        self.assertTrue(cmd.dry_run)
        self.assertEqual(len(cmd.flows), 1)
        # source_path is a flow data value (often a UC Volume path), NOT a
        # local filesystem path the server writes to — it is intentionally
        # NOT sandbox-constrained and passes through verbatim.
        self.assertEqual(cmd.flows[0].source_path, "/Volumes/c/s/v/in")
        self.assertEqual(cmd.flows[0].bronze_table, "raw_orders")
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)
        self.assertEqual(payload["flows_added"][0]["bronze_table"], "raw_orders")

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_sandboxes_onboarding_file(self, mock_add):
        mock_add.return_value = 0
        bundle_dir = self.root / "b"
        onboarding = self.root / "b" / "conf" / "onboarding.json"
        mcp_server.call_tool(
            "sdp_meta_bundle_add_flow",
            {
                "bundle_dir": str(bundle_dir),
                "onboarding_file": str(onboarding),
                "flows": [{"source_format": "cloudFiles", "bronze_table": "t"}],
            },
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.onboarding_file, str(onboarding))

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_rejects_onboarding_file_outside_root(self, mock_add):
        # ``/etc/passwd`` traverses the ``/etc`` -> ``/private/etc`` symlink on
        # macOS, so rejection may come from the symlink-escape branch or the
        # plain out-of-root branch. Match the substring common to both.
        with self.assertRaisesRegex(ValueError, "MCP filesystem root"):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_flow",
                {
                    "bundle_dir": str(self.root / "b"),
                    "onboarding_file": "/etc/passwd",
                    "flows": [{"source_format": "cloudFiles", "bronze_table": "t"}],
                },
            )
        mock_add.assert_not_called()

    def test_add_flow_rejects_empty_flows(self):
        with self.assertRaisesRegex(ValueError, "at least one entry"):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_flow",
                {"bundle_dir": str(self.root / "b"), "flows": []},
            )


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class ResourceTests(unittest.TestCase):

    def test_list_resources_includes_packaged_templates(self):
        resources = mcp_server.list_resources()
        uris = {str(r.uri) for r in resources}
        self.assertTrue(
            any(u.endswith("json/cloudfiles-onboarding.template") for u in uris),
            f"missing cloudfiles template in resources: {sorted(uris)[:5]}",
        )

    def test_read_resource_returns_template_text(self):
        text = mcp_server.read_resource(
            "sdp-meta://templates/json/cloudfiles-onboarding.template"
        )
        self.assertIn("data_flow_id", text)

    def test_read_resource_rejects_unknown_scheme(self):
        with self.assertRaisesRegex(ValueError, "Unsupported resource URI"):
            mcp_server.read_resource("https://example.com/foo")

    def test_read_resource_rejects_missing_template(self):
        with self.assertRaisesRegex(FileNotFoundError, "Template not found"):
            mcp_server.read_resource("sdp-meta://templates/json/nope.template")


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BuildServerTests(unittest.TestCase):
    """Smoke-test that build_server registers handlers without crashing."""

    def test_build_server_returns_named_server(self):
        srv = mcp_server.build_server(MagicMock())
        self.assertEqual(srv.name, "sdp-meta")


class CliWiringTests(unittest.TestCase):
    """The `mcp` command must be reachable from the CLI dispatcher."""

    def test_mcp_command_in_mapping(self):
        from databricks.labs.sdp_meta.cli import MAPPING

        self.assertIn("mcp", MAPPING)

    def test_mcp_command_in_labs_yml(self):
        import yaml

        with open("labs.yml") as f:
            doc = yaml.safe_load(f)
        commands = {c["name"] for c in doc["commands"]}
        self.assertIn("mcp", commands)

    def test_mcp_handler_raises_useful_error_when_extra_missing(self):
        """If `mcp` extra is missing, the CLI handler should ImportError clearly."""
        from databricks.labs.sdp_meta.cli import mcp as mcp_cmd

        with patch.dict("sys.modules", {"databricks.labs.sdp_meta.mcp.server": None}):
            with self.assertRaises(ImportError) as ctx:
                mcp_cmd(MagicMock())
            self.assertIn("mcp", str(ctx.exception).lower())


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class McpShadowGuardTests(unittest.TestCase):
    """Regression: the `mcp` CLI command must survive the local
    ``sdp_meta/mcp`` package shadowing the PyPI ``mcp`` SDK when
    ``databricks labs`` runs cli.py as a script (script dir on
    ``sys.path[0]``)."""

    def setUp(self):
        from databricks.labs.sdp_meta import cli

        self.cli = cli
        self.script_dir = os.path.dirname(os.path.abspath(cli.__file__))
        self._orig_path = list(sys.path)

    def tearDown(self):
        sys.path[:] = self._orig_path

    def test_guard_pops_shadowing_script_dir_and_runs(self):
        # Simulate the labs script invocation: the module's own dir (which
        # contains the local ``mcp`` subpackage) sits at sys.path[0].
        sys.path.insert(0, self.script_dir)
        with patch(
            "databricks.labs.sdp_meta.mcp.server.run_stdio"
        ) as mock_run:
            self.cli.mcp(MagicMock())
        mock_run.assert_called_once()
        # The guard removed the shadowing script dir from position 0 so the
        # bare ``mcp`` import could resolve to the installed SDK.
        if sys.path:
            self.assertNotEqual(
                os.path.abspath(sys.path[0]), self.script_dir
            )

    def test_missing_sdk_reports_extra_not_installed(self):
        # Genuine missing dependency: find_spec("mcp") is None AND the
        # server import fails -> the message must point at the extra.
        with patch.dict(
            "sys.modules", {"databricks.labs.sdp_meta.mcp.server": None}
        ):
            with patch("importlib.util.find_spec", return_value=None):
                with self.assertRaises(ImportError) as ctx:
                    self.cli.mcp(MagicMock())
        self.assertIn("extra is not installed", str(ctx.exception))

    def test_import_failure_with_sdk_present_reports_shadowing(self):
        # SDK is present but the server import still fails -> this is NOT a
        # missing extra; the message must call out shadowing instead of
        # sending the user to reinstall a dependency they already have.
        with patch.dict(
            "sys.modules", {"databricks.labs.sdp_meta.mcp.server": None}
        ):
            with patch("importlib.util.find_spec", return_value=object()):
                with self.assertRaises(ImportError) as ctx:
                    self.cli.mcp(MagicMock())
        self.assertIn("shadow", str(ctx.exception).lower())


if __name__ == "__main__":
    unittest.main()
