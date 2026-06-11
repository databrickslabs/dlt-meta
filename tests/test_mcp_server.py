"""Unit tests for the sdp-meta MCP server (issue #231).

These tests exercise the synchronous tool/resource dispatchers directly
(``call_tool`` / ``read_resource``) so we don't have to spin up the asyncio
stdio plumbing. Live workspace calls and the actual ``databricks bundle ...``
subprocesses are mocked at the bundle.py boundary.
"""

import json
import unittest
from unittest.mock import MagicMock, patch

try:
    from databricks.labs.sdp_meta.mcp import server as mcp_server  # noqa: F401
    _MCP_AVAILABLE = True
except ImportError:  # pragma: no cover - skip path
    _MCP_AVAILABLE = False


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

    @patch("databricks.labs.sdp_meta.bundle.bundle_init")
    @patch("databricks.labs.sdp_meta.bundle.write_quickstart_config_file")
    def test_quickstart_writes_config_and_calls_bundle_init(
        self, mock_write_cfg, mock_bundle_init
    ):
        mock_write_cfg.return_value = "/tmp/qs/config.json"
        mock_bundle_init.return_value = 0

        result = mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {"output_dir": "/tmp/qs", "quickstart": True},
        )
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)
        # write_quickstart_config_file is called with the resolved Path.
        mock_write_cfg.assert_called_once()
        # bundle_init received a BundleInitCommand pointing at our config.
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, "/tmp/qs/config.json")
        self.assertEqual(cmd.output_dir, "/tmp/qs")

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
        mcp_server.call_tool(
            "sdp_meta_bundle_init",
            {
                "output_dir": "/tmp/x",
                "quickstart": False,
                "config_file": "/tmp/x/cfg.json",
                "profile": "myprofile",
            },
        )
        cmd = mock_bundle_init.call_args[0][0]
        self.assertEqual(cmd.config_file, "/tmp/x/cfg.json")
        self.assertEqual(cmd.profile, "myprofile")


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleValidateToolTests(unittest.TestCase):

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_forwards_args_and_returns_rc(self, mock_validate):
        mock_validate.return_value = 0
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate",
            {"bundle_dir": "/tmp/b", "target": "dev", "profile": "p"},
        )
        cmd = mock_validate.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, "/tmp/b")
        self.assertEqual(cmd.target, "dev")
        self.assertEqual(cmd.profile, "p")
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)

    @patch("databricks.labs.sdp_meta.bundle.bundle_validate")
    def test_validate_propagates_non_zero_rc(self, mock_validate):
        mock_validate.return_value = 2
        result = mcp_server.call_tool(
            "sdp_meta_bundle_validate", {"bundle_dir": "/tmp/b"}
        )
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 2)


@unittest.skipUnless(_MCP_AVAILABLE, "mcp extra not installed")
class BundleAddFlowToolTests(unittest.TestCase):

    @patch("databricks.labs.sdp_meta.bundle.bundle_add_flow")
    def test_add_flow_builds_flow_specs_from_dicts(self, mock_add):
        mock_add.return_value = 0
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
            {"bundle_dir": "/tmp/b", "flows": flows_in, "dry_run": True},
        )
        cmd = mock_add.call_args[0][0]
        self.assertEqual(cmd.bundle_dir, "/tmp/b")
        self.assertTrue(cmd.dry_run)
        self.assertEqual(len(cmd.flows), 1)
        self.assertEqual(cmd.flows[0].source_path, "/Volumes/c/s/v/in")
        self.assertEqual(cmd.flows[0].bronze_table, "raw_orders")
        payload = json.loads(result[0].text)
        self.assertEqual(payload["returncode"], 0)
        self.assertEqual(payload["flows_added"][0]["bronze_table"], "raw_orders")

    def test_add_flow_rejects_empty_flows(self):
        with self.assertRaisesRegex(ValueError, "at least one entry"):
            mcp_server.call_tool(
                "sdp_meta_bundle_add_flow",
                {"bundle_dir": "/tmp/b", "flows": []},
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


if __name__ == "__main__":
    unittest.main()
