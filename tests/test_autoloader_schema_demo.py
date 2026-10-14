import json
import re
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

import yaml

from demo.launch_autoloader_schema_demo import SDPMetaAutoLoaderSchemaDemo


REPO_ROOT = Path(__file__).resolve().parents[1]
JSON_TEMPLATE = (
    REPO_ROOT / "demo/conf/json/autoloader-schema-onboarding.template"
)
YAML_TEMPLATE = (
    REPO_ROOT / "demo/conf/yml/autoloader-schema-onboarding.template.yml"
)
INTERACTIVE_DEMO = REPO_ROOT / "demo/SDP_META_INTERACTIVE_DEMO.py"


def _render_template(path):
    text = path.read_text(encoding="utf-8")
    substitutions = {
        "{uc_volume_path}": "/Volumes/main/meta/config/",
        "{uc_catalog_name}": "main",
        "{bronze_schema}": "bronze",
    }
    for key, value in substitutions.items():
        text = text.replace(key, value)
    if path.suffix in {".yml", ".yaml"}:
        return yaml.safe_load(text)
    return json.loads(text)


class AutoLoaderSchemaDemoConfigTests(TestCase):

    def test_interactive_demo_sql_uses_the_uc_identifier_boundary(self):
        source = INTERACTIVE_DEMO.read_text(encoding="utf-8")

        # The wrapper is the only allowed direct Spark SQL boundary. Any
        # future cell must call _run_sql so catalog/schema identifiers are
        # converted before SQL reaches Spark.
        self.assertEqual(source.count("spark.sql("), 1)

        wrapper_start = source.index("def _run_sql(")
        wrapper_end = source.index("\ndef _write_conf", wrapper_start)
        wrapper = source[wrapper_start:wrapper_end]

        # Every schema variable that creates a UC schema is explicitly
        # covered by the wrapper's name map. Adding another schema without
        # extending the map fails this test before the notebook can regress.
        created_schema_variables = set(
            re.findall(r"^([A-Za-z_]+_schema)\s*=\s*f[\"']", source, re.MULTILINE)
        )
        created_schema_variables.add("uc_schema_name")
        for schema_variable in created_schema_variables:
            self.assertIn(
                f"f\"{{uc_catalog_name}}.{{{schema_variable}}}\"",
                wrapper,
                msg=f"{schema_variable} is not mapped by _run_sql",
            )

    def test_interactive_demo_preinstall_validation_matches_raw_uc_names(self):
        source = INTERACTIVE_DEMO.read_text(encoding="utf-8")
        self.assertIn("_CONTROL_CHAR_RE_PREINSTALL", source)
        self.assertNotIn("_REGULAR_IDENT_RE_PREINSTALL", source)
        self.assertIn('or "`" in name', source)

    def test_json_and_yaml_templates_are_equivalent_and_have_no_ddl(self):
        json_payload = _render_template(JSON_TEMPLATE)
        yaml_payload = _render_template(YAML_TEMPLATE)

        self.assertEqual(json_payload, yaml_payload)
        self.assertEqual(len(json_payload), 1)
        flow = json_payload[0]
        self.assertNotIn("source_schema_path", flow["source_details"])

        options = flow["bronze_reader_options"]
        self.assertEqual(options["cloudFiles.inferColumnTypes"], "true")
        self.assertEqual(
            options["cloudFiles.schemaHints"],
            "event_id BIGINT, event_ts TIMESTAMP, amount DECIMAL(10,2)",
        )
        self.assertEqual(
            options["cloudFiles.schemaEvolutionMode"], "addNewColumns"
        )
        self.assertEqual(
            options["cloudFiles.rescuedDataColumn"], "_rescued_data"
        )

    def test_phase_fixtures_add_column_and_incompatible_value(self):
        fixture_root = (
            REPO_ROOT / "demo/resources/data/autoloader_schema_demo"
        )
        phase1 = [
            json.loads(line)
            for line in (
                fixture_root / "phase1/events.json"
            ).read_text(encoding="utf-8").splitlines()
        ]
        phase2 = [
            json.loads(line)
            for line in (
                fixture_root / "phase2/events.json"
            ).read_text(encoding="utf-8").splitlines()
        ]

        self.assertEqual(len(phase1), 3)
        self.assertTrue(all("device_type" not in row for row in phase1))
        self.assertEqual(len(phase2), 2)
        self.assertTrue(all("device_type" in row for row in phase2))
        self.assertIn("not-a-decimal", {row["amount"] for row in phase2})

    def test_yaml_runner_configuration_selects_yaml_template(self):
        demo = SDPMetaAutoLoaderSchemaDemo.__new__(
            SDPMetaAutoLoaderSchemaDemo
        )
        demo.args = {
            "uc_catalog_name": "main",
            "onboarding_file_format": "yaml",
        }
        demo.wsi = SimpleNamespace(_my_username="demo@example.com")

        conf = demo.init_runner_conf()

        self.assertEqual(conf.onboarding_file_format, "yaml")
        self.assertEqual(
            conf.cloudfiles_template,
            "demo/conf/yml/autoloader-schema-onboarding.template.yml",
        )
        self.assertTrue(conf.onboarding_file_path.endswith(".yml"))

    def test_runner_configuration_accepts_hyphenated_catalog_name(self):
        demo = SDPMetaAutoLoaderSchemaDemo.__new__(
            SDPMetaAutoLoaderSchemaDemo
        )
        demo.args = {"uc_catalog_name": "bad-catalog"}
        demo.wsi = SimpleNamespace(_my_username="demo@example.com")

        conf = demo.init_runner_conf()
        self.assertEqual(conf.uc_catalog_name, "bad-catalog")

    def test_interactive_demo_covers_evolution_validation_and_cleanup(self):
        notebook = INTERACTIVE_DEMO.read_text(encoding="utf-8")

        expected_markers = (
            "## Stage 13: Auto Loader Schema Inference & Evolution",
            '"cloudFiles.schemaHints"',
            '"cloudFiles.schemaEvolutionMode": "addNewColumns"',
            '"cloudFiles.rescuedDataColumn": "_rescued_data"',
            '"bronze.group": "SCHEMA_EVOLUTION"',
            "inferred_pipeline_id_file",
            "pipeline_specs = [",
            "Schema evolution validated:",
            "inferred_smoke_fqn",
            'label="schema inference phase 1",\n    full_refresh=True,',
            "def find_successor_update(",
            "following it instead of starting",
        )
        for marker in expected_markers:
            with self.subTest(marker=marker):
                self.assertIn(marker, notebook)


class AutoLoaderSchemaDemoWorkflowTests(TestCase):

    @patch(
        "demo.launch_autoloader_schema_demo.webbrowser.open"
    )
    def test_launcher_waits_for_workflow_validation(self, open_browser):
        demo = SDPMetaAutoLoaderSchemaDemo.__new__(
            SDPMetaAutoLoaderSchemaDemo
        )
        waiter = SimpleNamespace(
            run_id=456,
            result=MagicMock(),
        )
        demo.ws = MagicMock()
        demo.ws.config.host = "https://example.cloud.databricks.com"
        demo.ws.get_workspace_id.return_value = 789
        demo.ws.jobs.run_now.return_value = waiter
        demo.create_schema_workflow_spec = MagicMock(
            return_value=SimpleNamespace(job_id=123)
        )
        conf = SimpleNamespace(job_id=None)

        demo.launch_workflow(conf)

        self.assertEqual(conf.job_id, 123)
        demo.ws.jobs.run_now.assert_called_once_with(job_id=123)
        waiter.result.assert_called_once_with(
            timeout=timedelta(minutes=30)
        )
        open_browser.assert_called_once()

    def test_workflow_orders_both_phases_and_retries_evolution(self):
        demo = SDPMetaAutoLoaderSchemaDemo.__new__(
            SDPMetaAutoLoaderSchemaDemo
        )
        demo.ws = MagicMock()
        demo.ws.jobs.create.return_value = SimpleNamespace(job_id=123)
        conf = SimpleNamespace(
            run_id="run123",
            remote_whl_path="/Volumes/main/meta/config/demo.whl",
            uc_catalog_name="main",
            sdp_meta_schema="meta",
            bronze_schema="bronze",
            uc_volume_path="/Volumes/main/meta/config/",
            onboarding_file_path="demo/conf/json/onboarding.json",
            env="demo",
            bronze_pipeline_id="pipeline-id",
            runners_nb_path="/Users/demo/schema-demo",
        )

        demo.create_schema_workflow_spec(conf)

        tasks = demo.ws.jobs.create.call_args.kwargs["tasks"]
        task_by_key = {task.task_key: task for task in tasks}
        self.assertEqual(
            list(task_by_key),
            [
                "stage_phase_1",
                "onboarding_job",
                "pipeline_phase_1",
                "validate_phase_1",
                "stage_phase_2",
                "pipeline_phase_2",
                "validate_phase_2",
            ],
        )
        self.assertEqual(task_by_key["pipeline_phase_2"].max_retries, 2)
        self.assertEqual(
            task_by_key["pipeline_phase_2"].min_retry_interval_millis,
            10000,
        )
        self.assertEqual(
            task_by_key["pipeline_phase_2"].pipeline_task.pipeline_id,
            "pipeline-id",
        )
