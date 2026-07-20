"""Unit tests for standard-compute legacy upgrade orchestration."""

from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import MagicMock, patch

from databricks.sdk.service.pipelines import PipelineCluster

from integration_tests.run_backward_compat_tests import (
    BCRunnerConf,
    BackwardCompatRunner,
)
from integration_tests.version_profiles import CURRENT, LEGACY


class StandardLegacyUpgradeRunnerTests(TestCase):
    """Ensure standard-compute tests retain legacy pipeline publishing."""

    def setUp(self):
        self.ws = MagicMock()
        self.runner = BackwardCompatRunner({}, self.ws)
        self.conf = BCRunnerConf(
            run_id="test-run",
            uc_catalog_name="catalog",
            source_ref="v0.0.10",
            target_ref="v0.1.0",
            source_profile=LEGACY,
            target_profile=CURRENT,
            pipeline_mode="standard_legacy",
            pipeline_num_workers=2,
            bronze_schema="bronze",
            silver_schema="silver",
            sdp_meta_schema="specs",
            runners_nb_path="/Users/test/runners",
            source_main_whl_remote="/Volumes/test/source.whl",
            target_main_whl_remote="/Volumes/test/target.whl",
        )

    def _legacy_spec(self, target, wheel):
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=target,
            schema=None,
            serverless=False,
            clusters=[PipelineCluster(label="default", num_workers=2)],
            libraries=[],
            configuration={"dlt_meta_whl": wheel},
        )

    def test_standard_legacy_execution_kwargs_use_target_and_cluster(self):
        kwargs = self.runner._pipeline_execution_kwargs(self.conf, "bronze")

        self.assertEqual(kwargs["target"], "bronze")
        self.assertFalse(kwargs["serverless"])
        self.assertNotIn("schema", kwargs)
        self.assertEqual(kwargs["clusters"][0].label, "default")
        self.assertEqual(kwargs["clusters"][0].num_workers, 2)

    def test_serverless_dpm_execution_kwargs_use_schema(self):
        self.conf.pipeline_mode = "serverless_dpm"
        kwargs = self.runner._pipeline_execution_kwargs(self.conf, "bronze")

        self.assertEqual(kwargs, {"serverless": True, "schema": "bronze"})

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_standard_legacy_pipeline_bypasses_schema_fallback(self, sdk_create):
        self.ws.pipelines.create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)
        )

        pipeline_id = self.runner.create_pipeline(
            self.conf, "pipeline", "bronze", "A1", "bronze"
        )

        self.assertEqual(pipeline_id, "p1")
        sdk_create.assert_not_called()
        kwargs = self.ws.pipelines.create.call_args.kwargs
        self.assertEqual(kwargs["target"], "bronze")
        self.assertFalse(kwargs["serverless"])
        self.assertNotIn("schema", kwargs)
        self.assertEqual(kwargs["clusters"][0].num_workers, 2)

    def test_swap_standard_legacy_pipeline_preserves_target_and_cluster(self):
        self.conf.bronze_a1_pipeline_id = "bronze-a1"
        self.conf.bronze_a2_pipeline_id = "bronze-a2"
        self.conf.silver_pipeline_id = "silver"
        self.ws.pipelines.get.side_effect = [
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.target_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("bronze", self.conf.target_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("silver", self.conf.source_main_whl_remote)),
            SimpleNamespace(spec=self._legacy_spec("silver", self.conf.target_main_whl_remote)),
        ]

        self.runner.swap_pipelines_to_target(self.conf)

        self.assertEqual(self.ws.pipelines.update.call_count, 3)
        calls = self.ws.pipelines.update.call_args_list
        self.assertEqual(calls[0].kwargs["target"], "bronze")
        self.assertEqual(calls[1].kwargs["target"], "bronze")
        self.assertEqual(calls[2].kwargs["target"], "silver")
        for call in calls:
            self.assertFalse(call.kwargs["serverless"])
            self.assertNotIn("schema", call.kwargs)
            self.assertEqual(call.kwargs["clusters"][0].num_workers, 2)

    def _serverless_spec(self, schema, wheel):
        return SimpleNamespace(
            catalog="catalog",
            name="pipeline",
            target=None,
            schema=schema,
            serverless=True,
            clusters=[],
            libraries=[],
            configuration={"dlt_meta_whl": wheel},
        )

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_serverless_pipeline_verifies_wheel_key(self, sdk_create):
        self.conf.pipeline_mode = "serverless_dpm"
        sdk_create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._serverless_spec("bronze", self.conf.source_main_whl_remote)
        )

        pipeline_id = self.runner.create_pipeline(
            self.conf, "pipeline", "bronze", "A1", "bronze"
        )

        self.assertEqual(pipeline_id, "p1")
        sdk_create.assert_called_once()
        kwargs = sdk_create.call_args.kwargs
        self.assertEqual(kwargs["schema"], "bronze")
        self.assertTrue(kwargs["serverless"])

    @patch("integration_tests.run_backward_compat_tests.sdk_create_pipeline")
    def test_create_serverless_pipeline_fails_on_wrong_wheel(self, sdk_create):
        self.conf.pipeline_mode = "serverless_dpm"
        sdk_create.return_value = SimpleNamespace(pipeline_id="p1")
        self.ws.pipelines.get.return_value = SimpleNamespace(
            spec=self._serverless_spec("bronze", "/Volumes/test/stale.whl")
        )

        with self.assertRaisesRegex(RuntimeError, "dlt_meta_whl"):
            self.runner.create_pipeline(
                self.conf, "pipeline", "bronze", "A1", "bronze"
            )

    def test_standard_legacy_requires_positive_worker_count(self):
        runner = BackwardCompatRunner(
            {
                "uc_catalog_name": "catalog",
                "pipeline_mode": "standard_legacy",
            },
            self.ws,
        )
        self.ws.current_user.me.return_value = SimpleNamespace(
            user_name="test@example.com"
        )

        with self.assertRaisesRegex(ValueError, "pipeline_num_workers"):
            runner._build_runner_conf()
