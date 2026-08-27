"""Flask-client tests for the App's ``/deploy`` form handler:

* Server-side mandatory-field validation — the client-side JS check is
  the first line of defence; this is the second line, for hand-crafted
  POSTs and to short-circuit silent ``None`` schema → malformed
  ``catalog.None.bronze_dataflowspec`` pipeline configs.
* Key-translation: the HTML form field names (``spc_schema_name``,
  ``bronze_dataflowspec_table``) must be translated to the CLI's
  canonical key names (``sdp_meta_bronze_schema``,
  ``sdp_meta_silver_schema``, ``dataflowspec_bronze_table``,
  ``dataflowspec_silver_table``) before they hit the subprocess —
  same class of bug as the form-key / CLI-key mismatch guarded by
  test_onboarding_payload_uses_sdp_meta_keys on /onboarding.

The Flask ``app`` object lives in ``databricks_app/app.py``. That module
imports ``uc_preflight`` as a sibling, so the test bootstrap puts
``databricks_app/`` on ``sys.path`` before importing.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time
import unittest
from unittest import mock

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)


# A complete, valid deploy form. Each test starts from this and removes
# / overrides the field under test. Mirrors the field names on the HTML
# form (``deploymentForm`` in landingPage.html).
_VALID_DEPLOY_FORM = {
    "uc_enabled": "1",
    "uc_catalog_name": "my_catalog",
    "serverless": "1",
    "deploylayer": "bronze",
    "pipeline_name": "my_pipeline",
    "spc_schema_name": "sdp_meta_dataflowspecs",
    "dlt_target_schema": "sdp_meta_bronze",
    "onboard_bronze_group": "A1",
    "onboard_silver_group": "A1",
    "bronze_dataflowspec_table": "bronze_dataflowspec",
    "silver_dataflowspec_table": "silver_dataflowspec",
}


class DeployServerSideValidationTests(unittest.TestCase):
    """Server-side mandatory-field rejection. The JS in landingPage.html
    catches these on the client, but a hand-crafted POST can bypass the
    UI — and a missing schema field silently lands in the CLI as
    ``None``, producing a pipeline config like ``cat.None.bronze_*``
    which fails at runtime instead of immediately at the form."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def _post_missing(self, *field_names):
        data = dict(_VALID_DEPLOY_FORM)
        for fld in field_names:
            data.pop(fld, None)
        # Defensive: also send empty-string variants for fields we
        # didn't pop, since the App's check uses ``.strip()`` falsiness.
        return self.client.post("/deploy", data=data)

    def test_missing_pipeline_name_returns_400(self):
        resp = self._post_missing("pipeline_name")
        self.assertEqual(resp.status_code, 400, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("pipeline_name", body["error"])

    def test_missing_spc_schema_name_returns_400(self):
        resp = self._post_missing("spc_schema_name")
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("DataFlow Spec Schema", body["error"])

    def test_missing_dlt_target_schema_returns_400(self):
        resp = self._post_missing("dlt_target_schema")
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("Target Schema", body["error"])

    def test_missing_uc_catalog_when_uc_enabled_returns_400(self):
        resp = self._post_missing("uc_catalog_name")
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("Unity Catalog Name", body["error"])

    def test_uc_disabled_post_is_rejected_uc_only_contract(self):
        # The App is Unity Catalog-only; the legacy ``uc_enabled``
        # toggle was removed from the UI in favour of a hidden input
        # pinned to "1". A hand-crafted POST with ``uc_enabled=0`` is
        # unreachable from the App UI and must be rejected with a
        # clear 400 instead of being silently routed through the
        # deploy pipeline (which would build a pipeline config that
        # fails at runtime against any modern Databricks workspace).
        # Locks in the UC-only product contract -- see
        # ``handle_deploy_form`` for the matching guard.
        data = dict(_VALID_DEPLOY_FORM)
        data["uc_enabled"] = "0"
        data.pop("uc_catalog_name", None)
        with mock.patch.object(app_mod.subprocess, "Popen",
                               side_effect=lambda *a, **kw: _SilentProc()):
            resp = self.client.post("/deploy", data=data)
        self.assertEqual(resp.status_code, 400, resp.get_data(as_text=True))
        body = resp.get_json()
        self.assertIn("Unity Catalog", body["error"])
        self.assertIn("uc_enabled", body["error"])

    def test_missing_bronze_group_for_bronze_layer_returns_400(self):
        resp = self._post_missing("onboard_bronze_group")
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("Bronze Group", body["error"])

    def test_missing_silver_group_for_silver_layer_returns_400(self):
        data = dict(_VALID_DEPLOY_FORM)
        data["deploylayer"] = "silver"
        data.pop("onboard_silver_group", None)
        resp = self.client.post("/deploy", data=data)
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("Silver Group", body["error"])

    def test_missing_bronze_group_for_silver_layer_is_ok(self):
        # Bronze group is only required when deploying bronze or
        # bronze_silver. Silver-only deploys can omit it.
        data = dict(_VALID_DEPLOY_FORM)
        data["deploylayer"] = "silver"
        data.pop("onboard_bronze_group", None)
        with mock.patch.object(app_mod.subprocess, "Popen",
                               side_effect=lambda *a, **kw: _SilentProc()):
            resp = self.client.post("/deploy", data=data)
        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))

    def test_multiple_missing_fields_listed_in_error(self):
        resp = self._post_missing("pipeline_name", "dlt_target_schema")
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("pipeline_name", body["error"])
        self.assertIn("Target Schema", body["error"])

    def test_whitespace_only_field_treated_as_missing(self):
        data = dict(_VALID_DEPLOY_FORM)
        data["pipeline_name"] = "   "
        resp = self.client.post("/deploy", data=data)
        self.assertEqual(resp.status_code, 400)
        body = resp.get_json()
        self.assertIn("pipeline_name", body["error"])

    def test_malformed_uc_identifiers_rejected(self):
        """Every UC identifier the form supplies is validated, not just the
        catalog: a hyphen (legal in UC, illegal for unquoted SQL splicing)
        in the spec schema, target schema, or a dataflowspec table name must
        return 400 (issue #261)."""
        for field, bad_value in (
            ("uc_catalog_name", "my-catalog"),
            ("spc_schema_name", "bad-schema"),
            ("dlt_target_schema", "bad-target"),
            ("bronze_dataflowspec_table", "bad-table"),
            ("silver_dataflowspec_table", "bad-table"),
        ):
            with self.subTest(field=field):
                data = dict(_VALID_DEPLOY_FORM)
                data[field] = bad_value
                resp = self.client.post("/deploy", data=data)
                self.assertEqual(
                    resp.status_code, 400, resp.get_data(as_text=True)
                )
                self.assertIn("identifier", resp.get_json()["error"])


class DeployPayloadKeyMappingTests(unittest.TestCase):
    """Regression: the JSON envelope the App sends to the CLI subprocess
    MUST use the CLI's canonical key names. The HTML form's
    ``spc_schema_name`` becomes BOTH ``sdp_meta_bronze_schema`` and
    ``sdp_meta_silver_schema`` (one spec schema, two specs), and
    ``bronze_dataflowspec_table`` becomes ``dataflowspec_bronze_table``.
    Same shape of trap as the form-key / CLI-key mismatch guarded by
    test_onboarding_payload_uses_sdp_meta_keys on /onboarding."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_deploy_payload_uses_cli_canonical_keys(self):
        captured = {}

        def _fake_popen(args, **kwargs):
            captured["json_payload"] = json.loads(args[-1])
            return _SilentProc()

        with mock.patch.object(app_mod.subprocess, "Popen",
                               side_effect=_fake_popen):
            resp = self.client.post("/deploy", data={
                **_VALID_DEPLOY_FORM,
                "spc_schema_name": "my_dataflowspec_schema",
                "bronze_dataflowspec_table": "my_bronze_table",
                "silver_dataflowspec_table": "my_silver_table",
            })
            # The route returns immediately and the subprocess fires from
            # a daemon thread — wait for it to call Popen before asserting.
            for _ in range(50):
                if "json_payload" in captured:
                    break
                time.sleep(0.02)

        self.assertEqual(resp.status_code, 200, resp.get_data(as_text=True))
        payload = captured.get("json_payload")
        self.assertIsNotNone(payload, "subprocess.Popen was not invoked")

        # The user's chosen schema must land in BOTH bronze and silver
        # spec-schema slots — they live in the same dataflowspec schema.
        self.assertEqual(payload.get("sdp_meta_bronze_schema"), "my_dataflowspec_schema")
        self.assertEqual(payload.get("sdp_meta_silver_schema"), "my_dataflowspec_schema")

        # Table names must be translated from the HTML form names to
        # the CLI's canonical names.
        self.assertEqual(payload.get("dataflowspec_bronze_table"), "my_bronze_table")
        self.assertEqual(payload.get("dataflowspec_silver_table"), "my_silver_table")

        # The HTML-only key MUST NOT leak through — leaving it in would
        # let the CLI's ``_load_deploy_config_ui`` silently ignore it
        # and produce a malformed pipeline config.
        self.assertNotIn("spc_schema_name", payload)
        self.assertNotIn("bronze_dataflowspec_table", payload)

    def test_deploy_payload_serverless_is_string_for_cli(self):
        # The envelope contract: serverless is sent as the string "1"/"0"
        # (matching the HTML radio values). The CLI's ``_coerce_bool``
        # converts it to a real Python bool — that's where the
        # bool-coercion test lives (test_cli.py). Here we only verify
        # the App sends the expected string.
        captured = {}

        def _fake_popen(args, **kwargs):
            captured["json_payload"] = json.loads(args[-1])
            return _SilentProc()

        with mock.patch.object(app_mod.subprocess, "Popen",
                               side_effect=_fake_popen):
            self.client.post("/deploy", data={**_VALID_DEPLOY_FORM,
                                              "serverless": "1"})
            for _ in range(50):
                if "json_payload" in captured:
                    break
                time.sleep(0.02)

        payload = captured.get("json_payload")
        self.assertIsNotNone(payload)
        self.assertEqual(payload.get("serverless"), "1")
        self.assertEqual(payload.get("uc_enabled"), "1")


# ── helpers ─────────────────────────────────────────────────────────────


class _SilentProc:
    """Minimal stand-in for the subprocess the App spawns. The
    ``handle_deploy_form`` reader threads iterate ``stdout``/``stderr``
    until EOF; empty StringIO buffers give them an immediate EOF and
    the daemon thread exits cleanly without us needing to plumb real
    CLI output."""

    def __init__(self):
        self.stdout = io.StringIO("")
        self.stderr = io.StringIO("")
        self.returncode = 0

    def wait(self, timeout=None):
        return 0

    def poll(self):
        # H-1: the runner's reap-on-cleanup path calls ``proc.poll()``
        # to decide whether to terminate. Returning a non-None value
        # mimics "already exited".
        return 0


if __name__ == "__main__":
    unittest.main()
