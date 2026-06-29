"""Tests for the Monitor tab's SDP-META pipeline filter.

Background: the Databricks SDK returns two *different* shapes for
pipelines, and the App's filter must understand both:

  * ``ws.pipelines.list_pipelines()`` yields ``PipelineStateInfo`` — a
    flat summary object with no ``.spec`` and no tags / configuration.
  * ``ws.pipelines.get(id)`` returns ``GetPipelineResponse`` whose
    ``tags`` and ``configuration`` live under a nested ``.spec``
    attribute (a ``PipelineSpec``).

The previous filter read ``getattr(detail, "tags", ...)`` directly on
``GetPipelineResponse``, which always evaluated to ``None`` — the Monitor
tab therefore silently dropped every pipeline. This test module pins the
fix in place.

Three coverage tracks:
  1. ``_is_sdp_meta`` pure-function tests on every shape we expect.
  2. End-to-end Flask client test against the user-reported pipeline
     JSON ("sdp_meta_app_car_demo_pipeline") to catch any future
     regression of the same class.
  3. Defensive cases (no spec, tags as the wrong type, etc.).
"""

from __future__ import annotations

import os
import sys
import unittest
from types import SimpleNamespace
from unittest import mock

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402
from routes import pipelines as pipelines_mod  # noqa: E402


def _get_pipeline_response(*, tags=None, configuration=None):
    """Build a fake ``GetPipelineResponse`` (spec nested under ``.spec``)."""
    spec = SimpleNamespace(tags=tags or {}, configuration=configuration or {})
    return SimpleNamespace(
        pipeline_id="pid-1",
        name="some_pipeline",
        creator_user_name="me@example.com",
        last_modified=1700000000000,
        spec=spec,
    )


def _state_info():
    """Build a fake ``PipelineStateInfo`` (flat summary, no .spec)."""
    return SimpleNamespace(
        pipeline_id="pid-1",
        name="some_pipeline",
        state=SimpleNamespace(value="IDLE"),
    )


class IsSdpMetaTests(unittest.TestCase):
    """Unit tests for ``_is_sdp_meta`` on every input shape."""

    # ── Tag-based detection: any non-empty value is a match ─────────
    #
    # The producer in cli.py writes the SDP-META version (e.g. "0.1.0")
    # into the ``sdp_meta`` tag. The legacy sentinel ``"true"`` is
    # accepted for back-compat with pipelines created by older releases.

    def test_get_pipeline_response_with_version_tag(self):
        # Current producer: tag value is the SDP-META version.
        detail = _get_pipeline_response(tags={"sdp_meta": "0.1.0"})
        self.assertTrue(pipelines_mod._is_sdp_meta(detail))

    def test_get_pipeline_response_with_arbitrary_future_version_tag(self):
        # Future version strings (semver, pre-release suffix, build metadata)
        # must all match — the filter is "any non-empty value", not a
        # whitelist of known versions.
        for v in ["0.1.1", "0.2.0", "1.0.0-rc1", "1.0.0+build.42"]:
            with self.subTest(version=v):
                detail = _get_pipeline_response(tags={"sdp_meta": v})
                self.assertTrue(pipelines_mod._is_sdp_meta(detail))

    def test_get_pipeline_response_with_legacy_true_tag(self):
        # Back-compat: pipelines created before the version-tag change
        # have ``sdp_meta=true``. Must still match.
        detail = _get_pipeline_response(tags={"sdp_meta": "true"})
        self.assertTrue(pipelines_mod._is_sdp_meta(detail))

    def test_get_pipeline_response_tag_empty_string(self):
        # Empty / whitespace-only tag values must NOT match — they signal
        # an unset tag, not a positive identification.
        for v in ["", "   ", "\t"]:
            with self.subTest(value=repr(v)):
                detail = _get_pipeline_response(tags={"sdp_meta": v})
                self.assertFalse(pipelines_mod._is_sdp_meta(detail))

    # ── Config-key fallback (pre-tag pipelines) ────────────────────

    def test_get_pipeline_response_with_sdp_meta_config_keys(self):
        detail = _get_pipeline_response(configuration={
            "bronze.dataflowspecTable": "x.y.bronze_dataflowspec",
            "silver.dataflowspecTable": "x.y.silver_dataflowspec",
            "bronze.group": "A1",
            "silver.group": "A1",
        })
        self.assertTrue(pipelines_mod._is_sdp_meta(detail))

    def test_get_pipeline_response_with_only_one_sdp_meta_config_key(self):
        # A single config key is enough — covers half-configured pipelines.
        detail = _get_pipeline_response(configuration={
            "bronze.dataflowspecTable": "x.y.bronze_dataflowspec",
        })
        self.assertTrue(pipelines_mod._is_sdp_meta(detail))

    def test_get_pipeline_response_with_unrelated_config_keys(self):
        detail = _get_pipeline_response(configuration={
            "spark.databricks.foo": "bar",
            "pipelines.something": "else",
        })
        self.assertFalse(pipelines_mod._is_sdp_meta(detail))

    # ── PipelineStateInfo (list_pipelines summary) ────────────────

    def test_pipeline_state_info_returns_false(self):
        # Summary objects have no tags or configuration; the route's
        # behaviour is to call .get() to enrich, but if that fails the
        # fallback hits this path. False is the safe default — we don't
        # want false positives based purely on the pipeline name.
        self.assertFalse(pipelines_mod._is_sdp_meta(_state_info()))

    # ── Spec-flattened shape (user-pasted JSON / legacy tests) ────

    def test_spec_flattened_dict_envelope(self):
        # If a future SDK version flattens spec onto the top-level object,
        # _spec_of falls back to the object itself.
        flat = SimpleNamespace(
            tags={"sdp_meta": "true"},
            configuration={"bronze.group": "A1"},
        )
        self.assertTrue(pipelines_mod._is_sdp_meta(flat))

    # ── Defensive cases ──────────────────────────────────────────

    def test_tags_none_does_not_crash(self):
        detail = _get_pipeline_response(tags=None, configuration=None)
        self.assertFalse(pipelines_mod._is_sdp_meta(detail))

    def test_tags_wrong_type_does_not_crash(self):
        # Some downstream tooling may surface tags as a string or list.
        # The filter should treat anything that isn't a dict as "no tag".
        detail = SimpleNamespace(spec=SimpleNamespace(
            tags="sdp_meta=true",     # wrong type
            configuration={},
        ))
        self.assertFalse(pipelines_mod._is_sdp_meta(detail))

    def test_config_wrong_type_does_not_crash(self):
        detail = SimpleNamespace(spec=SimpleNamespace(
            tags={},
            configuration=[("bronze.group", "A1")],   # list, not dict
        ))
        self.assertFalse(pipelines_mod._is_sdp_meta(detail))

    def test_spec_is_none(self):
        # Some SDK versions return GetPipelineResponse with spec=None when
        # the caller has list-only permissions. Should not crash.
        detail = SimpleNamespace(spec=None, tags=None, configuration=None)
        self.assertFalse(pipelines_mod._is_sdp_meta(detail))


class SdpMetaVersionExtractionTests(unittest.TestCase):
    """Tests for ``_sdp_meta_version`` \u2014 the helper that resolves which
    SDP-META release created a given pipeline.

    Resolution order:
      1. ``sdp_meta`` tag if non-empty AND not the legacy ``"true"``
         sentinel \u2014 returned verbatim (e.g. ``"0.1.0"``).
      2. ``configuration["version"]`` fallback \u2014 covers pipelines
         tagged with the legacy ``"true"`` sentinel, since SDP-META has
         written ``configuration["version"]`` since the very first release.
      3. ``None`` if neither is available.
    """

    def test_version_string_tag_is_returned_verbatim(self):
        detail = _get_pipeline_response(tags={"sdp_meta": "0.1.0"})
        self.assertEqual(pipelines_mod._sdp_meta_version(detail), "0.1.0")

    def test_legacy_true_tag_falls_back_to_configuration_version(self):
        detail = _get_pipeline_response(
            tags={"sdp_meta": "true"},
            configuration={"version": "0.0.10"},
        )
        self.assertEqual(pipelines_mod._sdp_meta_version(detail), "0.0.10")

    def test_legacy_true_tag_without_configuration_version_returns_none(self):
        # The pipeline is recognised as SDP-META, but we have no way to
        # tell which version created it. None is the honest answer; the
        # UI should render nothing rather than a misleading "v1.0.0".
        detail = _get_pipeline_response(tags={"sdp_meta": "true"})
        self.assertIsNone(pipelines_mod._sdp_meta_version(detail))

    def test_no_tag_falls_back_to_configuration_version(self):
        # Pipeline detected via the config-key fallback path \u2014 surface
        # whatever version was recorded in configuration.
        detail = _get_pipeline_response(configuration={
            "bronze.dataflowspecTable": "x.y.b",
            "version": "0.0.9",
        })
        self.assertEqual(pipelines_mod._sdp_meta_version(detail), "0.0.9")

    def test_no_tag_no_config_version_returns_none(self):
        detail = _get_pipeline_response(configuration={
            "bronze.dataflowspecTable": "x.y.b",
        })
        self.assertIsNone(pipelines_mod._sdp_meta_version(detail))

    def test_tag_takes_priority_over_configuration_version(self):
        # If both are present and the tag is a real version, the tag wins
        # \u2014 the tag is the canonical signal; configuration["version"]
        # is a legacy bookkeeping field.
        detail = _get_pipeline_response(
            tags={"sdp_meta": "0.2.0"},
            configuration={"version": "0.1.0"},
        )
        self.assertEqual(pipelines_mod._sdp_meta_version(detail), "0.2.0")

    def test_whitespace_only_tag_value_returns_none(self):
        detail = _get_pipeline_response(
            tags={"sdp_meta": "   "},
            configuration={"version": "0.1.0"},
        )
        # Tag is treated as unset \u2014 fall back to configuration.
        self.assertEqual(pipelines_mod._sdp_meta_version(detail), "0.1.0")


class WorkspacePipelineUrlTests(unittest.TestCase):
    """Tests for ``_workspace_pipeline_url`` \u2014 the helper that constructs
    the deep link from the Monitor table into the Databricks pipeline UI."""

    def test_happy_path(self):
        url = pipelines_mod._workspace_pipeline_url(
            "https://example.cloud.databricks.com", "pid-1")
        self.assertEqual(url, "https://example.cloud.databricks.com/pipelines/pid-1")

    def test_trailing_slash_on_host_is_normalised(self):
        url = pipelines_mod._workspace_pipeline_url(
            "https://example.cloud.databricks.com/", "pid-1")
        self.assertEqual(url, "https://example.cloud.databricks.com/pipelines/pid-1")

    def test_multiple_trailing_slashes_are_normalised(self):
        url = pipelines_mod._workspace_pipeline_url(
            "https://example.cloud.databricks.com///", "pid-1")
        self.assertEqual(url, "https://example.cloud.databricks.com/pipelines/pid-1")

    def test_missing_host_returns_none(self):
        self.assertIsNone(pipelines_mod._workspace_pipeline_url(None, "pid-1"))
        self.assertIsNone(pipelines_mod._workspace_pipeline_url("", "pid-1"))
        # Host that is only slashes also resolves to None \u2014 rstrip empties it.
        self.assertIsNone(pipelines_mod._workspace_pipeline_url("///", "pid-1"))

    def test_missing_pipeline_id_returns_none(self):
        self.assertIsNone(pipelines_mod._workspace_pipeline_url(
            "https://example.cloud.databricks.com", None))
        self.assertIsNone(pipelines_mod._workspace_pipeline_url(
            "https://example.cloud.databricks.com", ""))


class ListPipelinesEndpointTests(unittest.TestCase):
    """End-to-end Flask client tests against ``/api/pipelines``.

    Verifies that the user-reported pipeline ("sdp_meta_app_car_demo_pipeline")
    survives the filter and reaches the response body. This is the
    regression guard for "Monitor shows nothing despite a running pipeline".
    """

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    _FAKE_HOST = "https://example-workspace.cloud.databricks.com"

    def _make_ws_mock(self, state_info, get_response, host=_FAKE_HOST):
        """Build a mocked WorkspaceClient where list yields one summary
        and .get returns the matching detail object.

        ``config.host`` is populated by default so the route can build the
        pipeline_url field. Pass ``host=None`` to exercise the missing-host
        fallback path.
        """
        ws = mock.MagicMock()
        ws.config.host = host
        ws.pipelines.list_pipelines.return_value = iter([state_info])
        ws.pipelines.get.return_value = get_response
        return ws

    def test_user_reported_pipeline_appears_in_response(self):
        # Mirrors the exact shape the user reported, updated for the
        # version-string tag format (sdp_meta=<version>). All fields are
        # nested under .spec just like ws.pipelines.get returns.
        summary = SimpleNamespace(
            pipeline_id="f3390fab-eaa7-4f9f-9450-e975a3eaa72f",
            name="sdp_meta_app_car_demo_pipeline",
            state=SimpleNamespace(value="IDLE"),
        )
        detail = SimpleNamespace(
            pipeline_id="f3390fab-eaa7-4f9f-9450-e975a3eaa72f",
            creator_user_name="sp-app@example.com",
            last_modified=1700000000000,
            spec=SimpleNamespace(
                tags={"sdp_meta": "0.1.0"},
                configuration={
                    "layer": "bronze_silver",
                    "bronze.group": "A1",
                    "bronze.dataflowspecTable":
                        "sdp_meta.sdp_meta_app_car_demo.bronze_dataflowspec",
                    "silver.group": "A1",
                    "silver.dataflowspecTable":
                        "sdp_meta.sdp_meta_app_car_demo.silver_dataflowspec",
                    "version": "0.1.0",
                },
            ),
        )
        ws = self._make_ws_mock(summary, detail)
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        self.assertEqual(resp.status_code, 200, resp.data)
        body = resp.get_json()
        self.assertIsInstance(body, list, body)
        self.assertEqual(len(body), 1, body)
        row = body[0]
        self.assertEqual(row["id"], "f3390fab-eaa7-4f9f-9450-e975a3eaa72f")
        self.assertEqual(row["name"], "sdp_meta_app_car_demo_pipeline")
        # sdp_meta_config must be sourced from .spec.configuration, not the
        # always-empty top-level configuration. This catches the same class
        # of bug as the _is_sdp_meta nesting issue.
        self.assertIn("bronze.dataflowspecTable", row["sdp_meta_config"])
        self.assertEqual(row["state"], "IDLE")
        # Version chip data — must come from the tag, not from
        # configuration["version"] when the tag is a real version.
        self.assertEqual(row["sdp_meta_version"], "0.1.0")
        # Pipeline URL — built from ws.config.host + /pipelines/<id>. Lets
        # the user click through from the Monitor table directly into the
        # Databricks pipeline UI (new tab).
        self.assertEqual(
            row["pipeline_url"],
            f"{self._FAKE_HOST}/pipelines/f3390fab-eaa7-4f9f-9450-e975a3eaa72f",
        )

    def test_legacy_tag_pipeline_surfaces_version_from_configuration(self):
        # Pipelines created BEFORE the version-tag change (i.e. tagged with
        # the legacy "true" sentinel) must still appear in Monitor, AND the
        # UI must be able to surface a version — pulled from configuration.
        summary = SimpleNamespace(
            pipeline_id="legacy-1",
            name="legacy_sdp_meta_pipeline",
            state=SimpleNamespace(value="IDLE"),
        )
        detail = SimpleNamespace(
            pipeline_id="legacy-1",
            creator_user_name="someone@example.com",
            last_modified=1700000000000,
            spec=SimpleNamespace(
                tags={"sdp_meta": "true"},
                configuration={
                    "bronze.group": "A1",
                    "version": "0.0.10",
                },
            ),
        )
        ws = self._make_ws_mock(summary, detail)
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        body = resp.get_json()
        self.assertEqual(len(body), 1, body)
        self.assertEqual(body[0]["sdp_meta_version"], "0.0.10")

    def test_non_sdp_meta_pipeline_is_filtered_out(self):
        summary = SimpleNamespace(
            pipeline_id="other-1",
            name="random_user_pipeline",
            state=SimpleNamespace(value="RUNNING"),
        )
        detail = SimpleNamespace(
            pipeline_id="other-1",
            creator_user_name="someone@example.com",
            last_modified=1700000000000,
            spec=SimpleNamespace(tags={}, configuration={"some.key": "value"}),
        )
        ws = self._make_ws_mock(summary, detail)
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(resp.get_json(), [])

    def test_pipeline_url_strips_trailing_slash_from_host(self):
        # ws.config.host may or may not carry a trailing slash depending on
        # how the workspace profile was configured. The URL builder must
        # produce exactly one slash between host and "/pipelines/<id>" \u2014
        # otherwise the browser hits a 404.
        summary = SimpleNamespace(
            pipeline_id="pid-trim",
            name="trim_test",
            state=SimpleNamespace(value="IDLE"),
        )
        detail = SimpleNamespace(
            pipeline_id="pid-trim",
            creator_user_name="me@example.com",
            last_modified=0,
            spec=SimpleNamespace(tags={"sdp_meta": "0.1.0"}, configuration={}),
        )
        ws = self._make_ws_mock(summary, detail,
                                host="https://example-workspace.cloud.databricks.com/")
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        body = resp.get_json()
        self.assertEqual(len(body), 1, body)
        self.assertEqual(
            body[0]["pipeline_url"],
            "https://example-workspace.cloud.databricks.com/pipelines/pid-trim",
        )

    def test_pipeline_url_is_none_when_host_missing(self):
        # If ws.config.host can't be resolved (rare \u2014 only happens with
        # exotic auth configs or test-time mocks), pipeline_url must be None
        # so the frontend renders the legacy in-app-events click target
        # instead of a broken external link.
        summary = SimpleNamespace(
            pipeline_id="pid-no-host",
            name="no_host_test",
            state=SimpleNamespace(value="IDLE"),
        )
        detail = SimpleNamespace(
            pipeline_id="pid-no-host",
            creator_user_name="me@example.com",
            last_modified=0,
            spec=SimpleNamespace(tags={"sdp_meta": "0.1.0"}, configuration={}),
        )
        ws = self._make_ws_mock(summary, detail, host=None)
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        body = resp.get_json()
        self.assertEqual(len(body), 1, body)
        self.assertIsNone(body[0]["pipeline_url"])

    def test_get_failure_falls_back_to_summary_and_filters_out(self):
        # When ws.pipelines.get raises (e.g. permission denied), the
        # current code falls back to the PipelineStateInfo summary — which
        # has no tags or configuration — so the pipeline must NOT slip
        # through the filter on the basis of name alone.
        summary = SimpleNamespace(
            pipeline_id="pid-x",
            name="sdp_meta_app_pipeline",  # name LOOKS SDP-META but no signal
            state=SimpleNamespace(value="IDLE"),
        )
        ws = mock.MagicMock()
        ws.pipelines.list_pipelines.return_value = iter([summary])
        ws.pipelines.get.side_effect = RuntimeError("permission denied")
        with mock.patch("databricks.sdk.WorkspaceClient", return_value=ws):
            resp = self.client.get("/api/pipelines")
        self.assertEqual(resp.status_code, 200, resp.data)
        self.assertEqual(resp.get_json(), [])


if __name__ == "__main__":
    unittest.main()
