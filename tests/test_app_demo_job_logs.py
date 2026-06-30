"""Tests for ``GET /api/job/<token>/logs`` request validation.

The polling endpoint in ``databricks_app/routes/demo.py`` parses an
``offset`` query parameter via ``int(...)``. A malformed value (non-
numeric string, negative integer) used to bubble out of ``int()`` as a
generic 500 via the global ``handle_exception`` hook in ``app.py`` — the
client got no actionable signal. These tests pin the 400-on-bad-offset
contract in place so future refactors of ``get_job_logs()`` don't
accidentally reintroduce the 500.

They mirror the validation contract enforced for ``limit`` in
``test_app_metadata_browse.py`` so the two endpoints stay consistent.
"""

from __future__ import annotations

import os
import sys
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)
import _jobs as _jobs_module  # noqa: E402


class JobLogsOffsetValidationTests(unittest.TestCase):
    """``offset`` validation runs before any log slicing."""

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()
        # Seed a real job so the 404-on-missing-token short-circuit
        # isn't what's masking the validation behavior.
        self.token = _jobs_module._new_job_token()
        job = _jobs_module._get_job(self.token)
        job['logs'] = [
            {'stream': 'stdout', 'line': 'line-0'},
            {'stream': 'stdout', 'line': 'line-1'},
            {'stream': 'stderr', 'line': 'line-2'},
        ]

    def tearDown(self):
        _jobs_module._jobs.pop(self.token, None)

    def _get(self, query):
        return self.client.get(f"/api/job/{self.token}/logs{query}")

    def test_non_numeric_offset_returns_400(self):
        """A non-numeric ``offset`` must produce a 400, not a 500."""
        resp = self._get("?offset=abc")
        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertIn("offset", payload["error"].lower())
        self.assertIn("integer", payload["error"].lower())

    def test_negative_offset_returns_400(self):
        """Negative ``offset`` would tail-slice the log buffer instead
        of returning the requested forward window."""
        resp = self._get("?offset=-1")
        self.assertEqual(resp.status_code, 400)
        self.assertIn("non-negative", resp.get_json()["error"].lower())

    def test_missing_offset_defaults_to_zero(self):
        """``offset`` is optional; absent means start of the buffer."""
        resp = self._get("")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(len(resp.get_json()["logs"]), 3)

    def test_valid_offset_slices_from_position(self):
        """A well-formed numeric offset returns the trailing slice."""
        resp = self._get("?offset=1")
        self.assertEqual(resp.status_code, 200)
        logs = resp.get_json()["logs"]
        self.assertEqual(len(logs), 2)
        self.assertEqual(logs[0]["line"], "line-1")

    def test_offset_beyond_end_returns_empty(self):
        """An offset past the end of the buffer returns an empty list,
        not an error. This is the steady-state case for polling once
        the client has caught up with the subprocess."""
        resp = self._get("?offset=99")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json()["logs"], [])

    def test_missing_token_returns_404_not_400(self):
        """``token`` lookup runs BEFORE offset validation — a missing
        token should surface as 404, not as an offset-validation 400."""
        resp = self.client.get("/api/job/does-not-exist/logs?offset=abc")
        self.assertEqual(resp.status_code, 404)
        self.assertIn("not found", resp.get_json()["error"].lower())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
