"""Tests for ``/api/metadata/table-data`` request validation.

Background: the handler in ``databricks_app/routes/metadata_browse.py``
parses the request body up front (catalog / schema / table / limit) and
then opens a try/except around the SQL execution. The ``int(limit)``
conversion lives OUTSIDE that try/except, so a malformed ``limit``
(non-numeric string, ``null``, zero, negative) used to surface as a
generic 500 via the global ``handle_exception`` hook in ``app.py``
instead of a proper 400 with a client-readable message.

These tests pin the 400-on-bad-limit contract in place so future
refactors of ``table_data()`` don't accidentally reintroduce the 500.
"""

from __future__ import annotations

import json
import os
import sys
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402  (deliberate post-sys.path-insert import)


class TableDataLimitValidationTests(unittest.TestCase):
    """``limit`` validation runs before any SDK / SQL work."""

    def setUp(self):
        app_mod.app.testing = True
        self.client = app_mod.app.test_client()

    def _post(self, body):
        return self.client.post(
            "/api/metadata/table-data",
            data=json.dumps(body),
            content_type="application/json",
        )

    def test_non_numeric_limit_returns_400(self):
        """A non-numeric ``limit`` must produce a 400, not a 500."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": "abc",
        })
        self.assertEqual(resp.status_code, 400)
        payload = resp.get_json()
        self.assertIn("limit", payload["error"].lower())
        self.assertIn("integer", payload["error"].lower())

    def test_null_limit_returns_400(self):
        """``limit: null`` (TypeError on ``int(None)``) must produce a 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": None,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("limit", resp.get_json()["error"].lower())

    def test_zero_limit_returns_400(self):
        """``LIMIT 0`` is a degenerate query; reject with 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": 0,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("positive", resp.get_json()["error"].lower())

    def test_negative_limit_returns_400(self):
        """Negative ``limit`` would yield invalid SQL; reject with 400."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": -5,
        })
        self.assertEqual(resp.status_code, 400)
        self.assertIn("positive", resp.get_json()["error"].lower())

    def test_valid_limit_passes_validation(self):
        """A well-formed numeric limit must NOT be rejected by limit
        validation. We deliberately omit a configured warehouse so the
        handler short-circuits at the warehouse check (also a 400, but
        with a different error string) — that's how we know the limit
        check passed without having to mock the SDK."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": 50,
        })
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("limit must", err)
        self.assertIn("warehouse", err)

    def test_string_numeric_limit_is_accepted(self):
        """JSON clients sometimes send numbers as strings; ``int('50')``
        works fine, so this is NOT a limit-validation failure. Same
        warehouse-short-circuit assertion as the valid-limit case."""
        resp = self._post({
            "catalog": "c", "schema": "s", "table": "t", "limit": "50",
        })
        self.assertEqual(resp.status_code, 400)
        err = resp.get_json()["error"].lower()
        self.assertNotIn("limit must", err)
        self.assertIn("warehouse", err)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
