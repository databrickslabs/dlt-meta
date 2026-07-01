"""Tests for the App's landing-page rendering.

Currently scoped to the version-string injection contract: the landing
page footer must render the *installed* ``databricks.labs.sdp_meta``
package version, not a hardcoded literal. This pins the regression
where the footer string drifted from ``__about__.__version__`` and
shipped as a stale ``v0.1.0`` long after the package version moved on.

The mechanism under test:

  * ``app.py`` imports ``__version__`` from
    ``databricks.labs.sdp_meta.__about__`` at startup and registers a
    Jinja ``context_processor`` exposing it as ``{{ app_version }}``.
  * ``templates/landingPage.html`` renders ``SDP-META v{{ app_version }}``
    in the sidebar footer.

If either end of that contract regresses (the context processor goes
away, the template re-hardcodes the version, or the import path
changes) these tests fail loudly.
"""

from __future__ import annotations

import os
import sys
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import app as app_mod  # noqa: E402


class LandingPageVersionInjectionTests(unittest.TestCase):
    """``GET /`` must render the installed package's version in the
    sidebar footer."""

    def setUp(self):
        self.client = app_mod.app.test_client()

    def test_footer_renders_installed_package_version(self):
        from databricks.labs.sdp_meta.__about__ import __version__

        resp = self.client.get("/")
        self.assertEqual(resp.status_code, 200)
        html = resp.get_data(as_text=True)
        # The footer string is human-readable, so this assertion
        # doubles as a smoke check that the page actually rendered
        # (and didn't fall back to a Flask 500 HTML error page).
        self.assertIn(f"SDP-META v{__version__}", html)

    def test_footer_does_not_carry_stale_hardcoded_version(self):
        """Regression guard: the previous footer hardcoded
        ``SDP-META v0.1.0``. If the package's real version ever moves
        and someone re-hardcodes the literal, we want to know."""
        from databricks.labs.sdp_meta.__about__ import __version__

        resp = self.client.get("/")
        html = resp.get_data(as_text=True)
        # Only flag the legacy literal when it WOULDN'T match the
        # installed version. If the package happens to still be at
        # 0.1.0 today, both literals coincide and this assertion is
        # trivially true; once the package version moves on, this
        # test fails fast on any future re-hardcoding.
        if __version__ != "0.1.0":
            self.assertNotIn("SDP-META v0.1.0", html)

    def test_context_processor_exposes_app_version(self):
        """The context processor is the single source of truth that
        the template depends on. Reach into it directly so a future
        refactor that breaks the wiring (e.g. removing the processor
        but leaving the template variable) is caught even if the
        landing page itself happens to keep rendering."""
        from databricks.labs.sdp_meta.__about__ import __version__

        with app_mod.app.app_context():
            # Flask exposes context-processor output via
            # ``app.update_template_context`` on an empty dict.
            ctx: dict = {}
            app_mod.app.update_template_context(ctx)
            self.assertIn("app_version", ctx)
            self.assertEqual(ctx["app_version"], __version__)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
