"""Shared pytest configuration for the dlt-meta / sdp-meta unit-test suite.

Two pieces of cross-cutting test hygiene live here:

1. Block ``webbrowser.open`` for the whole test session.

   ``src/databricks/labs/sdp_meta/cli.py`` calls ``webbrowser.open(...)``
   after creating jobs / pipelines so an interactive ``sdp-meta``
   session lands the user on the Databricks UI for the new artifact.
   The CLI tests in ``tests/test_cli.py`` mock :class:`WorkspaceClient`
   but historically did NOT mock the ``webbrowser`` module, so every
   test that exercised those code paths fired ``webbrowser.open`` with
   a URL built from MagicMock attributes — popping a real browser tab
   on every test run.

   ``cli.py`` now routes through ``_maybe_open_url`` which honours
   ``SDP_META_NO_BROWSER=1``. Setting the env var here, at conftest
   import time (which runs *before* any test module is collected),
   guarantees the suppression is in place even for tests that import
   ``cli`` at module-load time.

   Use ``os.environ.setdefault`` so a developer can still opt back in
   for a single run with ``SDP_META_NO_BROWSER=0 pytest tests/...``
   when they want to debug the actual browser-launch behaviour.

2. Belt-and-braces ``$BROWSER`` override.

   Anything we missed routing through ``_maybe_open_url`` (third-party
   libs, future regressions, the SDK's U2M OAuth flow if a test ever
   constructs a real ``WorkspaceClient``) still goes through Python's
   ``webbrowser`` module, which honours the ``$BROWSER`` env var when
   picking the launcher. Pointing it at ``true`` (the Unix no-op
   command) makes the OS layer a silent no-op too. Belt and braces.
"""
from __future__ import annotations

import os

# 1. App-side opt-out flag — read by cli._maybe_open_url.
os.environ.setdefault("SDP_META_NO_BROWSER", "1")

# 2. OS-side fallback — Python's webbrowser module respects $BROWSER.
#    `true` exits 0 immediately and never paints anything to the screen.
os.environ.setdefault("BROWSER", "true")
