"""Service-layer helpers for the SDP-META Databricks App.

Pure-Python modules with no Flask dependency, so they can be unit-
tested in isolation and reused across multiple route handlers. The
``app.py`` entrypoint re-exports the privately-named helpers
(``_BUNDLED_DEMO_SPECS``, ``_extract_required_files``, etc.) for
backward compatibility with the existing test suite that patches
through ``app_mod.<name>``.
"""
