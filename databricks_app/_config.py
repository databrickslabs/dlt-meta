"""App-wide configuration: repo-root resolution + warehouse globals.

The App container's layout depends on how it's deployed:

  Mode A \u2014 full repo deployed (recommended):
    Container layout:
      /app/python/source_code/          \u2190 repo root
        setup.py, src/, demo/, integration_tests/, databricks_app/

  Mode B \u2014 only databricks_app/ deployed (legacy):
    start.sh clones the full repo to /tmp/sdp-meta and uses that.

``start.sh`` exports ``SDP_META_HOME`` so ``_repo_root()`` picks it up
regardless of which mode the App is in.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


# Runtime warehouse override \u2014 set via ``/api/warehouse/configure``.
# Survives across requests within a single app process; cleared on
# restart. Takes priority over the ``DATABRICKS_SQL_WAREHOUSE_ID`` env var.
_runtime_warehouse_id: str = ""


def _get_warehouse_id() -> str:
    """Return the active warehouse ID: runtime override \u2192 env var \u2192 empty string."""
    return (_runtime_warehouse_id or os.environ.get('DATABRICKS_SQL_WAREHOUSE_ID', '')).strip()


def _set_runtime_warehouse_id(value: str) -> None:
    """Set the process-global runtime warehouse override. Called by
    ``/api/warehouse/configure``; isolated here so route modules don't
    need to ``global``-declare a state variable they don't own."""
    global _runtime_warehouse_id
    _runtime_warehouse_id = value


def _repo_root() -> str:
    """Return the sdp-meta repo root (no trailing slash).

    Resolution order:
      1. ``SDP_META_HOME`` env var \u2014 explicit override for non-standard
         layouts (set by ``start.sh``).
      2. ``__file__`` \u2014 one directory up from ``databricks_app/``.
    """
    override = os.environ.get('SDP_META_HOME', '').strip().rstrip('/')
    if override:
        logger.info("SDP_META_HOME override: %s", override)
        return override

    # _config.py lives in databricks_app/, parent is the repo root
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info("Repo root derived from __file__: %s", root)

    # Warn loudly if expected directories are absent so the log is actionable
    for expected in ('demo', 'integration_tests', 'src'):
        if not os.path.isdir(os.path.join(root, expected)):
            logger.warning(
                "Expected directory '%s/' not found under repo root '%s'. "
                "Make sure the full sdp-meta repo was deployed (not just databricks_app/).",
                expected, root,
            )
    return root
