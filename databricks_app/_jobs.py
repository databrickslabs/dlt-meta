"""Process-global background-job store.

Keyed by a hex token returned to the client when a long-running
subprocess (onboarding, deploy) starts in a background thread. The
``/api/job/<token>/logs`` endpoint polls each entry until ``done`` is
true.

Each entry has the shape:
  {
    'logs': [{'stream': 'stdout'|'stderr', 'line': str}, ...],
    'done': bool,
    'returncode': int | None,
    'stdout': str,           # full stdout joined when done
    'stderr': str,           # full stderr joined when done
    'modal_content': dict | None,
    'error': str | None,     # set when the background thread itself crashes
  }
"""

from __future__ import annotations

import uuid


_jobs: dict = {}


def _new_job_token() -> str:
    """Allocate a fresh job-token entry in ``_jobs`` and return the token."""
    token = uuid.uuid4().hex
    _jobs[token] = {
        'logs': [],
        'done': False,
        'returncode': None,
        'stdout': '',
        'stderr': '',
        'modal_content': None,
        'error': None,
    }
    return token


def _get_job(token: str):
    """Look up a job entry by token, returning ``None`` if absent."""
    return _jobs.get(token)
