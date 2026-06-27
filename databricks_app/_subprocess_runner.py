"""Run a CLI subprocess in a background thread, streaming stdout / stderr
into a job entry in the ``_jobs`` registry.

Used by ``/onboarding`` and ``/deploy`` so the frontend can poll
``/api/job/<token>/logs`` for incremental output while the CLI runs.

Why a separate module: the launch + reader-thread + queue pattern was
duplicated verbatim in both routes (~60 lines each). Pulling it out
keeps the route bodies focused on payload-building and lets the
streaming machinery be unit-tested independently \u2014 plus it keeps the
behaviour identical across the two routes so a bug fix in one
automatically applies to the other.
"""

from __future__ import annotations

import logging
import os
import queue as _queue_module
import subprocess
import sys
import threading

import _jobs as _jobs_module  # noqa: E402 \u2014 absolute import; databricks_app/ is not a package

logger = logging.getLogger(__name__)


def _run_cli_json_payload(token: str, json_string: str, cwd: str,
                           cleanup_path: str | None = None) -> None:
    """Launch ``python -m databricks.labs.sdp_meta.cli <json>`` in a
    background thread and stream its output into ``_jobs[token]``.

    Arguments:
        token: a job token previously allocated via ``_new_job_token``.
        json_string: the single positional argument the CLI expects.
        cwd: working directory for the subprocess \u2014 typically
            ``_repo_root()`` so demo scripts can resolve relative paths.
        cleanup_path: optional path to ``os.unlink`` after the
            subprocess completes \u2014 used to clean up the tempfile that
            ``_resolve_local_onboarding_path`` may have created when the
            user pointed at a UC Volume / DBFS spec.

    Returns immediately after spawning the background thread; the
    caller is expected to return the token to the client and poll.
    """
    job = _jobs_module._jobs[token]

    def _run():
        try:
            _env = {**os.environ, 'PYTHONUNBUFFERED': '1'}
            proc = subprocess.Popen(
                [sys.executable, '-m', 'databricks.labs.sdp_meta.cli', json_string],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,          # line-buffered on our side
                env=_env,           # force child Python to flush every print()
                cwd=cwd,
            )
            q: _queue_module.Queue = _queue_module.Queue()

            def _reader(pipe, stream_name):
                for line in pipe:
                    q.put((stream_name, line.rstrip('\n')))
                q.put((stream_name, None))  # sentinel

            t1 = threading.Thread(target=_reader, args=(proc.stdout, 'stdout'), daemon=True)
            t2 = threading.Thread(target=_reader, args=(proc.stderr, 'stderr'), daemon=True)
            t1.start()
            t2.start()

            stdout_parts: list = []
            stderr_parts: list = []
            done_count = 0
            while done_count < 2:
                stream, line = q.get(timeout=600)
                if line is None:
                    done_count += 1
                    continue
                job['logs'].append({'stream': stream, 'line': line})
                if stream == 'stdout':
                    stdout_parts.append(line)
                else:
                    stderr_parts.append(line)

            proc.wait()
            job['stdout'] = '\n'.join(stdout_parts)
            job['stderr'] = '\n'.join(stderr_parts)
            job['returncode'] = proc.returncode
        except Exception as exc:
            logger.exception("Background CLI subprocess thread failed")
            job['error'] = str(exc)
            job['returncode'] = -1
        finally:
            job['done'] = True
            if cleanup_path and os.path.exists(cleanup_path):
                try:
                    os.unlink(cleanup_path)
                except OSError:
                    pass

    threading.Thread(target=_run, daemon=True).start()
