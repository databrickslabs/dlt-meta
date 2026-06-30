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

    # Maximum seconds with no output before we treat the child as
    # hung. Pulled out so it shows up in stack traces and so a test
    # can monkey-patch it instead of waiting 10 minutes.
    _IDLE_TIMEOUT_S = 600
    # Graceful-shutdown grace period after ``terminate()`` before we
    # escalate to ``kill()``. CLI shells out to ``pip wheel`` which
    # can take a few seconds to unwind cleanly.
    _TERMINATE_GRACE_S = 10

    def _run():
        proc: subprocess.Popen | None = None
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
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

            # Read-loop is wrapped so that a queue timeout (child has
            # gone silent for ``_IDLE_TIMEOUT_S``) flips us into the
            # reap path WITHOUT discarding the lines we already
            # collected. The previous code re-raised through the
            # outer ``except Exception`` which clobbered ``stdout`` /
            # ``stderr`` and never called ``proc.wait()`` \u2014 leaking
            # the child as a zombie until the worker process exited.
            done_count = 0
            timed_out = False
            try:
                while done_count < 2:
                    stream, line = q.get(timeout=_IDLE_TIMEOUT_S)
                    if line is None:
                        done_count += 1
                        continue
                    job['logs'].append({'stream': stream, 'line': line})
                    if stream == 'stdout':
                        stdout_parts.append(line)
                    else:
                        stderr_parts.append(line)
            except _queue_module.Empty:
                timed_out = True
                logger.warning(
                    "Subprocess silent for %ds; terminating PID %s",
                    _IDLE_TIMEOUT_S,
                    getattr(proc, 'pid', '?'),
                )
                job['error'] = (
                    f"Subprocess produced no output for {_IDLE_TIMEOUT_S} "
                    f"seconds; terminated to avoid leaking a zombie process. "
                    f"Any output collected before the timeout is preserved "
                    f"below."
                )

            # Either the readers drained both pipes (success path) or
            # we timed out and are about to escalate in ``finally``.
            # Calling ``wait()`` here for the success path lets us
            # populate ``returncode`` from the natural exit; the
            # timeout path will instead see returncode from
            # ``terminate`` + ``wait`` below.
            if not timed_out:
                proc.wait()
            job['stdout'] = '\n'.join(stdout_parts)
            job['stderr'] = '\n'.join(stderr_parts)
            job['returncode'] = proc.returncode
        except Exception as exc:
            logger.exception("Background CLI subprocess thread failed")
            # Preserve the partial output we collected before the
            # exception so the UI can show whatever progress the CLI
            # made before it crashed. The job dict is pre-populated
            # with ``stdout=''`` / ``stderr=''`` / ``error=None`` by
            # ``_new_job_token``, so ``setdefault`` would be a no-op
            # \u2014 we only skip overwriting when a meaningful value
            # is already present (i.e. the success path got far
            # enough to set them).
            if not job.get('stdout'):
                job['stdout'] = '\n'.join(stdout_parts)
            if not job.get('stderr'):
                job['stderr'] = '\n'.join(stderr_parts)
            if not job.get('error'):
                job['error'] = str(exc)
            job['returncode'] = -1
        finally:
            # Always reap the child, even when the read loop never
            # reached ``proc.wait()`` (timeout) or the Popen call
            # itself raised (proc is None). Without this the child
            # outlives the gunicorn worker as a zombie.
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    try:
                        proc.wait(timeout=_TERMINATE_GRACE_S)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait()
                except OSError:
                    # ProcessLookupError etc. \u2014 child already gone.
                    pass
                # If we had to terminate, refresh returncode from the
                # signal that landed.
                if 'returncode' not in job or job['returncode'] is None:
                    job['returncode'] = proc.returncode
            job['done'] = True
            if cleanup_path and os.path.exists(cleanup_path):
                try:
                    os.unlink(cleanup_path)
                except OSError:
                    pass

    threading.Thread(target=_run, daemon=True).start()
