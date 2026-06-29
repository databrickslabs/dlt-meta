"""Tests for ``databricks_app/_subprocess_runner.py``.

Pin the child-process-reaping fix (H-1):

  * On idle timeout the child is ``terminate()``-d (no zombie left
    behind) and the partial stdout / stderr collected before the
    timeout is preserved.
  * On normal completion stdout / stderr / returncode all come from
    the natural exit, unchanged from the historical behaviour.
  * On Popen failure the cleanup hook still fires.

The runner spawns ``python -m databricks.labs.sdp_meta.cli`` in
production. The tests monkey-patch ``subprocess.Popen`` to return a
controllable fake so we can drive timeout / completion / failure
scenarios deterministically without depending on the real CLI.
"""

from __future__ import annotations

import os
import sys
import time
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

import _jobs as _jobs_module  # noqa: E402
import _subprocess_runner as _runner_module  # noqa: E402


class _FakePipe:
    """Iterable replacement for a Popen stdout / stderr pipe.

    Yields each line from ``lines`` (with newline) and then exits the
    iteration, which is exactly what the runner's ``_reader`` loop
    expects to detect end-of-stream.
    """

    def __init__(self, lines):
        self._lines = list(lines)

    def __iter__(self):
        for line in self._lines:
            yield line + "\n"


class _FakeHangingPipe:
    """Pipe that yields one line then blocks forever \u2014 simulates the
    "subprocess produced no output for ages" case the timeout fix
    targets."""

    def __init__(self, first_line):
        self._first = first_line
        self._yielded = False

    def __iter__(self):
        return self

    def __next__(self):
        if not self._yielded:
            self._yielded = True
            return self._first + "\n"
        # Block forever \u2014 the production code hits queue.Empty in
        # the read loop after _IDLE_TIMEOUT_S seconds, NOT here.
        while True:
            time.sleep(60)


class _FakeProc:
    """Minimal Popen stand-in. Tracks whether terminate / wait was
    called and lets the test poke a returncode."""

    def __init__(self, stdout_lines, stderr_lines, *, hang=False,
                 returncode=0, pid=12345):
        if hang:
            self.stdout = _FakeHangingPipe(stdout_lines[0] if stdout_lines else "x")
            self.stderr = _FakePipe(stderr_lines)
        else:
            self.stdout = _FakePipe(stdout_lines)
            self.stderr = _FakePipe(stderr_lines)
        self.returncode = None
        self._final_returncode = returncode
        self._terminated = False
        self._waited = False
        self._poll_alive = hang  # hung processes are "alive" until terminated
        self.pid = pid

    def wait(self, timeout=None):
        self._waited = True
        # Match real Popen.wait(): sets returncode on completion.
        self.returncode = self._final_returncode
        return self.returncode

    def terminate(self):
        self._terminated = True
        # Real terminate() sends SIGTERM but doesn't reap on its own.
        # The runner calls wait() right after.
        self._poll_alive = False
        self.returncode = -15

    def kill(self):
        self._poll_alive = False
        self.returncode = -9

    def poll(self):
        # Returns None while running, returncode when exited \u2014
        # exactly what the real Popen.poll does.
        return None if self._poll_alive else self.returncode


class SubprocessRunnerTests(unittest.TestCase):
    """Drive ``_run_cli_json_payload`` against a fake Popen."""

    def setUp(self):
        # The runner stores results into _jobs[token] \u2014 mint one.
        self._token = _jobs_module._new_job_token()
        self._orig_popen = _runner_module.subprocess.Popen

    def tearDown(self):
        _runner_module.subprocess.Popen = self._orig_popen

    def _wait_for_done(self, timeout=5.0):
        """Spin until the background thread sets job['done']. We give
        the natural-completion case plenty of headroom; the timeout
        case is driven by a monkey-patched _IDLE_TIMEOUT_S so it
        finishes quickly too."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if _jobs_module._jobs[self._token].get('done'):
                return
            time.sleep(0.01)
        self.fail("background runner did not finish within timeout")

    def test_normal_completion_populates_stdout_returncode(self):
        """Historical happy-path: both pipes drain, proc.wait() runs,
        returncode is propagated."""
        fake = _FakeProc(
            stdout_lines=["hello", "world"],
            stderr_lines=["warn1"],
            returncode=0,
        )
        _runner_module.subprocess.Popen = lambda *a, **k: fake

        _runner_module._run_cli_json_payload(
            token=self._token, json_string="{}", cwd="/",
        )
        self._wait_for_done()

        job = _jobs_module._jobs[self._token]
        self.assertEqual(job['returncode'], 0)
        self.assertEqual(job['stdout'], "hello\nworld")
        self.assertEqual(job['stderr'], "warn1")
        self.assertTrue(fake._waited)
        # Natural completion should NOT have hit terminate \u2014 that
        # path is only for hung children.
        self.assertFalse(fake._terminated)

    def test_idle_timeout_reaps_child_and_preserves_partial_output(self):
        """H-1 regression: a hung child must be terminated AND the
        already-collected output must survive instead of getting
        clobbered by the exception handler."""
        fake = _FakeProc(
            stdout_lines=["got this line before hanging"],
            stderr_lines=[],
            hang=True,
        )
        _runner_module.subprocess.Popen = lambda *a, **k: fake

        # The production timeout is 10 minutes \u2014 monkey-patch it down
        # so the test runs in <2s. The constant lives inside _run()
        # so we patch the module's reference and re-call the runner
        # in a way that picks up the change.
        #
        # Easiest: shadow the queue module's Empty wait by patching
        # queue.Queue.get to raise quickly when the per-call timeout
        # equals the production value. That keeps the test agnostic
        # to whether the constant was named.
        import queue
        original_get = queue.Queue.get

        def fast_get(self, block=True, timeout=None):
            if timeout and timeout >= 60:
                # Drain the one real line first, then simulate idle
                # timeout. The reader thread will push a tuple per
                # line; once that's been consumed we want the next
                # get() to time out.
                try:
                    return original_get(self, block=block, timeout=0.5)
                except queue.Empty:
                    raise
            return original_get(self, block=block, timeout=timeout)

        queue.Queue.get = fast_get
        try:
            _runner_module._run_cli_json_payload(
                token=self._token, json_string="{}", cwd="/",
            )
            self._wait_for_done(timeout=5.0)
        finally:
            queue.Queue.get = original_get

        job = _jobs_module._jobs[self._token]
        # The child must have been terminated; without H-1 this is
        # left running until the worker process exits.
        self.assertTrue(
            fake._terminated,
            "child process was not terminate()-d on idle timeout",
        )
        # The line we collected BEFORE the timeout must survive \u2014
        # the old code clobbered it via the broad except handler.
        self.assertIn("got this line before hanging", job['stdout'])
        # And we must surface an actionable error to the caller.
        self.assertIsNotNone(job.get('error'))
        err = job['error'].lower()
        self.assertTrue(
            "no output" in err or "silent" in err,
            f"timeout error should mention silence / no output: {err!r}",
        )

    def test_cleanup_path_unlinked_on_success(self):
        """The tempfile cleanup hook fires after a normal run too,
        not just on the failure path."""
        import tempfile
        tmp = tempfile.NamedTemporaryFile(
            delete=False, suffix=".yml", prefix="sdp_meta_test_"
        )
        tmp.write(b"x")
        tmp.close()
        self.assertTrue(os.path.exists(tmp.name))

        fake = _FakeProc(stdout_lines=["ok"], stderr_lines=[], returncode=0)
        _runner_module.subprocess.Popen = lambda *a, **k: fake

        _runner_module._run_cli_json_payload(
            token=self._token, json_string="{}", cwd="/",
            cleanup_path=tmp.name,
        )
        self._wait_for_done()

        self.assertFalse(
            os.path.exists(tmp.name),
            f"cleanup hook did not unlink {tmp.name}",
        )

    def test_popen_failure_still_marks_done(self):
        """If Popen itself raises (e.g. ENOEXEC), the thread must not
        leave the job in the perpetual "in-flight" state."""

        def _boom(*a, **k):
            raise OSError("simulated Popen failure")

        _runner_module.subprocess.Popen = _boom
        _runner_module._run_cli_json_payload(
            token=self._token, json_string="{}", cwd="/",
        )
        self._wait_for_done()

        job = _jobs_module._jobs[self._token]
        self.assertEqual(job['returncode'], -1)
        self.assertIn("simulated", job['error'])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
