"""Auto-loaded compatibility shim for SDP-META demos running inside Databricks Apps.

Why this file exists
--------------------
``integration_tests/run_integration_tests.py::SDPMETARunner.upload_files_to_databricks``
imports each runner via::

    self.ws.workspace.upload(
        path=".../runners/foo.py",
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        content=...,
    )

The SDK's ``workspace.upload`` calls ``/api/2.0/workspace/import`` and
Databricks always stores ``format=SOURCE`` imports as a NOTEBOOK with the
file extension stripped — confirmed by listing a deployed run::

    $ databricks workspace list /Users/<sp>/sdp_meta_demo/<run_id>/runners
    NOTEBOOK  PYTHON  /Users/.../runners/init_sdp_meta_pipeline   # no ".py"
    NOTEBOOK  PYTHON  /Users/.../runners/validate                 # no ".py"

The DLT pipeline / Workflow definitions in the same module reference the
runners WITH ``.py`` (``runners/foo.py``); the platform does an exact path
match on ``notebook_path`` and 404s with::

    Unable to access the notebook ".../runners/foo.py". Either it does not
    exist, or the identity used to run this pipeline lacks the required
    permissions.

We can't make the workspace store the notebook at a ``.py`` suffix — the
runners use notebook-only features (``%pip install``, ``# COMMAND ----------``
cells), so uploading them as workspace files (which the workspace-files
``import-file`` endpoint also rejects when the ``# Databricks notebook
source`` magic header is present) breaks the demos.

The constraint from the user: do NOT modify
``integration_tests/run_integration_tests.py`` or any of the
``demo/launch_*_demo.py`` launchers. Fix it in one centralized place inside
the App's source tree.

How this works
--------------
Python's ``site`` module imports a top-level module called ``sitecustomize``
on every interpreter startup, scanning ``sys.path`` for it. ``app.py`` adds
``databricks_app/`` to ``PYTHONPATH`` for every demo subprocess, so this file
is auto-loaded the moment the demo's ``python demo/launch_*_demo.py``
subprocess starts — before any demo code, before the SDK is imported, and
without touching the demo files.

The shim:

1. Self-checks ``DATABRICKS_APP_PORT`` to be sure we're inside the Apps
   container (the env var the platform injects). Local CLI runs of the
   integration tests are unaffected.
2. Wraps ``WorkspaceClient.pipelines.create`` and
   ``WorkspaceClient.jobs.create`` (via ``WorkspaceClient.__init__``) so any
   ``notebook_path`` / ``NotebookLibrary.path`` ending in ``.py`` has the
   suffix stripped before reaching the platform. The pipeline / job then
   references the actual stored notebook path (no extension), and the
   notebook lookup succeeds.

This is intentionally narrow: only ``.create`` calls on those two APIs are
wrapped, and only when the path string ends with ``.py``. Any other use
of the SDK is untouched.
"""

from __future__ import annotations

import inspect
import os
import sys


def _bound_arg(func, args, kwargs, name):
    """Resolve a parameter by name whether it was passed positionally or by
    keyword. Returns None if absent or if the signature can't be introspected
    (in which case we fall back to the keyword-only lookup at the call site).
    """
    try:
        bound = inspect.signature(func).bind_partial(*args, **kwargs)
        return bound.arguments.get(name)
    except (TypeError, ValueError):
        return kwargs.get(name)


def _strip_py_in_pipeline_libraries(libraries) -> None:
    if not libraries:
        return
    for lib in libraries:
        nb = getattr(lib, "notebook", None)
        if nb is None:
            continue
        path = getattr(nb, "path", None)
        if isinstance(path, str) and path.endswith(".py"):
            new_path = path[:-3]
            print(
                f"[app-sitecustomize] pipelines.create: rewriting notebook "
                f"path '{path}' -> '{new_path}'",
                file=sys.stderr,
            )
            nb.path = new_path


def _strip_py_in_job_tasks(tasks) -> None:
    if not tasks:
        return
    for task in tasks:
        nb_task = getattr(task, "notebook_task", None)
        if nb_task is None:
            continue
        path = getattr(nb_task, "notebook_path", None)
        if isinstance(path, str) and path.endswith(".py"):
            new_path = path[:-3]
            print(
                f"[app-sitecustomize] jobs.create: rewriting notebook_path "
                f"'{path}' -> '{new_path}' "
                f"(task_key={getattr(task, 'task_key', '?')})",
                file=sys.stderr,
            )
            nb_task.notebook_path = new_path


def _install_workspace_client_patch() -> None:
    """Patch ``WorkspaceClient.__init__`` so every constructed client has its
    ``pipelines.create`` and ``jobs.create`` wrapped.

    Patching at ``__init__`` time is more robust than patching the
    ``PipelinesAPI`` / ``JobsAPI`` classes directly: the SDK assigns those
    APIs as instance attributes in ``__init__``, and the per-instance
    callables we replace are simple bound methods.
    """
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        # SDK isn't available in this subprocess (e.g. an unrelated helper
        # script started without the venv). Nothing to patch.
        return

    sentinel = "__app_sitecustomize_patched__"
    if getattr(WorkspaceClient.__init__, sentinel, False):
        return

    original_init = WorkspaceClient.__init__

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)

        pipelines_api = getattr(self, "pipelines", None)
        if pipelines_api is not None and not getattr(
            pipelines_api.create, sentinel, False
        ):
            original_pipelines_create = pipelines_api.create

            def wrapped_pipelines_create(*pa, **pkw):
                _strip_py_in_pipeline_libraries(
                    _bound_arg(original_pipelines_create, pa, pkw, "libraries")
                )
                return original_pipelines_create(*pa, **pkw)

            wrapped_pipelines_create.__app_sitecustomize_patched__ = True  # type: ignore[attr-defined]
            pipelines_api.create = wrapped_pipelines_create  # type: ignore[assignment]

        jobs_api = getattr(self, "jobs", None)
        if jobs_api is not None and not getattr(
            jobs_api.create, sentinel, False
        ):
            original_jobs_create = jobs_api.create

            def wrapped_jobs_create(*ja, **jkw):
                _strip_py_in_job_tasks(
                    _bound_arg(original_jobs_create, ja, jkw, "tasks")
                )
                return original_jobs_create(*ja, **jkw)

            wrapped_jobs_create.__app_sitecustomize_patched__ = True  # type: ignore[attr-defined]
            jobs_api.create = wrapped_jobs_create  # type: ignore[assignment]

    patched_init.__app_sitecustomize_patched__ = True  # type: ignore[attr-defined]
    WorkspaceClient.__init__ = patched_init


# Only activate inside a Databricks Apps container. Outside the App (CLI
# integration tests, local notebook runs), this module is a no-op even if
# it ends up on sys.path somehow.
if os.environ.get("DATABRICKS_APP_PORT"):
    _install_workspace_client_patch()
    print(
        "[app-sitecustomize] installed pipelines/jobs notebook_path .py-strip shim",
        file=sys.stderr,
    )
