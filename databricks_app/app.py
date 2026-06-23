from flask import Flask, render_template, request, jsonify
from werkzeug.exceptions import HTTPException
from dataclasses import asdict
import subprocess
import sys
import os
import logging
import re
import json

# UC identifier validation. The sdp-meta wheel is built and installed by
# databricks_app/start.sh in the App container, so this import succeeds at
# request-handling time. Local-dev runs that haven't installed the wheel
# (`pip install -e .` skipped) get a graceful no-op fallback so /onboarding
# and /deploy still respond instead of 500-ing on import.
try:
    from databricks.labs.sdp_meta.identifiers import validate_uc_identifier
except ImportError:  # pragma: no cover — only hit when the wheel is missing.
    def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
        return name

# UC catalog pre-flight (Apps-SP grants). Lives next to app.py so the
# probe + GRANT SQL builder ship in the same source tree and can be
# imported without any PYTHONPATH gymnastics.
from uc_preflight import check_app_sp_grants_on_catalog  # noqa: E402

# Always log to stdout/stderr (captured by the Apps runtime). Add a file
# handler only if the target path is writable — the App container's working
# directory can be read-only, and a FileHandler that can't open its file
# raises during basicConfig and takes down the whole app at import time.
_log_handlers: list[logging.Handler] = [logging.StreamHandler()]
try:
    _log_file = os.path.join(os.environ.get("TMPDIR", "/tmp"), "dlt-meta-app.log")
    _log_handlers.append(logging.FileHandler(_log_file))
except OSError:
    pass  # read-only FS — stdout/stderr capture is enough.

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=_log_handlers)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def _repo_root() -> str:
    """Return the dlt-meta repo root (no trailing slash).

    On Databricks Apps the container layout after deploying from the repo root is:
        /app/python/source_code/          ← repo root (PYTHONPATH set here by platform)
            setup.py
            app.yaml
            requirements.txt
            src/                          ← sdp-meta package (installed by start.sh)
            demo/                         ← demo scripts
            integration_tests/            ← imported by demo scripts
            databricks_app/
                app.py                    ← this file

    So: os.path.dirname(os.path.dirname(__file__)) == /app/python/source_code/
    which is exactly the repo root where demo/ and integration_tests/ live.

    Resolution order:
    1. DLT_META_HOME env var — explicit override for non-standard layouts.
    2. __file__ — one directory up from databricks_app/.
    """
    override = os.environ.get('DLT_META_HOME', '').strip().rstrip('/')
    if override:
        logger.info("DLT_META_HOME override: %s", override)
        return override

    # app.py lives in databricks_app/, parent is the repo root
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logger.info("Repo root derived from __file__: %s", root)

    # Warn loudly if expected directories are absent so the log is actionable
    for expected in ('demo', 'integration_tests', 'src'):
        if not os.path.isdir(os.path.join(root, expected)):
            logger.warning(
                "Expected directory '%s/' not found under repo root '%s'. "
                "Make sure the full dlt-meta repo was deployed (not just databricks_app/).",
                expected, root,
            )
    return root


@app.errorhandler(Exception)
def handle_exception(exc):
    """Catch all unhandled exceptions and return JSON instead of an HTML 500 page.
    Standard HTTP errors (404, 405, etc.) are passed through normally so Flask
    can return the correct status code without logging them as application errors."""
    if isinstance(exc, HTTPException):
        return exc  # let Flask render the normal HTTP error response
    logger.exception("Unhandled exception in route: %s", exc)
    return jsonify({
        'error': str(exc),
        'stdout': '',
        'stderr': '',
        'returncode': -1,
        'modal_content': None,
    }), 500


@app.after_request
def add_security_headers(response):
    """Attach HTTP security headers to every response (fix M4)."""
    response.headers['Content-Security-Policy'] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "frame-src 'self' *.cloud.databricks.com; "
        "object-src 'none';"
    )
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'SAMEORIGIN'
    response.headers['Referrer-Policy'] = 'strict-origin-when-cross-origin'
    return response


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('landingPage.html')


@app.route('/onboarding', methods=['POST'])
def handle_onboard_form():

    logger.info("onboard details: %s", dict(request.form))
    current_directory = _repo_root()

    uc_enabled = request.form.get('unity_catalog_enabled') == "1"
    uc_name = request.form.get('unity_catalog_name', '')

    # Validate UC identifier at the App boundary so a malformed name surfaces
    # as an actionable 400 instead of a generic ``cli.py`` traceback. The
    # underlying onboarding code splices catalog / schema names into SQL
    # strings unquoted (issue #261), so this is also a defense-in-depth
    # check against SQL-identifier injection via the form. Only enforced
    # when UC is enabled — the non-UC HMS path doesn't take a catalog.
    if uc_enabled and uc_name:
        try:
            validate_uc_identifier(uc_name, kind="unity_catalog_name")
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    # Create JSON object from form data
    json_data = {
        "unity_catalog_enabled": "1" if uc_enabled else "0",
        "unity_catalog_name": uc_name,
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "onboarding_file_path": request.form.get('onboarding_file_path', 'demo/conf/onboarding.template'),
        # Default to the demo/ directory under the repo root that
        # ``_repo_root()`` discovered. The previous hardcoded default
        # ``/app/python/source_code/dlt-meta/demo/`` was wrong for the
        # current Mode A layout (the App mounts the repo at
        # ``/app/python/source_code/`` directly — no ``dlt-meta/`` segment).
        "local_directory": request.form.get(
            'local_directory',
            os.path.join(current_directory, 'demo') + os.sep,
        ),
        "dlt_meta_schema": request.form.get('dlt_meta_schema',
                                            'dlt_meta_dataflowspecs_4e6c360d3e5c4b5ca6687fec8ffe2e14'),
        "bronze_schema": request.form.get('bronze_schema', 'dltmeta_bronze_9c1aa383b36a49198d3e99d25f7180a4'),
        "silver_schema": request.form.get('silver_schema', 'dltmeta_silver_7b4e981029b843c799bf61a0a121b3ca'),
        "dlt_meta_layer": request.form.get('dlt_meta_layer', '1'),
        "bronze_table": request.form.get('bronze_table', 'bronze_dataflowspec'),
        "silver_table": request.form.get('silver_table', 'silver_dataflowspec'),
        "overwrite": "1" if request.form.get('overwrite') == "1" else "0",
        "version": request.form.get('version', 'v1'),
        "environment": request.form.get('environment', 'prod'),
        "author": request.form.get('author', 'app-40zbx9 meta-dlt'),
        "update_paths": "1" if request.form.get('update_paths') == "1" else "0",
        "command": "onboard_ui",
        "flags": {"log_level": "info"},
    }

    json_string = json.dumps(json_data)
    # Fix C2: use argument list with shell=False to prevent shell injection via
    # user-supplied form values. json.dumps does not escape single-quotes, so
    # passing json_string inside a shell-quoted string is exploitable.
    result = subprocess.run(
        [sys.executable, os.path.join(current_directory, 'src', 'cli.py'), json_string],
        shell=False,
        capture_output=True,
        text=True,
    )
    return extract_command_output(result)


@app.route('/deploy', methods=['POST'])
def handle_deploy_form():
    logger.info("deploy details: %s", dict(request.form))
    current_directory = _repo_root()

    uc_enabled = request.form.get('uc_enabled') == "1"
    uc_name = request.form.get('uc_catalog_name', '')

    # Validate UC identifier at the App boundary — same reasoning as
    # /onboarding. Only enforced when UC is enabled.
    if uc_enabled and uc_name:
        try:
            validate_uc_identifier(uc_name, kind="uc_catalog_name")
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 400

    json_data = {
        "uc_enabled": "1" if uc_enabled else "0",
        "uc_catalog_name": uc_name,
        "serverless": "1" if request.form.get('serverless') == "1" else "0",
        "layer": request.form.get('deploylayer', 'bronze'),
        "pipeline_name": request.form.get('pipeline_name', 'dlt_meta_pipeline'),
        "dlt_target_schema": request.form.get("dlt_target_schema"),
        "command": "deploy_ui",
        "flags": {"log_level": "info"},
        "onboard_bronze_group": request.form.get("onboard_bronze_group"),
        "onboard_silver_group": request.form.get("onboard_silver_group"),
        "dlt_meta_schema": request.form.get("spc_schema_name"),
        "bronze_dataflowspec_table": request.form.get("bronze_dataflowspec_table"),
        "dataflowspec_silver_table": request.form.get("silver_dataflowspec_table"),
    }

    json_string = json.dumps(json_data)
    # Fix C2: use argument list with shell=False — same reasoning as /onboarding
    result = subprocess.run(
        [sys.executable, os.path.join(current_directory, 'src', 'cli.py'), json_string],
        shell=False,
        capture_output=True,
        text=True,
    )
    return extract_command_output(result)


@app.route('/check-uc-grants', methods=['GET'])
def check_uc_grants():
    """Probe whether the App SP has the privileges every demo needs on a UC catalog.

    Front-end "Test App access" button calls this to verify a catalog
    BEFORE the user clicks any demo. Returns 200 with the structured
    :class:`uc_preflight.PreflightResult` payload regardless of outcome —
    the ``ok`` flag tells the UI whether to render the success indicator
    or the "Grant required" panel. The same probe runs as a 400-blocker
    inside ``/rundemo`` (see below).
    """
    uc_name = (request.args.get('uc_name') or '').strip()
    if not uc_name:
        return jsonify({'error': 'uc_name query parameter is required'}), 400
    try:
        validate_uc_identifier(uc_name, kind='uc_name')
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    result = check_app_sp_grants_on_catalog(uc_name)
    return jsonify(asdict(result))


@app.route('/rundemo', methods=['POST'])
def run_demo():
    payload = request.get_json(silent=True) or {}
    code_to_run = payload.get('demo_name', '')
    logger.info("processing demo: %s", payload)
    current_directory = _repo_root()
    # Allow-list of demos exposed by /rundemo. Each entry maps the UI
    # ``data-command`` token to the launcher script + how to pass the UC
    # catalog name on its CLI:
    #
    #   ``file``     — path to the launcher (relative to repo root).
    #   ``uc_arg``   — flag the launcher's argparse expects for the UC
    #                  catalog. The legacy ``launch_*_demo.py`` scripts use
    #                  underscored ``--uc_catalog_name``;
    #                  ``launch_interactive_demo.py`` uses hyphenated
    #                  ``--uc-catalog-name`` (issue #261-era convention).
    #   ``extra_args`` — fixed positional/flag args appended after the UC
    #                  catalog. Used for the interactive demo so it returns
    #                  promptly with the run URL instead of blocking the
    #                  Flask request for the default 90-minute job timeout.
    #
    # Removed demos:
    #
    #   - ``demo_dabs`` (generate_dabs_resources.py) — shells out to
    #     ``databricks bundle deploy/run`` which uses Terraform under the
    #     hood. The Apps container ships only the SDK + CLI + sdp-meta
    #     wheel; pulling in Terraform for one demo isn't worth the
    #     runtime weight. Use the bundle CLI from a local terminal.
    #   - ``demo_dlt_sink`` (launch_dlt_sink_demo.py) — requires Kafka /
    #     Event Hubs source + secret-scope wiring that isn't available
    #     to the App's service principal in this workspace. Run via the
    #     CLI launcher with ``--profile`` instead.
    demo_dict = {
        "demo_cloudfiles": {
            "file": "demo/launch_af_cloudfiles_demo.py",
            "uc_arg": "--uc_catalog_name",
        },
        "demo_acf": {
            "file": "demo/launch_acfs_demo.py",
            "uc_arg": "--uc_catalog_name",
        },
        "demo_silverfanout": {
            "file": "demo/launch_silver_fanout_demo.py",
            "uc_arg": "--uc_catalog_name",
        },
        "demo_dias": {
            "file": "demo/launch_dais_demo.py",
            "uc_arg": "--uc_catalog_name",
        },
        "demo_interactive": {
            "file": "demo/launch_interactive_demo.py",
            "uc_arg": "--uc-catalog-name",
            # The interactive launcher submits a serverless job, prints
            # the run URL EARLY (before polling), and then blocks on
            # ``waiter.result(timeout=timedelta(minutes=N))``. We pass a
            # 1-minute timeout so the Flask request unblocks shortly
            # after submission with the run URL captured in stdout; the
            # actual demo job continues running in the workspace and the
            # user clicks through via the surfaced URL.
            #
            # ``--install-source pypi`` makes the spawned job
            # ``pip install databricks-labs-sdp-meta`` from PyPI on
            # every demo launch. Decouples the demo from whatever
            # branch the App container happens to have checked out,
            # so once a new release is published the App auto-picks
            # it up with zero redeploys.
            #
            # To pin a specific release instead of always-latest, add
            # e.g. ``"--pypi-version", "0.1.0"`` to the list below.
            #
            # The 1-minute timeout makes the launcher unblock shortly
            # after submitting the job so the Flask request returns
            # with the run URL; the demo continues running in the
            # workspace and the user clicks through.
            "extra_args": [
                "--install-source", "pypi",
                "--timeout-minutes", "1",
            ],
        },
    }
    demo_entry = demo_dict.get(code_to_run, None)
    uc_name = payload.get('uc_name', '')

    # Fix C3: reject unknown demo names (demo_entry already validated by dict lookup)
    if demo_entry is None:
        return jsonify({'error': 'Unknown demo name'}), 400

    demo_file = demo_entry["file"]
    demo_uc_arg = demo_entry["uc_arg"]
    demo_extra_args = demo_entry.get("extra_args", [])

    # ── UC catalog pre-flight ───────────────────────────────────────────────
    # Validate the catalog name shape and confirm the App SP has the
    # privileges every demo launcher transitively needs (USE_CATALOG +
    # CREATE_SCHEMA — the demos all CREATE SCHEMA <cat>.<schema>).
    #
    # Without this check, a missing grant surfaces inside the demo
    # subprocess as a generic ``PermissionDenied`` traceback in stderr —
    # actionable but ugly. Returning 400 here lets the front-end render
    # the "Grant required" panel with the exact GRANT SQL the catalog
    # owner needs to run, plus a "Verify and retry" button.
    try:
        validate_uc_identifier(uc_name, kind="uc_catalog_name")
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400

    preflight = check_app_sp_grants_on_catalog(uc_name)
    if not preflight.ok:
        # 400 — client-correctable: the catalog owner needs to run SQL.
        # Front-end pattern-matches on ``grant_required: True`` so the
        # demo modal can render the panel instead of a generic error.
        body = asdict(preflight)
        body['grant_required'] = True
        body['error'] = (
            f"App service principal '{preflight.sp_display_name}' "
            f"({preflight.sp_principal}) is missing "
            f"{', '.join(preflight.missing)} on catalog '{uc_name}'. "
            "Run the GRANT SQL below as the catalog owner and retry."
        )
        return jsonify(body), 400

    # Build subprocess environment.
    #
    # PYTHONPATH entries (order matters — earlier wins on import resolution):
    #   1. databricks_app/  — Python's `site` module auto-imports a top-level
    #      module called `sitecustomize` from sys.path on every interpreter
    #      startup. databricks_app/sitecustomize.py installs the App-mode
    #      shim that strips trailing ".py" from notebook_path arguments on
    #      WorkspaceClient.pipelines.create / .jobs.create. The shim is a
    #      no-op outside the App container; see databricks_app/sitecustomize.py
    #      for the full rationale. Putting databricks_app/ on PYTHONPATH is
    #      what activates it in the demo subprocess, without any change to
    #      demo/ or integration_tests/.
    #   2. repo root — so demo scripts can import `integration_tests` (lives
    #      at the repo root, not inside the demo/ directory that Python adds
    #      automatically as sys.path[0]).
    demo_env = os.environ.copy()
    databricks_app_dir = os.path.dirname(os.path.abspath(__file__))
    existing_pypath = demo_env.get('PYTHONPATH', '')
    pypath_entries = [databricks_app_dir, current_directory]
    if existing_pypath:
        pypath_entries.append(existing_pypath)
    demo_env['PYTHONPATH'] = ':'.join(pypath_entries)

    result = subprocess.run(
        [
            sys.executable,
            os.path.join(current_directory, demo_file),
            demo_uc_arg,
            uc_name,
            *demo_extra_args,
        ],
        shell=False,
        capture_output=True,
        text=True,
        cwd=current_directory,
        env=demo_env,
    )
    return extract_command_output(result)


def extract_command_output(result):
    stdout = result.stdout

    # Pipeline IDs are UUIDs (e.g. a1b2c3d4-…); job IDs are numeric.
    # Try UUID-style pipeline_id first, then fall back to numeric ids.
    pipeline_id_match = re.search(
        r"pipeline_id[=:\s]+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
        stdout, re.IGNORECASE,
    )
    # ``job_id=N`` / ``pipeline=N`` covers the bundle/CLI demos.
    # The interactive demo prints a hash-routed legacy run URL of the
    # shape ``<host>/?o=ID#job/<JOB_ID>/run/<RUN_ID>`` (workspace-side
    # ``Jobs.get_run().run_page_url`` on the serverless-stable shard),
    # so also recognise the numeric id inside ``#job/<N>/`` so the
    # success modal lights up for /demo_interactive launches.
    job_id_match = re.search(
        r"job_id=(\d+)|pipeline=(\d+)|#job/(\d+)/", stdout
    )

    if pipeline_id_match:
        pipeline_id = pipeline_id_match.group(1)
    elif job_id_match:
        pipeline_id = (
            job_id_match.group(1)
            or job_id_match.group(2)
            or job_id_match.group(3)
        )
    else:
        pipeline_id = None

    # ── URL extraction ───────────────────────────────────────────────────────
    # Resolve the job/pipeline URL the user should click on, in priority order:
    #
    #   1. The explicit ``url=https://...`` printed by SDPMETARunner.open_job_url
    #      (and the demo helpers that mimic it). Most authoritative.
    #   2. Any URL containing ``/jobs/`` or ``/pipelines/`` — what the demos
    #      ultimately want to surface.
    #   3. Any URL in stdout, EXCLUDING SDK-internal endpoints (``/oidc/...``,
    #      ``/api/...``). Inside a Databricks Apps container the SDK logs the
    #      OAuth token endpoint (``{host}/oidc/v1/token``) when it acquires a
    #      service-principal token; without this filter the previous "last URL
    #      wins" heuristic would surface that endpoint as the deploy result.
    SDK_INTERNAL_PATHS = ('/oidc/', '/api/')

    def _strip_trailing_punct(u: str) -> str:
        return re.sub(r'[,;:.)+]+$', '', u)

    job_url = None

    explicit_match = re.search(
        r"(?:job created successfully|pipeline created successfully|launched|run page).*?(?:url=)?(https?://\S+)",
        stdout,
        re.IGNORECASE,
    )
    if explicit_match:
        job_url = _strip_trailing_punct(explicit_match.group(1))
    else:
        all_urls = [_strip_trailing_punct(u) for u in re.findall(r"https?://\S+", stdout)]
        # Only surface URLs that actually point at a job or pipeline. Anything
        # else in stdout (workspace root, OIDC token endpoint, REST API base,
        # docs links, etc.) is not a valid "open in Databricks" target and
        # would otherwise dress up a silent demo failure as a success.
        #
        # ``#job/`` / ``#pipeline/`` covers the hash-routed legacy run URLs
        # emitted by the interactive demo (workspace-side ``run_page_url``
        # on the serverless-stable shard).
        job_pipeline_urls = [
            u for u in all_urls
            if (
                '/jobs/' in u or '/pipelines/' in u
                or '#job/' in u or '#pipeline/' in u
            )
            and not any(p in u for p in SDK_INTERNAL_PATHS)
        ]
        if job_pipeline_urls:
            job_url = job_pipeline_urls[-1]

    # If we extracted a pipeline UUID but the URL is missing/wrong, build the
    # direct pipeline URL from any workspace-host URL we did find.
    if pipeline_id and (not job_url or ('/pipelines/' not in job_url and '/jobs/' not in job_url)):
        # Match AWS (*.cloud.databricks.com), Azure (*.azuredatabricks.net),
        # and GCP (*.gcp.databricks.com) workspace hosts — restricting to AWS
        # would silently drop the success modal on Azure/GCP workspaces.
        all_hosts = re.findall(
            r"(https?://[a-zA-Z0-9.\-]+\."
            r"(?:cloud\.databricks\.com|azuredatabricks\.net|gcp\.databricks\.com))",
            stdout,
        )
        if all_hosts:
            job_url = f"{all_hosts[0]}/pipelines/{pipeline_id}"

    if job_url:
        modal_html = {'title': 'Pipeline Created Successfully',
                      'job_id': pipeline_id,
                      'job_url': job_url
                      }
    else:
        modal_html = None
    # Return the response as JSON
    return jsonify({
        'modal_content': modal_html,
        'stdout': result.stdout,
        'stderr': result.stderr,
        'returncode': result.returncode
    })


if __name__ == '__main__':
    # Fix C4: never run the Werkzeug interactive debugger in production.
    # To enable debug mode locally: export FLASK_DEBUG=true
    app.run(debug=os.getenv('FLASK_DEBUG', 'false').lower() == 'true')
