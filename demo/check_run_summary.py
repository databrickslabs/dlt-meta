"""
Tabular summary: rows generated / bronze / silver per job run.

Usage:
    python demo/check_run_summary.py --profile=DEFAULT --run_id=<run_id>

Generated rows are inferred from CSV files whose last_modified timestamp falls
in the window [run_start, next_run_start).  Row count per file comes from the
job's table_data_rows_count parameter — no per-file SQL needed.

The Unity Catalog name is derived automatically from the setup job's
onboarding_job task (database parameter), so it does not need to be supplied.
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone

from databricks.sdk.service.sql import StatementState

sys.path.insert(0, ".")
from integration_tests.run_integration_tests import get_workspace_api_client

# ── CLI arguments ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Per-run row summary for the Techsummit demo.")
parser.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile (default: DEFAULT)")
parser.add_argument("--run_id", required=True, help="run_id printed at the end of the setup run")
args = parser.parse_args()

PROFILE = args.profile
RUN_ID = args.run_id

ws = get_workspace_api_client(PROFILE)

# ── resolve job IDs by name ────────────────────────────────────────────────────


def find_job(name):
    return next((j for j in ws.jobs.list(name=name) if j.settings.name == name), None)


setup_job = find_job(f"dlt-meta-techsummit-demo-{RUN_ID}")
incr_job = find_job(f"dlt-meta-techsummit-demo-incremental-{RUN_ID}")
if not setup_job:
    sys.exit(f"Setup job not found for run_id={RUN_ID}")

# ── derive catalog from setup job's onboarding_job task ───────────────────────
setup_details = ws.jobs.get(job_id=setup_job.job_id)
onboarding_task = next(
    (t for t in setup_details.settings.tasks if t.task_key == "onboarding_job"),
    None,
)
if not onboarding_task or not onboarding_task.python_wheel_task:
    sys.exit("Could not find onboarding_job task in setup job — cannot derive catalog.")
database = onboarding_task.python_wheel_task.named_parameters.get("database", "")
CATALOG = database.split(".")[0]
if not CATALOG:
    sys.exit(f"Could not parse catalog from onboarding_job database='{database}'.")
print(f"Derived catalog: {CATALOG}")

# ── collect job runs (limit=20 per job) ordered oldest-first ───────────────────
runs = []
for job, label in [(setup_job, "setup")] + ([(incr_job, "incremental")] if incr_job else []):
    for run in ws.jobs.list_runs(job_id=job.job_id, limit=20):
        result = (str(run.state.result_state or run.state.life_cycle_state)
                  .replace("RunResultState.", "").replace("RunLifeCycleState.", ""))
        rows_per_file = 10  # default
        try:
            detail = ws.jobs.get_run(run_id=run.run_id)
            for t in (detail.tasks or []):
                if t.task_key in ("generate_data", "generate_incremental_data"):
                    if t.notebook_task and t.notebook_task.base_parameters:
                        rows_per_file = int(
                            t.notebook_task.base_parameters.get("table_data_rows_count", 10)
                        )
        except Exception:
            pass
        runs.append({
            "label": label,
            "run_id": run.run_id,
            "start_ms": run.start_time or 0,
            "result": result,
            "rows_per_file": rows_per_file,
        })

runs.sort(key=lambda r: r["start_ms"])

# ── list CSV files in source volume with modification timestamps ───────────────
vol_input = (
    f"/Volumes/{CATALOG}/dlt_meta_dataflowspecs_demo_{RUN_ID}"
    f"/{CATALOG}_volume_{RUN_ID}/resources/data/input"
)
print("Listing source CSV files...")
csv_files = []
try:
    for tbl_dir in ws.files.list_directory_contents(vol_input):
        for f in ws.files.list_directory_contents(tbl_dir.path):
            if f.name and f.name.endswith(".csv"):
                csv_files.append({"modified": f.last_modified or 0, "table": tbl_dir.name})
except Exception as e:
    print(f"  Warning: {e}")
csv_files.sort(key=lambda f: f["modified"])
print(f"  {len(csv_files)} CSV file(s) found")

# ── assign CSV files to runs by modified timestamp window ─────────────────────
now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
for i, run in enumerate(runs):
    w_start = run["start_ms"]
    w_end = runs[i + 1]["start_ms"] if i + 1 < len(runs) else now_ms
    matched = [f for f in csv_files if w_start <= f["modified"] < w_end]
    run["new_files"] = len(matched)
    run["generated"] = len(matched) * run["rows_per_file"]

# ── SQL helper ─────────────────────────────────────────────────────────────────
wh_id = next(w for w in ws.warehouses.list() if str(w.state).endswith("RUNNING")).id


def q(sql):
    resp = ws.statement_execution.execute_statement(
        statement=sql, warehouse_id=wh_id, wait_timeout="30s"
    )
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(1)
        resp = ws.statement_execution.get_statement(resp.statement_id)
    return resp.result.data_array or [] if resp.status.state == StatementState.SUCCEEDED else []

# ── STREAMING UPDATE history for bronze and silver ────────────────────────────


def streaming_updates(schema, table):
    updates = []
    for row in q(f"DESCRIBE HISTORY {CATALOG}.{schema}.{table}"):
        version, ts, op, raw = row[0], row[1], row[4], row[12]  # noqa: F841
        if op == "STREAMING UPDATE":
            try:
                m = json.loads(raw) if raw else {}
            except Exception:
                m = {}
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            updates.append({"ts": dt, "rows": int(m.get("numOutputRows", 0))})
    updates.sort(key=lambda u: u["ts"])
    return updates


print("Reading Delta history...")
bronze_upd = streaming_updates(f"dlt_meta_bronze_demo_{RUN_ID}", "table_1")
silver_upd = streaming_updates(f"dlt_meta_silver_demo_{RUN_ID}", "table_1")

for i, run in enumerate(runs):
    run["bronze"] = bronze_upd[i]["rows"] if i < len(bronze_upd) else "—"
    run["silver"] = silver_upd[i]["rows"] if i < len(silver_upd) else "—"

# ── print table ────────────────────────────────────────────────────────────────
print()
HDR = (f"{'Date/Time (UTC)':<22}  {'Type':<13}  {'Status':<10}  "
       f"{'New CSVs':>8}  {'Generated':>10}  {'Bronze':>8}  {'Silver':>8}")
print(HDR)
print("─" * len(HDR))
for run in runs:
    dt = datetime.fromtimestamp(run["start_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{dt:<22}  {run['label']:<13}  {run['result']:<10}  "
          f"{run['new_files']:>8}  {str(run['generated']):>10}  "
          f"{str(run['bronze']):>8}  {str(run['silver']):>8}")
