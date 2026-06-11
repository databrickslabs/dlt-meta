# Databricks notebook source
# Trigger the LFC ingestion pipeline and wait for it to finish before bronze/silver run.
# Used by the incremental job so DLT-Meta reads the latest data from the streaming tables.

# COMMAND ----------

dbutils.widgets.text("run_id", "", "run_id")
dbutils.widgets.text("target_catalog", "", "target_catalog")
dbutils.widgets.text("trigger_interval_min", "5", "trigger_interval_min")

# COMMAND ----------

import json
import time
from databricks.sdk import WorkspaceClient

_run_id = (dbutils.widgets.get("run_id") or "").strip()
_catalog = (dbutils.widgets.get("target_catalog") or "").strip()
_trigger = (dbutils.widgets.get("trigger_interval_min") or "").strip()

if not _run_id or not _catalog:
    raise ValueError("run_id and target_catalog are required")

# Continuous mode: no discrete "run" to wait for; skip trigger and proceed
if _trigger == "0":
    print("Continuous mode (trigger_interval_min=0): skipping ingestion trigger; proceeding.")
    dbutils.notebook.exit(0)

# COMMAND ----------

_path = (
    f"/Volumes/{_catalog}/sdp_meta_dataflowspecs_lfc_{_run_id}"
    f"/{_catalog}_lfc_volume_{_run_id}/conf/lfc_created.json"
)
try:
    payload = json.loads(dbutils.fs.head(_path))
except Exception as e:
    raise FileNotFoundError(f"Cannot read {_path}: {e}") from e

ig_pipeline_id = payload.get("ig_pipeline_id")
lfc_scheduler_job_id = payload.get("lfc_scheduler_job_id")
if not ig_pipeline_id:
    raise ValueError(f"lfc_created.json missing ig_pipeline_id: {payload}")

ws = WorkspaceClient()

# Force trigger: run the LFC scheduler job once (same as lfcdemo-database.py jobs_runnow).
# We do not wait for the job run to finish; we wait for the pipeline update below.
# If the scheduler job was deleted (e.g. by LFC auto-cleanup), lfc_created.json still has its id;
# fall back to starting the ingestion pipeline directly.
if lfc_scheduler_job_id:
    try:
        ws.jobs.run_now(job_id=lfc_scheduler_job_id)
        print(f"Triggered ingestion via scheduler job {lfc_scheduler_job_id} (not waiting for job run).")
    except Exception as e:
        err = str(e).lower()
        if "does not exist" in err or "invalidparametervalue" in err or "job" in err and "not found" in err:
            print(f"Scheduler job {lfc_scheduler_job_id} no longer exists ({e}); starting ingestion pipeline directly.")
            ws.pipelines.start_update(pipeline_id=ig_pipeline_id)
        else:
            raise
else:
    ws.pipelines.start_update(pipeline_id=ig_pipeline_id)
    print("No scheduler job; started pipeline update directly.")

print(f"Waiting for ingestion pipeline {ig_pipeline_id} latest update to complete...")

# COMMAND ----------

def _is_completed(s):
    if not s:
        return False
    s = str(s).upper()
    return s == "COMPLETED" or s.split(".")[-1] == "COMPLETED"

def _is_failed(s):
    if not s:
        return False
    s = str(s).upper()
    return s == "FAILED" or s.split(".")[-1] == "FAILED"

# COMMAND ----------

_TIMEOUT_SEC = 1200
_POLL_SEC = 30
_start = time.time()
_update_id = None

while True:
    elapsed = int(time.time() - _start)
    p = ws.pipelines.get(pipeline_id=ig_pipeline_id)
    updates = p.latest_updates or []
    if not updates:
        print(f"  [{elapsed:>5}s] Waiting for update to appear...")
        time.sleep(_POLL_SEC)
        continue
    latest = updates[0]
    _update_id = latest.update_id
    state = str(latest.state)
    print(f"  [{elapsed:>5}s] update state={state}")
    if _is_completed(state):
        print("Ingestion pipeline update COMPLETED.")
        dbutils.notebook.exit(0)
    if _is_failed(state):
        raise RuntimeError(f"Ingestion pipeline update FAILED: state={state}")
    if elapsed >= _TIMEOUT_SEC:
        raise TimeoutError(f"Ingestion pipeline did not complete within {_TIMEOUT_SEC}s")
    time.sleep(_POLL_SEC)
