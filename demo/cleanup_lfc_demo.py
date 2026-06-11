"""
Clean up all resources created by a launch_lfc_demo.py setup run.

Usage:
  python demo/cleanup_lfc_demo.py --run_id=<run_id> --profile=<profile>

Objects removed by run_id (always):
  - Databricks jobs   : sdp-meta-lfc-demo-{run_id}
                        sdp-meta-lfc-demo-incremental-{run_id}
  - DLT pipelines     : bronze + silver (IDs read from job before deletion)
  - UC schemas        : sdp_meta_dataflowspecs_lfc_{run_id}
                        sdp_meta_bronze_lfc_{run_id}
                        sdp_meta_silver_lfc_{run_id}
  - Workspace dir     : /Users/{user}/sdp_meta_lfc_demo/{run_id}/

LFC resources (run-scoped when possible):
  - The notebook writes conf/lfc_created.json to the volume with lfc_schema, pipeline IDs, scheduler job ID.
  - Cleanup reads that file (before deleting the volume) and then deletes only that LFC schema,
    those pipelines, and that job.
  - If the file is missing (e.g. run from before this feature), use flags to delete all:
    --include-all-lfc-schemas   (drops ALL {user}_{source_type}_* schemas)
    --include-all-lfc-pipelines (deletes ALL {user}_*_gw/_ig pipelines and scheduler jobs)

The Unity Catalog itself (e.g. 'main') is NOT deleted.
"""

import os
import sys

# Must happen before any databricks.* import so src/ is part of the
# databricks namespace package when it is first loaded.
_repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _repo_root)
sys.path.insert(0, os.path.join(_repo_root, "src"))

import argparse
import json
import re
import time

from databricks.sdk.service.sql import StatementState
from integration_tests.run_integration_tests import get_workspace_api_client


# ── Name prefix ─────────────────────────────────────────────────────────────
# Mirrors launch_lfc_demo.py — change here to rename all references at once.
_DEMO_SLUG = "sdp-meta-lfc"  # hyphenated  → job/pipeline names
_DEMO_PREFIX = "sdp_meta"      # underscored → UC schema names, workspace paths


def read_lfc_created(ws, catalog, run_id):
    """
    Read conf/lfc_created.json from the run's volume (written by lfcdemo-database.py).
    Returns dict with lfc_schema, gw_pipeline_id, ig_pipeline_id, lfc_scheduler_job_id, or None.
    """
    path = (
        f"/Volumes/{catalog}/{_DEMO_PREFIX}_dataflowspecs_lfc_{run_id}"
        f"/{catalog}_lfc_volume_{run_id}/conf/lfc_created.json"
    )
    try:
        resp = ws.files.download(file_path=path)
        data = resp.contents.read()
        return json.loads(data.decode("utf-8"))
    except Exception:
        return None


def parse_args():
    p = argparse.ArgumentParser(description="Clean up LFC demo resources for a given run_id.")
    p.add_argument("--run_id", required=True, help="run_id printed by launch_lfc_demo.py setup")
    p.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile (default: DEFAULT)")
    p.add_argument("--catalog", default="main", help="Unity Catalog name (default: main)")
    p.add_argument(
        "--include-all-lfc-schemas",
        action="store_true",
        help="Also drop ALL LFC streaming-table schemas for this user (not scoped to run_id).",
    )
    p.add_argument(
        "--include-all-lfc-pipelines",
        action="store_true",
        help="Also delete ALL LFC scheduler jobs and gateway/ingestion pipelines for this user.",
    )
    p.add_argument(
        "--skip_lfc_pipelines",
        action="store_true",
        help="Deprecated: use default (no LFC pipeline deletion) or --include-all-lfc-pipelines.",
    )
    return p.parse_args()


def make_sql_runner(ws):
    wh = next(
        (w for w in ws.warehouses.list() if str(w.state).endswith("RUNNING")),
        None,
    )
    if not wh:
        raise RuntimeError("No running SQL warehouse found — cannot execute DROP SCHEMA CASCADE.")
    wh_id = wh.id

    def run(stmt):
        r = ws.statement_execution.execute_statement(
            statement=stmt, warehouse_id=wh_id, wait_timeout="30s"
        )
        while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
            time.sleep(1)
            r = ws.statement_execution.get_statement(r.statement_id)
        return r.status.state

    return run


def delete_jobs_and_pipelines(ws, run_id):
    """Delete DLT-Meta jobs, extracting pipeline IDs before the job is gone."""
    pipeline_ids = []
    for jname in [
        f"{_DEMO_SLUG}-demo-{run_id}",
        f"{_DEMO_SLUG}-demo-{run_id}-downstream",
        f"{_DEMO_SLUG}-demo-incremental-{run_id}",
    ]:
        j = next((x for x in ws.jobs.list(name=jname) if x.settings.name == jname), None)
        if not j:
            print(f"  Job not found (skipped): {jname}")
            continue
        jd = ws.jobs.get(job_id=j.job_id)
        pids = [t.pipeline_task.pipeline_id for t in jd.settings.tasks if t.pipeline_task]
        pipeline_ids.extend(pids)
        ws.jobs.delete(j.job_id)
        print(f"  Deleted job     : {jname}  (pipeline_ids={pids})")

    for pid in pipeline_ids:
        try:
            ws.pipelines.delete(pid)
            print(f"  Deleted pipeline: {pid}")
        except Exception as e:
            print(f"  Pipeline {pid}: {e}")


def delete_sdp_meta_schemas(ws, catalog, run_id, sql):
    """Drop the three DLT-Meta schemas created by the setup run."""
    for sname in [
        f"{_DEMO_PREFIX}_dataflowspecs_lfc_{run_id}",
        f"{_DEMO_PREFIX}_bronze_lfc_{run_id}",
        f"{_DEMO_PREFIX}_silver_lfc_{run_id}",
    ]:
        s = next((x for x in ws.schemas.list(catalog_name=catalog) if x.name == sname), None)
        if not s:
            print(f"  Schema not found (skipped): {catalog}.{sname}")
            continue
        for vol in ws.volumes.list(catalog_name=catalog, schema_name=sname):
            ws.volumes.delete(vol.full_name)
            print(f"  Deleted volume  : {vol.full_name}")
        result = sql(f"DROP SCHEMA IF EXISTS {s.full_name} CASCADE")
        print(f"  DROP SCHEMA     : {s.full_name} CASCADE → {result}")


def delete_lfc_schemas_all(ws, catalog, name_prefix, sql):
    """Drop all LFC streaming-table schemas for this user (--include-all-lfc-schemas)."""
    all_lfc = [
        s for s in ws.schemas.list(catalog_name=catalog)
        if s.name.startswith(name_prefix) and (
            "sqlserver" in s.name or "mysql" in s.name or "postgresql" in s.name
        )
    ]
    print(f"\n  Found {len(all_lfc)} LFC streaming-table schema(s)")
    for s in all_lfc:
        result = sql(f"DROP SCHEMA IF EXISTS {s.full_name} CASCADE")
        print(f"  DROP SCHEMA     : {s.full_name} CASCADE → {result}")


def delete_lfc_schema_from_created(ws, catalog, lfc_created, sql):
    """Drop the single LFC schema recorded in lfc_created (run-scoped)."""
    schema_name = (lfc_created or {}).get("lfc_schema")
    if not schema_name:
        return
    full_name = f"{catalog}.{schema_name}"
    result = sql(f"DROP SCHEMA IF EXISTS {full_name} CASCADE")
    print(f"  DROP SCHEMA     : {full_name} CASCADE → {result}")


def delete_lfc_from_created(ws, lfc_created):
    """Delete only the LFC scheduler job and gateway/ingestion pipelines recorded in lfc_created (run-scoped)."""
    if not lfc_created:
        return
    job_id = lfc_created.get("lfc_scheduler_job_id")
    if job_id:
        try:
            ws.jobs.delete(job_id=job_id)
            print(f"  Deleted job     : {job_id}")
        except Exception as e:
            print(f"  Job {job_id}: {e}")
    for key in ("gw_pipeline_id", "ig_pipeline_id"):
        pid = lfc_created.get(key)
        if pid:
            try:
                ws.pipelines.delete(pid)
                print(f"  Deleted pipeline: {pid}")
            except Exception as e:
                print(f"  Pipeline {pid}: {e}")


def delete_lfc_pipelines_and_jobs_all(ws, name_prefix):
    """Delete ALL LFC gateway/ingestion pipelines and their scheduler jobs (--include-all-lfc-pipelines)."""
    lfc_jobs = [
        j for j in ws.jobs.list()
        if (j.settings.name or "").startswith(name_prefix) and "_ig_" in (j.settings.name or "")
    ]
    print(f"\n  Found {len(lfc_jobs)} LFC scheduler job(s)")
    for j in lfc_jobs:
        try:
            ws.jobs.delete(j.job_id)
            print(f"  Deleted job     : {j.settings.name} ({j.job_id})")
        except Exception as e:
            print(f"  Job {j.settings.name}: {e}")

    try:
        all_with_prefix = list(ws.pipelines.list_pipelines(filter=f"name LIKE '{name_prefix}%'"))
    except Exception:
        all_with_prefix = list(ws.pipelines.list_pipelines())
    lfc = [
        p for p in all_with_prefix
        if (p.name or "").startswith(name_prefix)
        and (p.name or "").endswith(("_gw", "_ig"))
    ]
    print(f"\n  Found {len(lfc)} LFC pipeline(s)")
    for p in lfc:
        try:
            ws.pipelines.delete(p.pipeline_id)
            print(f"  Deleted pipeline: {p.name} ({p.pipeline_id})")
        except Exception as e:
            print(f"  Pipeline {p.name}: {e}")


def delete_workspace_dir(ws, username, run_id):
    nb_path = f"/Users/{username}/{_DEMO_PREFIX}_lfc_demo/{run_id}"
    try:
        ws.workspace.delete(nb_path, recursive=True)
        print(f"\n  Deleted workspace dir: {nb_path}")
    except Exception as e:
        print(f"\n  Workspace dir skipped: {e}")


def main():
    args = parse_args()
    run_id = args.run_id
    catalog = args.catalog

    print(f"Connecting with profile '{args.profile}'...")
    ws = get_workspace_api_client(args.profile)
    username = ws.current_user.me().user_name
    name_prefix = re.sub(r"[.\-@]", "_", username.split("@")[0]).lower()
    sql = make_sql_runner(ws)

    print(f"  user       : {username}")
    print(f"  run_id     : {run_id}")
    print(f"  catalog    : {catalog}")
    print(f"  name_prefix: {name_prefix}\n")

    print("Step 1 — Deleting DLT-Meta jobs and pipelines...")
    delete_jobs_and_pipelines(ws, run_id)

    # Read LFC-created resources from volume before we delete it (written by lfcdemo-database.py)
    lfc_created = read_lfc_created(ws, catalog, run_id)
    if lfc_created:
        print(f"\n  Read lfc_created.json: schema={lfc_created.get('lfc_schema')}")

    print("\nStep 2 — Dropping DLT-Meta UC schemas...")
    delete_sdp_meta_schemas(ws, catalog, run_id, sql)

    if lfc_created:
        print("\nStep 3 — Dropping LFC streaming-table schema (from notebook output)...")
        delete_lfc_schema_from_created(ws, catalog, lfc_created, sql)
    elif args.include_all_lfc_schemas:
        print("\nStep 3 — Dropping LFC streaming-table schemas (all for this user)...")
        delete_lfc_schemas_all(ws, catalog, name_prefix, sql)
    else:
        print("\nStep 3 — Skipped (no lfc_created.json; use --include-all-lfc-schemas to drop all)")

    if lfc_created:
        print("\nStep 4 — Deleting LFC scheduler job and pipelines (from notebook output)...")
        delete_lfc_from_created(ws, lfc_created)
    elif args.include_all_lfc_pipelines:
        print("\nStep 4 — Deleting LFC scheduler jobs, gateway/ingestion pipelines (all for this user)...")
        delete_lfc_pipelines_and_jobs_all(ws, name_prefix)
    elif args.skip_lfc_pipelines:
        print("\nStep 4 — Skipped (--skip_lfc_pipelines)")
    else:
        print("\nStep 4 — Skipped (no lfc_created.json; use --include-all-lfc-pipelines to delete all)")

    print("\nStep 5 — Deleting workspace directory...")
    delete_workspace_dir(ws, username, run_id)

    print("\nCleanup complete.")


if __name__ == "__main__":
    main()
