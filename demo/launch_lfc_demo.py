"""
Launch the DLT-Meta Lakeflow Connect (LFC) Demo.

Setup run (first time):
  1. Uploads demo/lfcdemo-database.py to the Databricks workspace.
  2. Creates and runs a job that:
       a. lfc_setup        — runs lfcdemo-database.py, which creates the LFC gateway +
                             ingestion pipelines and starts DML against the source DB.
       b. onboarding_job   — registers intpk and dtix as delta (streaming table) sources
                             in the DLT-Meta dataflow spec schema.
       c. bronze_dlt       — DLT-Meta bronze pipeline reads the LFC streaming tables.
       d. silver_dlt       — DLT-Meta silver pipeline (pass-through transformations).

Incremental run (re-trigger after initial setup):
  python demo/launch_lfc_demo.py --profile=DEFAULT --run_id=<run_id_from_setup>
  Re-triggers bronze_dlt → silver_dlt against the latest LFC streaming-table state.
  (LFC ingestion pipeline continuously updates the streaming tables while it is running.)
"""

import io
import json
import os
import sys
import traceback
import uuid
import webbrowser
from dataclasses import dataclass

# Ensure src/ is on sys.path so the local sdp_meta package resolves its own
# absolute imports (e.g. databricks.labs.sdp_meta.__about__) correctly when
# running directly from the repo root with PYTHONPATH=$(pwd).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments,
)

LFC_TABLES = ["intpk", "dtix"]
# intpk uses CDF (readChangeFeed). dtix uses snapshot (no CDF) — see LFC_DTIX_BRONZE_APPLY_CHANGES_FROM_SNAPSHOT.
LFC_TABLE_BRONZE_READER_OPTIONS = {"intpk": {"readChangeFeed": "true"}}
# intpk: bronze_cdc_apply_changes (process CDC). Uses Delta CDF columns: _change_type, _commit_version.
# LFC streaming table must have delta.enableChangeDataFeed = true for intpk.
LFC_INTPK_BRONZE_CDC_APPLY_CHANGES = {
    "keys": ["pk"],
    "sequence_by": "_commit_version",
    "scd_type": "1",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
}
# dtix: LFC SCD Type 2 — the LFC streaming table already has __START_AT/__END_AT columns.
# DLT reserves __START_AT/__END_AT as system column names for ALL DLT operations.
# Solution: use apply_changes_from_snapshot (snapshot, not CDF) and rename the reserved
# LFC columns via a bronze_custom_transform in init_sdp_meta_pipeline.py:
#   __START_AT → lfc_start_at,  __END_AT → lfc_end_at
# source_format="snapshot" + source_details.snapshot_format="delta" triggers the
# snapshot-comparison CDC path in DLT-Meta.
#
# Key choice — why (dt, lfc_end_at) and not (dt, lfc_start_at):
#   LFC assigns __END_AT a unique __cdc_internal_value for EVERY row, including initial-load
#   rows whose __START_AT is null.  For no-PK tables the source can hold many rows with the
#   same dt and null __START_AT, making (dt, lfc_start_at) non-unique.
#   __END_AT is null only for the single currently-active version of each dt value, so
#   (dt, __END_AT) is globally unique across both historical and active rows.
LFC_DTIX_BRONZE_APPLY_CHANGES_FROM_SNAPSHOT = {
    "keys": ["dt", "lfc_end_at"],
    "scd_type": "1",
}
# Silver reads from bronze intpk with readChangeFeed=true so that MERGE operations written
# by bronze CDC (apply_changes) are consumed as logical CDC rows rather than raw Delta files.
# Without CDF, Delta streaming raises DELTA_SOURCE_TABLE_IGNORE_CHANGES on any MERGE commit.
LFC_INTPK_SILVER_READER_OPTIONS = {"readChangeFeed": "true"}
# Silver CDC config for intpk: CDF-aware (handles _change_type, sequences by _commit_version).
LFC_INTPK_SILVER_CDC_APPLY_CHANGES = {
    "keys": ["pk"],
    "sequence_by": "_commit_version",
    "scd_type": "1",
    "apply_as_deletes": "_change_type = 'delete'",
    "except_column_list": ["_change_type", "_commit_version", "_commit_timestamp"],
}
# dtix silver: same snapshot approach as bronze; reads bronze as a snapshot.
# Bronze already has lfc_start_at/lfc_end_at (renamed from LFC's __START_AT/__END_AT).
LFC_DTIX_SILVER_APPLY_CHANGES_FROM_SNAPSHOT = {
    "keys": ["dt", "lfc_end_at"],
    "scd_type": "1",
}
LFC_DEFAULT_SCHEMA = "lfcddemo"
# Cap jobs.list() to avoid slow full-workspace iteration (API returns 25 per page)
JOBS_LIST_LIMIT = 100

# ── Name prefix ─────────────────────────────────────────────────────────────
# Change these two constants to rename all job/pipeline/schema/path references
# at once without hunting through the file.
_DEMO_SLUG = "sdp-meta-lfc"  # hyphenated  → job/pipeline names
_DEMO_PREFIX = "sdp_meta"      # underscored → UC schema names, workspace paths


@dataclass
class LFCRunnerConf(SDPMetaRunnerConf):
    """Configuration for the LFC demo runner."""
    lfc_schema: str = None          # source schema on the source DB (passed to notebook as source_schema)
    connection_name: str = None     # Databricks connection name for the source DB
    cdc_qbc: str = "cdc"            # LFC pipeline mode
    trigger_interval_min: str = "5"  # LFC trigger interval in minutes
    sequence_by_pk: bool = False   # if True, use primary key for CDC silver sequence_by; else use dt
    # default True; notebook triggers onboarding→bronze→silver when ready.
    # Use --no_parallel_downstream to run as single job.
    parallel_downstream: bool = True
    downstream_job_id: int = None  # when parallel_downstream, ID of the onboarding→bronze→silver job
    lfc_notebook_ws_path: str = None  # resolved workspace path of the uploaded LFC notebook
    setup_job_id: int = None  # setup job id (set when resolving incremental; used to write metadata)
    # "cdf" = custom next_snapshot_and_version lambda (O(1) fast skip); "full" = view-based full scan
    snapshot_method: str = "cdf"


class DLTMETALFCDemo(SDPMETARunner):
    """Run the DLT-Meta Lakeflow Connect Demo."""

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    # ── helpers ──────────────────────────────────────────────────────────────

    def _is_incremental(self) -> bool:
        """True when --run_id is supplied, implying incremental (re-trigger) mode."""
        return bool(self.args.get("run_id"))

    # ── configuration ────────────────────────────────────────────────────────

    def init_runner_conf(self) -> LFCRunnerConf:
        """
        Build an LFCRunnerConf from CLI args.
        In incremental mode the existing setup job is inspected to fill in
        any missing fields (uc_catalog_name, lfc_schema, pipeline IDs).
        """
        run_id = self.args["run_id"] if self._is_incremental() else uuid.uuid4().hex
        lfc_schema = self.args.get("source_schema") or LFC_DEFAULT_SCHEMA

        runner_conf = LFCRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            sdp_meta_schema=f"{_DEMO_PREFIX}_dataflowspecs_lfc_{run_id}",
            bronze_schema=f"{_DEMO_PREFIX}_bronze_lfc_{run_id}",
            silver_schema=f"{_DEMO_PREFIX}_silver_lfc_{run_id}",
            runners_full_local_path="demo/notebooks/lfc_runners",
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/{_DEMO_PREFIX}_lfc_demo/{run_id}",
            int_tests_dir="demo",
            env="prod",
            lfc_schema=lfc_schema,
            connection_name=self.args.get("connection_name"),
            cdc_qbc=self.args.get("cdc_qbc") or "cdc",
            trigger_interval_min=str(self.args.get("trigger_interval_min") or "5"),
            sequence_by_pk=bool(self.args.get("sequence_by_pk")),
            parallel_downstream=not bool(self.args.get("no_parallel_downstream")),
            snapshot_method=self.args.get("snapshot_method") or "cdf",
        )

        if self.args.get("uc_catalog_name"):
            runner_conf.uc_catalog_name = self.args["uc_catalog_name"]
            runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_lfc_volume_{run_id}"

        if self._is_incremental():
            self._resolve_incremental_conf(runner_conf)

        return runner_conf

    def _setup_metadata_path(self, runner_conf: LFCRunnerConf) -> str:
        """Workspace path for conf/setup_metadata.json (job_id, uc_catalog_name, incremental_job_id)."""
        return f"{runner_conf.runners_nb_path}/conf/setup_metadata.json"

    def _read_setup_metadata(self, runner_conf: LFCRunnerConf) -> dict | None:
        """Read setup_metadata.json from workspace; returns None if missing."""
        path = self._setup_metadata_path(runner_conf)
        try:
            with self.ws.workspace.download(path) as f:
                return json.load(f)
        except Exception:
            return None

    def _write_setup_metadata(self, runner_conf: LFCRunnerConf, data: dict):
        """Write setup_metadata.json to workspace (creates conf dir if needed)."""
        path = self._setup_metadata_path(runner_conf)
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/conf")
        self.ws.workspace.upload(
            path=path,
            content=io.BytesIO(json.dumps(data, indent=2).encode("utf-8")),
            format=ImportFormat.AUTO,
        )

    def _job_set_no_retry(self, job_id: int):
        """Set job-level max_retries=0 so the job is not retried on failure (SDK has no job-level field).
        Note: Marking a job/task as 'production' in the UI does not change retry behavior; only this setting does."""
        try:
            self.ws.api_client.do(
                "POST",
                "/api/2.1/jobs/update",
                body={"job_id": job_id, "new_settings": {"max_retries": 0}},
            )
        except Exception as e:
            raise RuntimeError(
                f"Failed to set job {job_id} to max_retries=0 (job may retry on failure): {e}"
            ) from e

    def _resolve_incremental_conf(self, runner_conf: LFCRunnerConf):
        """
        Populate uc_catalog_name (if not supplied), lfc_schema, and bronze/silver
        pipeline IDs by inspecting the existing LFC setup job. Prefer job_id from
        setup_metadata.json (fast); fall back to jobs.list by name (slow).
        """
        setup_job_name = f"{_DEMO_SLUG}-demo-{runner_conf.run_id}"
        print(f"Looking up setup job '{setup_job_name}'...")
        setup_job = None
        meta = self._read_setup_metadata(runner_conf)
        if meta and meta.get("job_id") is not None:
            try:
                job_details = self.ws.jobs.get(job_id=meta["job_id"])
                if job_details.settings.name == setup_job_name:
                    setup_job = job_details
                    print(f"  Found job_id={setup_job.job_id} (from setup_metadata.json)")
            except Exception:
                pass
        if not setup_job:
            setup_job = next(
                (
                    j
                    for j in self.ws.jobs.list(name=setup_job_name, limit=JOBS_LIST_LIMIT)
                    if j.settings.name == setup_job_name
                ),
                None,
            )
            if not setup_job:
                raise ValueError(
                    f"Setup job '{setup_job_name}' not found in first {JOBS_LIST_LIMIT} jobs. "
                    "Ensure the original setup run completed successfully."
                )
            print(f"  Found job_id={setup_job.job_id}")
            job_details = self.ws.jobs.get(job_id=setup_job.job_id)
        else:
            job_details = setup_job
        runner_conf.setup_job_id = job_details.job_id

        # When parallel_downstream was used, onboarding and pipeline IDs live in the downstream job
        job_for_downstream = job_details
        if meta and meta.get("downstream_job_id") is not None:
            try:
                job_for_downstream = self.ws.jobs.get(job_id=meta["downstream_job_id"])
                print(f"  Using downstream_job_id={meta['downstream_job_id']} for onboarding/pipelines")
            except Exception:
                pass

        if not runner_conf.uc_catalog_name:
            # Derive uc_catalog_name from the onboarding_job task's "database" parameter
            onboarding_task = next(
                (t for t in job_for_downstream.settings.tasks if t.task_key == "onboarding_job"),
                None,
            )
            if onboarding_task and onboarding_task.python_wheel_task:
                database = onboarding_task.python_wheel_task.named_parameters.get("database", "")
                runner_conf.uc_catalog_name = database.split(".")[0]
            if not runner_conf.uc_catalog_name:
                raise ValueError(
                    "Could not derive uc_catalog_name from the existing job. "
                    "Please supply --uc_catalog_name explicitly."
                )
            runner_conf.uc_volume_name = (
                f"{runner_conf.uc_catalog_name}_lfc_volume_{runner_conf.run_id}"
            )
            print(f"  Derived uc_catalog_name={runner_conf.uc_catalog_name}")

        # Always (re-)derive uc_volume_path — not set by initialize_uc_resources in incr. mode
        runner_conf.uc_volume_path = (
            f"/Volumes/{runner_conf.uc_catalog_name}/"
            f"{runner_conf.sdp_meta_schema}/{runner_conf.uc_volume_name}/"
        )

        # Derive lfc_schema and trigger_interval_min from the lfc_setup task if not supplied
        lfc_task = next(
            (t for t in job_details.settings.tasks if t.task_key == "lfc_setup"),
            None,
        )
        if lfc_task and lfc_task.notebook_task and lfc_task.notebook_task.base_parameters:
            params = lfc_task.notebook_task.base_parameters
            if not runner_conf.lfc_schema or runner_conf.lfc_schema == LFC_DEFAULT_SCHEMA:
                runner_conf.lfc_schema = params.get("source_schema") or runner_conf.lfc_schema
            runner_conf.trigger_interval_min = (
                params.get("trigger_interval_min") or runner_conf.trigger_interval_min
            )
        print(f"  Derived lfc_schema={runner_conf.lfc_schema}")
        print(f"  Derived trigger_interval_min={runner_conf.trigger_interval_min}")

        # Extract pipeline IDs from job that has bronze_dlt/silver_dlt (main or downstream)
        print("Extracting pipeline IDs from job tasks...")
        for t in job_for_downstream.settings.tasks:
            if t.task_key == "bronze_dlt" and t.pipeline_task:
                runner_conf.bronze_pipeline_id = t.pipeline_task.pipeline_id
            elif t.task_key == "silver_dlt" and t.pipeline_task:
                runner_conf.silver_pipeline_id = t.pipeline_task.pipeline_id

        if not runner_conf.bronze_pipeline_id or not runner_conf.silver_pipeline_id:
            raise ValueError(
                f"Could not find pipeline IDs in setup job tasks for run_id={runner_conf.run_id}. "
                "Ensure the setup run completed successfully."
            )
        print(f"  bronze_pipeline_id={runner_conf.bronze_pipeline_id}")
        print(f"  silver_pipeline_id={runner_conf.silver_pipeline_id}")

    # ── resource creation ────────────────────────────────────────────────────

    def _write_conf_files_to_volume(self, runner_conf: LFCRunnerConf):
        """
        Write onboarding.json, silver_transformations.json, and bronze_dqe.json
        directly to the UC Volume via the Files API.
        intpk: source_format=delta + readChangeFeed + bronze/silver_cdc_apply_changes (SCD1).
        dtix:  source_format=snapshot + snapshot_format=delta + bronze/silver_apply_changes_from_snapshot.
               DLT reserves __START_AT/__END_AT globally; init_sdp_meta_pipeline.py renames them
               to lfc_start_at/lfc_end_at via bronze_custom_transform before DLT sees the schema.
        """
        vol = runner_conf.uc_volume_path.rstrip("/")
        onboarding = []
        for i, tbl in enumerate(LFC_TABLES):
            if tbl == "dtix":
                # dtix: LFC SCD2 table has __START_AT/__END_AT which DLT reserves globally.
                # Use apply_changes_from_snapshot (batch snapshot, no CDF) to avoid the
                # reserved-column conflict. The bronze custom transform in
                # init_sdp_meta_pipeline.py renames __START_AT→lfc_start_at and
                # __END_AT→lfc_end_at before DLT analyses the schema.
                entry = {
                    "data_flow_id": str(i + 1),
                    "data_flow_group": "A1",
                    "source_format": "snapshot",
                    "source_details": {
                        "source_catalog_prod": runner_conf.uc_catalog_name,
                        "source_database": runner_conf.lfc_schema,
                        "source_table": tbl,
                        "snapshot_format": "delta",
                    },
                    "bronze_database_prod": (
                        f"{runner_conf.uc_catalog_name}.{runner_conf.bronze_schema}"
                    ),
                    "bronze_table": tbl,
                    "bronze_apply_changes_from_snapshot": LFC_DTIX_BRONZE_APPLY_CHANGES_FROM_SNAPSHOT,
                    "silver_database_prod": (
                        f"{runner_conf.uc_catalog_name}.{runner_conf.silver_schema}"
                    ),
                    "silver_table": tbl,
                    "silver_transformation_json_prod": (
                        f"{vol}/conf/silver_transformations.json"
                    ),
                    "silver_apply_changes_from_snapshot": LFC_DTIX_SILVER_APPLY_CHANGES_FROM_SNAPSHOT,
                }
            else:
                entry = {
                    "data_flow_id": str(i + 1),
                    "data_flow_group": "A1",
                    "source_format": "delta",
                    "source_details": {
                        "source_catalog_prod": runner_conf.uc_catalog_name,
                        "source_database": runner_conf.lfc_schema,
                        "source_table": tbl,
                    },
                    "bronze_database_prod": (
                        f"{runner_conf.uc_catalog_name}.{runner_conf.bronze_schema}"
                    ),
                    "bronze_table": tbl,
                    "bronze_reader_options": LFC_TABLE_BRONZE_READER_OPTIONS.get(tbl, {}),
                    "bronze_database_quarantine_prod": (
                        f"{runner_conf.uc_catalog_name}.{runner_conf.bronze_schema}"
                    ),
                    "bronze_quarantine_table": f"{tbl}_quarantine",
                    "silver_database_prod": (
                        f"{runner_conf.uc_catalog_name}.{runner_conf.silver_schema}"
                    ),
                    "silver_table": tbl,
                    "silver_transformation_json_prod": (
                        f"{vol}/conf/silver_transformations.json"
                    ),
                    "silver_data_quality_expectations_json_prod": (
                        f"{vol}/conf/dqe/silver_dqe.json"
                    ),
                    "bronze_cdc_apply_changes": LFC_INTPK_BRONZE_CDC_APPLY_CHANGES,
                    "bronze_data_quality_expectations_json_prod": (
                        f"{vol}/conf/dqe/bronze_dqe.json"
                    ),
                }
                # Silver reads from bronze intpk via CDF; sequence_by is always _commit_version.
                # The --sequence_by_pk flag no longer changes sequencing (CDF removes that ambiguity).
                entry["silver_reader_options"] = LFC_INTPK_SILVER_READER_OPTIONS
                entry["silver_cdc_apply_changes"] = LFC_INTPK_SILVER_CDC_APPLY_CHANGES
            onboarding.append(entry)

        # Pass-through: select all columns as-is
        silver_transformations = [
            {"target_table": tbl, "select_exp": ["*"]} for tbl in LFC_TABLES
        ]

        bronze_dqe = {"expect": {"valid_row": "true"}}
        silver_dqe = {"expect": {"valid_row": "true"}}

        uploads = {
            f"{vol}/conf/onboarding.json": onboarding,
            f"{vol}/conf/silver_transformations.json": silver_transformations,
            f"{vol}/conf/dqe/bronze_dqe.json": bronze_dqe,
            f"{vol}/conf/dqe/silver_dqe.json": silver_dqe,
        }
        print("Uploading DLT-Meta configuration to UC Volume...")
        for path, content in uploads.items():
            data = json.dumps(content, indent=2).encode()
            self.ws.files.upload(file_path=path, contents=io.BytesIO(data), overwrite=True)
            print(f"  Uploaded {path}")

    def _upload_init_and_lfc_notebooks(self, runner_conf: LFCRunnerConf) -> str:
        """
        Upload init_sdp_meta_pipeline.py, trigger_ingestion_and_wait.py, and
        lfcdemo-database.py to the Databricks workspace.
        Returns the workspace path of the uploaded LFC notebook (without extension).
        """
        from databricks.sdk.service.workspace import Language

        print(f"Uploading notebooks to {runner_conf.runners_nb_path}...")
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/runners")

        for nb_file in (
            "demo/notebooks/lfc_runners/init_sdp_meta_pipeline.py",
            "demo/notebooks/lfc_runners/trigger_ingestion_and_wait.py",
        ):
            nb_name = nb_file.split("/")[-1]
            with open(nb_file, "rb") as f:
                self.ws.workspace.upload(
                    path=f"{runner_conf.runners_nb_path}/runners/{nb_name}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=f.read(),
                    overwrite=True,
                )
            print(f"  Uploaded {nb_name}")

        # lfcdemo-database.py — runs LFC setup. Uploaded as a Databricks
        # notebook source file (Python) so it lines up with all other demo
        # notebooks in this repo (init_sdp_meta_pipeline.py,
        # trigger_ingestion_and_wait.py, etc.) and is reviewable as plain
        # Python in git diffs.
        lfc_nb_ws_path = f"{runner_conf.runners_nb_path}/lfcdemo-database"
        with open("demo/lfcdemo-database.py", "rb") as f:
            self.ws.workspace.upload(
                path=lfc_nb_ws_path,
                format=ImportFormat.SOURCE,
                language=Language.PYTHON,
                content=f.read(),
                overwrite=True,
            )
        print(f"  Uploaded lfcdemo-database.py to {lfc_nb_ws_path}")
        return lfc_nb_ws_path

    def _upload_trigger_ingestion_notebook(self, runner_conf: LFCRunnerConf):
        """Re-upload trigger_ingestion_and_wait.py and init_sdp_meta_pipeline.py for incremental runs.

        Both notebooks are re-uploaded on every incremental so local fixes are picked up without
        requiring a full teardown and re-setup of the run.
        """
        from databricks.sdk.service.workspace import Language
        self.ws.workspace.mkdirs(f"{runner_conf.runners_nb_path}/runners")
        for nb_file in [
            "demo/notebooks/lfc_runners/trigger_ingestion_and_wait.py",
            "demo/notebooks/lfc_runners/init_sdp_meta_pipeline.py",
        ]:
            nb_name = os.path.splitext(os.path.basename(nb_file))[0]
            path = f"{runner_conf.runners_nb_path}/runners/{nb_name}"
            with open(nb_file, "rb") as f:
                self.ws.workspace.upload(
                    path=path,
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=f.read(),
                    overwrite=True,
                )
            print(f"  Uploaded {nb_name} for incremental run.")

    def create_bronze_silver_dlt(self, runner_conf: LFCRunnerConf):
        # Pass the snapshot strategy to the bronze pipeline's Spark conf so that
        # init_sdp_meta_pipeline.py can select between the custom next_snapshot_and_version
        # lambda ("cdf", default) and the built-in view-based full scan ("full").
        bronze_extra_conf = {"dtix_snapshot_method": runner_conf.snapshot_method}
        runner_conf.bronze_pipeline_id = self.create_sdp_meta_pipeline(
            f"{_DEMO_SLUG}-bronze-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
            extra_config=bronze_extra_conf,
        )
        runner_conf.silver_pipeline_id = self.create_sdp_meta_pipeline(
            f"{_DEMO_SLUG}-silver-{runner_conf.run_id}",
            "silver",
            "A1",
            runner_conf.silver_schema,
            runner_conf,
        )

    # ── run orchestration ────────────────────────────────────────────────────

    def run(self, runner_conf: LFCRunnerConf):
        """
        Setup path  : initialize UC resources → upload conf/notebooks/wheel →
                      create DLT pipelines → create and trigger the main job.
        Incremental : re-trigger bronze_dlt → silver_dlt against the current
                      state of the LFC streaming tables.
        """
        try:
            if self._is_incremental():
                self._run_incremental(runner_conf)
            else:
                # 1. Create UC catalog schemas + volume
                self.initialize_uc_resources(runner_conf)
                # 2. Write onboarding/transformation/DQE JSON to the volume
                self._write_conf_files_to_volume(runner_conf)
                # 3. Upload notebooks and wheel
                runner_conf.lfc_notebook_ws_path = self._upload_init_and_lfc_notebooks(runner_conf)
                print("Python wheel upload starting...")
                runner_conf.remote_whl_path = (
                    f"{self.wsi._upload_wheel(uc_volume_path=runner_conf.uc_volume_path)}"
                )
                print(f"Python wheel upload to {runner_conf.remote_whl_path} completed!!!")
                # 4. Create bronze + silver DLT pipelines
                self.create_bronze_silver_dlt(runner_conf)
                # 5. Create and trigger the orchestration job
                self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()

    def _run_incremental(self, runner_conf: LFCRunnerConf):
        """
        Create (first time) or reuse the incremental job, then trigger it.
        First task: trigger LFC ingestion (jobs.run_now) and wait for pipeline update;
        then bronze_dlt → silver_dlt.
        """
        self._upload_trigger_ingestion_notebook(runner_conf)
        incr_job_name = f"{_DEMO_SLUG}-demo-incremental-{runner_conf.run_id}"
        existing_job = next(
            (
                j
                for j in self.ws.jobs.list(name=incr_job_name, limit=JOBS_LIST_LIMIT)
                if j.settings.name == incr_job_name
            ),
            None,
        )
        if existing_job:
            incr_job = existing_job
        else:
            incr_job = self._create_incremental_workflow(runner_conf)
            print(f"Incremental job created. job_id={incr_job.job_id}")

        self.ws.jobs.run_now(job_id=incr_job.job_id)
        url = (
            f"{self.ws.config.host}/jobs/{incr_job.job_id}"
            f"?o={self.ws.get_workspace_id()}"
        )
        webbrowser.open(url)
        print(f"Incremental run triggered. job_id={incr_job.job_id}, url={url}")

    def launch_workflow(self, runner_conf: LFCRunnerConf):
        if runner_conf.parallel_downstream:
            downstream_job = self._create_downstream_only_job(runner_conf)
            runner_conf.downstream_job_id = downstream_job.job_id
        created_job = self._create_lfc_demo_workflow(runner_conf)
        runner_conf.job_id = created_job.job_id
        meta = {"job_id": created_job.job_id, "uc_catalog_name": runner_conf.uc_catalog_name}
        if runner_conf.parallel_downstream and runner_conf.downstream_job_id:
            meta["downstream_job_id"] = runner_conf.downstream_job_id
        self._write_setup_metadata(runner_conf, meta)
        self.ws.jobs.run_now(job_id=created_job.job_id)

        oid = self.ws.get_workspace_id()
        vol_url = (
            f"{self.ws.config.host}/explore/data/volumes/"
            f"{runner_conf.uc_catalog_name}/{runner_conf.sdp_meta_schema}/{runner_conf.uc_volume_name}"
            f"?o={oid}"
        )
        ws_url = f"{self.ws.config.host}/#workspace/Workspace{runner_conf.runners_nb_path}"
        job_url = f"{self.ws.config.host}/jobs/{created_job.job_id}?o={oid}"
        webbrowser.open(job_url)

        profile = self.args.get("profile") or "DEFAULT"
        print(
            f"\n  Volume    : {vol_url}"
            f"\n  Workspace : {ws_url}"
            f"\n  Job       : {job_url}"
            + (
                f"\n  Downstream: {self.ws.config.host}/jobs/{runner_conf.downstream_job_id}?o={oid}"
                if runner_conf.parallel_downstream and runner_conf.downstream_job_id else ""
            )
            + f"\n\nSetup complete!"
            f"\n  run_id : {runner_conf.run_id}"
            f"\nTo re-trigger bronze/silver with the latest LFC data, run:"
            f"\n  python demo/launch_lfc_demo.py --profile={profile} --run_id={runner_conf.run_id}"
            f"\nTo clean up all resources for this run:"
            f"\n  python demo/cleanup_lfc_demo.py --profile={profile} --run_id={runner_conf.run_id}"
        )

    # ── job definitions ──────────────────────────────────────────────────────

    def _downstream_tasks(self, runner_conf: LFCRunnerConf):
        """Onboarding → bronze_dlt → silver_dlt (no dependency on lfc_setup)."""
        return [
            jobs.Task(
                task_key="onboarding_job",
                description="Register LFC streaming tables as DLT-Meta delta sources",
                environment_key="dl_meta_int_env",
                max_retries=0,
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze_silver",
                        "database": (
                            f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}"
                        ),
                        "onboarding_file_path": (
                            f"{runner_conf.uc_volume_path}conf/onboarding.json"
                        ),
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/silver"
                        ),
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "bronze_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/bronze"
                        ),
                        "import_author": _DEMO_SLUG,
                        "version": "v1",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
        ]

    def _create_downstream_only_job(self, runner_conf: LFCRunnerConf):
        """Create job: onboarding_job → bronze_dlt → silver_dlt (triggered by notebook when volume is ready)."""
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        created = self.ws.jobs.create(
            name=f"{_DEMO_SLUG}-demo-{runner_conf.run_id}-downstream",
            environments=dltmeta_environments,
            tasks=self._downstream_tasks(runner_conf),
        )
        self._job_set_no_retry(created.job_id)
        return created

    def _create_lfc_demo_workflow(self, runner_conf: LFCRunnerConf):
        """
        Create the main setup job. If parallel_downstream: single task lfc_setup (notebook
        triggers downstream job when ready). Else: lfc_setup → onboarding_job → bronze_dlt → silver_dlt.
        """
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]

        base_params = {
            "connection": runner_conf.connection_name,
            "cdc_qbc": runner_conf.cdc_qbc,
            "trigger_interval_min": runner_conf.trigger_interval_min,
            "target_catalog": runner_conf.uc_catalog_name,
            "source_schema": runner_conf.lfc_schema,
            "run_id": runner_conf.run_id,
            "sequence_by_pk": str(runner_conf.sequence_by_pk).lower(),
        }
        if runner_conf.parallel_downstream:
            base_params["downstream_job_id"] = str(runner_conf.downstream_job_id)

        lfc_setup_task = jobs.Task(
            task_key="lfc_setup",
            description=(
                "Run lfcdemo-database.py: creates LFC gateway + ingestion pipelines, "
                "starts DML; when parallel_downstream, triggers onboarding→bronze→silver when ready and keeps running"
            ),
            max_retries=0,
            timeout_seconds=0,
            notebook_task=jobs.NotebookTask(
                notebook_path=runner_conf.lfc_notebook_ws_path,
                base_parameters=base_params,
            ),
        )

        if runner_conf.parallel_downstream:
            tasks = [lfc_setup_task]
        else:
            onboarding_task = jobs.Task(
                task_key="onboarding_job",
                description="Register LFC streaming tables as DLT-Meta delta sources",
                depends_on=[jobs.TaskDependency(task_key="lfc_setup")],
                environment_key="dl_meta_int_env",
                max_retries=0,
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze_silver",
                        "database": (
                            f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}"
                        ),
                        "onboarding_file_path": (
                            f"{runner_conf.uc_volume_path}conf/onboarding.json"
                        ),
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/silver"
                        ),
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "bronze_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}data/dlt_spec/bronze"
                        ),
                        "import_author": _DEMO_SLUG,
                        "version": "v1",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            )
            tasks = [
                lfc_setup_task,
                onboarding_task,
                jobs.Task(
                    task_key="bronze_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    max_retries=0,
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_dlt",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                    max_retries=0,
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    ),
                ),
            ]

        created = self.ws.jobs.create(
            name=f"{_DEMO_SLUG}-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=tasks,
        )
        self._job_set_no_retry(created.job_id)
        return created

    def _create_incremental_workflow(self, runner_conf: LFCRunnerConf):
        """
        Create incremental job: trigger_ingestion_and_wait → bronze_dlt → silver_dlt.
        Trigger task runs the LFC scheduler job once (jobs.run_now) and waits for the
        ingestion pipeline's latest update to complete before bronze/silver run.
        """
        trigger_nb_path = f"{runner_conf.runners_nb_path}/runners/trigger_ingestion_and_wait.py"
        tasks = [
            jobs.Task(
                task_key="trigger_ingestion_and_wait",
                description="Trigger LFC ingestion (jobs.run_now) and wait for pipeline update to complete",
                max_retries=0,
                notebook_task=jobs.NotebookTask(
                    notebook_path=trigger_nb_path,
                    base_parameters={
                        "run_id": runner_conf.run_id,
                        "target_catalog": runner_conf.uc_catalog_name,
                        "trigger_interval_min": runner_conf.trigger_interval_min,
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="trigger_ingestion_and_wait")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                max_retries=0,
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
        ]
        created = self.ws.jobs.create(
            name=f"{_DEMO_SLUG}-demo-incremental-{runner_conf.run_id}",
            tasks=tasks,
        )
        self._job_set_no_retry(created.job_id)
        return created


lfc_args_map = {
    "--profile": "Databricks CLI profile name (default: DEFAULT)",
    "--uc_catalog_name": "Unity Catalog name — required for setup, derived from job in incremental mode",
    "--source_schema": "Source schema on the source database (default: lfcddemo)",
    "--connection_name": "Databricks connection name for source DB (e.g. lfcddemo-azure-sqlserver)",
    "--cdc_qbc": "LFC pipeline mode: cdc | qbc | cdc_single_pipeline (default: cdc)",
    "--trigger_interval_min": "LFC trigger interval in minutes — positive integer (default: 5)",
    "--sequence_by_pk": "Use primary key for CDC silver sequence_by; default: use dt column",
    "--no_parallel_downstream": (
        "Disable parallel downstream (single job: lfc_setup → onboarding → bronze → silver)."
        " Default: parallel_downstream is on."
    ),
    "--run_id": "Existing run_id to re-trigger bronze/silver; implies incremental mode",
}

lfc_mandatory_args = ["uc_catalog_name", "connection_name"]


def main():
    args = process_arguments()
    if not args.get("run_id"):
        for required in lfc_mandatory_args:
            if not args.get(required):
                raise SystemExit(
                    f"Error: --{required} is required for a new setup run. "
                    f"(Pass --run_id to resume an existing run.)"
                )
    ws = get_workspace_api_client(args["profile"])
    runner = DLTMETALFCDemo(args, ws, "demo")
    print("initializing complete")
    runner_conf = runner.init_runner_conf()
    runner.run(runner_conf)


if __name__ == "__main__":
    main()
