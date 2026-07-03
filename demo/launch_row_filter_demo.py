"""End-to-end demo for SDP-META `bronze_row_filter` / `silver_row_filter`.

Standalone counterpart to `launch_silver_fanout_demo.py`. Stages, in
order, against a UC catalog the caller owns:

1. Generate per-run schemas + UC volume; render the row-filter
   onboarding template into per-run JSON / YAML and upload the demo
   data fixtures + onboarding spec to the volume.
2. Build ONE combined Lakeflow Spark Declarative Pipeline that
   materializes both bronze + silver in a single DAG via
   `layer=bronze_silver` (the same combined-mode topology the
   sdp-meta DAB template emits).
3. Submit a Databricks workflow with this task graph:

       setup_row_filter_udf  ->  onboarding_job  ->  sdp_meta_pipeline
                                                                  ->  validate

   * `setup_row_filter_udf` runs FIRST and creates the row-filter UDF
     `<catalog>.<bronze_schema>.region_filter`. The onboarding rows
     reference this UDF via `bronze_row_filter` / `silver_row_filter`
     -- UC fails `CREATE TABLE` if the function isn't there when the
     pipeline first creates the target table, so this notebook
     CANNOT run after onboarding.
   * `onboarding_job` writes the dataflow spec rows into BOTH the
     `bronze_dataflowspec_cdc` and `silver_dataflowspec_cdc` tables.
   * `sdp_meta_pipeline` is the single combined SDP -- it reads both
     dataflowspec tables and materializes bronze + silver in one DAG.
   * `validate` reads bronze + silver `customers` and asserts the
     filter is enforced (non-admin sees only `region IN ('US','UK')`,
     totalling 8 of the 16 source rows; admin sees all 16).

Usage:
    python demo/launch_row_filter_demo.py \
        --uc_catalog_name <your_catalog> \
        --profile <your_profile>

Optional:
    --onboarding_file_format yaml   (default json)
"""

import os
import sys
import uuid
import traceback

# Make `integration_tests` importable when launched as
# `python demo/launch_row_filter_demo.py` from the repo root, without
# requiring the user to first `export PYTHONPATH=$(pwd)`. The sibling
# demos in this folder (launch_dais_demo.py etc.) rely on that env var
# being set externally; this one self-bootstraps so a fresh checkout
# can run the demo with a single command.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from databricks.sdk.service import jobs, compute  # noqa: E402
from databricks.sdk.service.pipelines import (  # noqa: E402
    NotebookLibrary,
    PipelineLibrary,
)

from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: E402
from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments,
)


class SDPMETARowFilterDemo(SDPMETARunner):
    """End-to-end demo for `bronze_row_filter` / `silver_row_filter`."""

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # Intentionally not auto-cleaning so an operator can inspect
        # the bronze / silver tables and the row-filter UDF after the
        # run. Drop manually with `DROP CATALOG ... CASCADE` or by
        # deleting the per-run schemas listed in the run output.

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_rls_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_rls_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_rls_demo_{run_id}",
            runners_nb_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_rls_demo/{run_id}"
            ),
            runners_full_local_path="demo/notebooks/row_filter_runners",
            source="cloudfiles",
            # The runner expects A1 + A2 templates for source=cloudfiles
            # (see `generate_onboarding_file`). We only run a single
            # bronze + silver pair for the row-filter narrative -- the
            # A2 template is reused as a harmless duplicate that lands
            # in the `_A2` onboarding file and is then ignored by the
            # overridden `create_bronze_silver_dlt` below (which does
            # NOT create an A2 pipeline).
            cloudfiles_template="demo/conf/json/row_filter-onboarding.template",
            cloudfiles_A2_template="demo/conf/json/row_filter-onboarding.template",
            onboarding_file_path="demo/conf/json/row_filter_onboarding.json",
            onboarding_A2_file_path="demo/conf/json/row_filter_onboarding_A2.json",
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
            env="demo",
        )
        runner_conf.uc_catalog_name = self.args["uc_catalog_name"]
        return runner_conf

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        """Create ONE combined Lakeflow Spark Declarative Pipeline that
        materializes bronze + silver in a single DAG (`layer=bronze_silver`),
        instead of two separate pipelines. Mirrors the
        `pipeline_mode=combined` topology the DAB template produces (see
        `templates/dab/.../sdp_meta_pipelines.yml.tmpl`).

        sdp-meta dispatches on `layer=bronze_silver` and reads BOTH
        `bronze.dataflowspecTable` and `silver.dataflowspecTable` from
        the pipeline configuration -- so the onboarding job still
        populates both `bronze_dataflowspec_cdc` and
        `silver_dataflowspec_cdc` exactly as in split mode."""
        pipeline_id = self._create_combined_pipeline(
            f"sdp-meta-pipeline-rls-demo-{runner_conf.run_id}",
            "A1",
            runner_conf,
        )
        # Stash on both fields so any parent-class hook that reads
        # `bronze_pipeline_id` / `silver_pipeline_id` keeps working.
        # The workflow spec below references `bronze_pipeline_id` only
        # via the single sdp_meta_pipeline task.
        runner_conf.bronze_pipeline_id = pipeline_id
        runner_conf.silver_pipeline_id = pipeline_id

    def _create_combined_pipeline(
        self,
        pipeline_name: str,
        group: str,
        runner_conf: SDPMetaRunnerConf,
    ) -> str:
        catalog = runner_conf.uc_catalog_name
        sdp_meta_schema = runner_conf.sdp_meta_schema
        configuration = {
            "layer": "bronze_silver",
            "bronze.dataflowspecTable": (
                f"{catalog}.{sdp_meta_schema}.bronze_dataflowspec_cdc"
            ),
            "bronze.group": group,
            "silver.dataflowspecTable": (
                f"{catalog}.{sdp_meta_schema}.silver_dataflowspec_cdc"
            ),
            "silver.group": group,
            "sdp_meta_whl": runner_conf.remote_whl_path,
            "pipelines.externalSink.enabled": "true",
        }
        # `schema` is the SDP-level default; sdp-meta still writes each
        # table to its own (catalog, schema) from the dataflowspec rows
        # (bronze rows -> bronze_schema, silver rows -> silver_schema).
        created = self.ws.pipelines.create(
            catalog=catalog,
            name=pipeline_name,
            serverless=True,
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=(
                            f"{runner_conf.runners_nb_path}"
                            f"/runners/init_sdp_meta_pipeline.py"
                        )
                    )
                )
            ],
            schema=runner_conf.bronze_schema,
        )
        if created is None:
            raise Exception("Combined sdp-meta pipeline creation failed")
        return created.pipeline_id

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self._create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)

    def _create_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_int_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]

        return self.ws.jobs.create(
            name=f"sdp-meta-row-filter-demo-{runner_conf.run_id}",
            environments=sdp_meta_environments,
            tasks=[
                # 1) Create the row-filter UDF FIRST. The bronze table
                # creation that happens later in `bronze_dlt` will fail
                # with "function does not exist" if this task hasn't run.
                jobs.Task(
                    task_key="setup_row_filter_udf",
                    description=(
                        "Create the UC row-filter UDF that the bronze "
                        "and silver onboarding rows reference."
                    ),
                    timeout_seconds=0,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=(
                            f"{runner_conf.runners_nb_path}"
                            f"/runners/setup_row_filter_udf.py"
                        ),
                        base_parameters={
                            "uc_catalog_name": runner_conf.uc_catalog_name,
                            "bronze_schema": runner_conf.bronze_schema,
                        },
                    ),
                ),
                # 2) Populate bronze_dataflowspec_cdc /
                # silver_dataflowspec_cdc from the rendered
                # row-filter onboarding spec.
                jobs.Task(
                    task_key="onboarding_job",
                    description="Populate bronze + silver dataflow specs.",
                    depends_on=[
                        jobs.TaskDependency(task_key="setup_row_filter_udf")
                    ],
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": (
                                f"{runner_conf.uc_catalog_name}"
                                f".{runner_conf.sdp_meta_schema}"
                            ),
                            "onboarding_file_path": (
                                f"{runner_conf.uc_volume_path}"
                                f"{runner_conf.onboarding_file_path}"
                            ),
                            "silver_dataflowspec_table": (
                                "silver_dataflowspec_cdc"
                            ),
                            "bronze_dataflowspec_table": (
                                "bronze_dataflowspec_cdc"
                            ),
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True",
                        },
                    ),
                ),
                # 3) Combined sdp-meta pipeline -- materializes BOTH
                # bronze.customers (with `bronze_row_filter`) and
                # silver.customers (with `silver_row_filter`) in a
                # single SDP DAG via `layer=bronze_silver`.
                jobs.Task(
                    task_key="sdp_meta_pipeline",
                    description=(
                        "Combined bronze+silver sdp-meta Lakeflow "
                        "Spark Declarative Pipeline."
                    ),
                    depends_on=[
                        jobs.TaskDependency(task_key="onboarding_job")
                    ],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                # 4) Verify the filter is enforced. Fails the workflow
                # if a non-admin reader can see DE / JP rows.
                jobs.Task(
                    task_key="validate",
                    description="Assert the row filter is enforced.",
                    depends_on=[
                        jobs.TaskDependency(task_key="sdp_meta_pipeline")
                    ],
                    timeout_seconds=0,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=(
                            f"{runner_conf.runners_nb_path}"
                            f"/runners/validate.py"
                        ),
                        base_parameters={
                            "uc_catalog_name": runner_conf.uc_catalog_name,
                            "bronze_schema": runner_conf.bronze_schema,
                            "silver_schema": runner_conf.silver_schema,
                        },
                    ),
                ),
            ],
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    runner = SDPMETARowFilterDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = runner.init_runner_conf()
    runner.run(runner_conf)


if __name__ == "__main__":
    main()
