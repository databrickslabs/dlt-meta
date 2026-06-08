"""SDP-META Multi-Source AUTO CDC demo launcher (issue #294).

The demo provisions:

* Three regional bronze CDC tables (US / EU / APAC), each landing raw
  customer CDC events from its own folder under ``demo/resources/data/
  multi_source_cdc/``. Each region uses a different column shape on
  purpose so the per-flow ``select_exp`` normalization on silver is
  actually doing something the user can see.
* One unified silver ``customers`` SCD-1 table that pulls from all three
  bronze tables via ``silver_cdc_apply_changes_flows``. Each flow
  rewrites its source columns into the canonical
  ``(customer_id, firstname, lastname, email, address, region)`` shape
  before the merge, so DLT sees a single consistent input to apply
  changes from.

The launcher follows the **same single-pipeline pattern as Stage 11 of
the interactive demo notebook** (``demo/SDP_META_INTERACTIVE_DEMO.py``):
ONE Lakeflow Spark Declarative Pipeline runs both bronze and silver
layers together (``layer=bronze_silver`` with ``bronze.group=A1`` and
``silver.group=A1``). This keeps the multi-source CDC fan-in
(N bronze views → one silver target) inside a single observable DLT
flow graph, which is the whole point of the feature.
"""

import sys
import traceback
import uuid
from pathlib import Path

# Importing from `src/` and `integration_tests/` after a small path shim so
# the script runs straight from a clone without requiring the user to
# export PYTHONPATH first. Mirrors the pattern in
# demo/launch_interactive_demo.py.
REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (REPO_ROOT, REPO_ROOT / "src"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from databricks.labs.sdp_meta.install import WorkspaceInstaller  # noqa: E402
from databricks.sdk.service import compute, jobs  # noqa: E402
from databricks.sdk.service.pipelines import (  # noqa: E402
    NotebookLibrary,
    PipelineLibrary,
)

from integration_tests.run_integration_tests import (  # noqa: E402
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments,
)


class SDPMETAMultiSourceCDCDemo(SDPMETARunner):
    """Multi-source AUTO CDC into a single silver target (issue #294).

    Subclasses :class:`SDPMETARunner` so we inherit:
      * UC schema / volume creation
      * Template substitution + onboarding-file generation
      * Wheel upload + notebook upload

    We override :meth:`create_bronze_silver_dlt` to create **one
    combined** ``bronze_silver`` pipeline (matching Stage 11 of the
    interactive demo notebook), instead of the parent's default of two
    separate pipelines. We also override :meth:`launch_workflow` to point
    at a small 3-task job (``onboarding_job -> sdp-meta-pipeline ->
    validate_results``) because the standard ``create_workflow_spec``
    assumes the two-pipeline shape.
    """

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
        # finally:
        #     self.clean_up(runner_conf)

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_msc_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_msc_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_msc_demo_{run_id}",
            runners_nb_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_msc_demo/{run_id}"
            ),
            runners_full_local_path=(
                "./demo/notebooks/multi_source_cdc_runners/"
            ),
            source="cloudfiles",
            cloudfiles_template=(
                "demo/conf/json/multi-source-cdc-onboarding.template"
            ),
            # The standard runner code generates an A2 onboarding file
            # too. We keep the default A2 template path so the file gets
            # generated (and then ignored by our custom workflow below).
            # Same trick as launch_silver_fanout_demo.
            onboarding_file_path="demo/conf/json/onboarding_msc.json",
            onboarding_A2_file_path="demo/conf/json/onboarding_msc_A2.json",
            onboarding_file_format=(
                self.args.get("onboarding_file_format") or "json"
            ),
            env="demo",
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_msc_demo/"
                f"{run_id}/demo-output.csv"
            ),
        )
        return runner_conf

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        """Create a SINGLE combined ``bronze_silver`` pipeline.

        Overrides the parent's default of two separate pipelines so the
        whole multi-source CDC fan-in lives inside one observable DLT
        flow graph, exactly like Stage 11 of the interactive demo
        notebook. The combined pipeline:

          * runs ``layer=bronze_silver``
          * reads bronze dataflow specs from
            ``{layer}_dataflowspec_cdc`` (group ``A1``)
          * reads silver dataflow specs from the same table family,
            also group ``A1``
          * uses the same generic ``init_sdp_meta_pipeline.py`` runner
            notebook that the standard demo uses

        We stash the resulting pipeline id on
        ``runner_conf.bronze_pipeline_id`` so the parent's ``clean_up``
        deletes it; ``silver_pipeline_id`` stays ``None`` because there
        is no separate silver pipeline to clean up.
        """
        configuration = {
            "layer": "bronze_silver",
            "bronze.group": "A1",
            "silver.group": "A1",
            "bronze.dataflowspecTable": (
                f"{runner_conf.uc_catalog_name}."
                f"{runner_conf.sdp_meta_schema}.bronze_dataflowspec_cdc"
            ),
            "silver.dataflowspecTable": (
                f"{runner_conf.uc_catalog_name}."
                f"{runner_conf.sdp_meta_schema}.silver_dataflowspec_cdc"
            ),
            "sdp_meta_whl": runner_conf.remote_whl_path,
            "pipelines.externalSink.enabled": "true",
        }
        created = self.ws.pipelines.create(
            catalog=runner_conf.uc_catalog_name,
            # Pipeline resource name mirrors the workflow task_key
            # (``sdp-meta-pipeline``) so the Pipelines UI and the Jobs
            # UI line up at a glance for this combined MSC run.
            name=f"sdp-meta-pipeline-{runner_conf.run_id}",
            serverless=True,
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=(
                            f"{runner_conf.runners_nb_path}/runners/"
                            "init_sdp_meta_pipeline.py"
                        )
                    )
                )
            ],
            # Lakeflow direct publishing mode requires a pipeline-level
            # target schema, but DataflowPipeline writes every table at
            # its own (bronze_database_<env> / silver_database_<env>)
            # destination via DataflowSpec, so this is effectively a
            # placeholder. Using ``bronze_schema`` here mirrors what
            # Stage 11 of the interactive notebook does.
            schema=runner_conf.bronze_schema,
        )
        if created is None:
            raise Exception("Combined bronze+silver pipeline creation failed")
        runner_conf.bronze_pipeline_id = created.pipeline_id
        # No separate silver pipeline in this demo — it's fused into the
        # combined pipeline above. Leave ``silver_pipeline_id`` unset so
        # the parent's clean_up doesn't try to delete a non-existent id.
        runner_conf.silver_pipeline_id = None

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_msc_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)

    def create_msc_workflow_spec(self, runner_conf: SDPMetaRunnerConf):
        """3-task workflow: onboarding -> sdp-meta-pipeline -> validate.

        The single ``sdp-meta-pipeline`` task runs the combined
        Lakeflow Spark Declarative Pipeline that processes BOTH bronze
        (3 regional CDC tables) and silver (multi-source AUTO CDC into
        the unified ``customers`` target) inside one DLT flow graph.
        Matches Stage 11 of the interactive demo notebook.
        """
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="sdp_meta_msc_demo_env",
                spec=compute.Environment(
                    client="1",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        tasks = [
            jobs.Task(
                task_key="onboarding_job",
                description=(
                    "Onboard bronze + silver dataflow specs from the "
                    "multi-source CDC template"
                ),
                environment_key="sdp_meta_msc_demo_env",
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze_silver",
                        "database": (
                            f"{runner_conf.uc_catalog_name}."
                            f"{runner_conf.sdp_meta_schema}"
                        ),
                        "onboarding_file_path": (
                            f"{runner_conf.uc_volume_path}"
                            f"{runner_conf.onboarding_file_path}"
                        ),
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}"
                            f"data/sdp_meta_msc/silver"
                        ),
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "bronze_dataflowspec_path": (
                            f"{runner_conf.uc_volume_path}"
                            f"data/sdp_meta_msc/bronze"
                        ),
                        "import_author": "sdp-meta-demo",
                        "version": "v1",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True",
                    },
                ),
            ),
            jobs.Task(
                # Named ``sdp-meta-pipeline`` (not
                # ``bronze_silver_dlt``) because this single task runs
                # the combined Lakeflow Spark Declarative Pipeline that
                # contains BOTH the bronze and the silver layers — the
                # workflow graph in the Jobs UI should reflect that.
                task_key="sdp-meta-pipeline",
                description=(
                    "Combined bronze + silver pipeline: 3 regional CDC "
                    "bronze tables (customers_us_cdc, customers_eu_cdc, "
                    "customers_apac_cdc) plus the unified silver "
                    "customers table merged via multi-source AUTO CDC"
                ),
                depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="validate_results",
                description=(
                    "Assert per-region counts in bronze and the merged "
                    "row count in silver"
                ),
                depends_on=[
                    jobs.TaskDependency(task_key="sdp-meta-pipeline")
                ],
                notebook_task=jobs.NotebookTask(
                    notebook_path=(
                        f"{runner_conf.runners_nb_path}/runners/validate.py"
                    ),
                    base_parameters={
                        "uc_enabled": "True",
                        "uc_catalog_name": runner_conf.uc_catalog_name,
                        "bronze_schema": runner_conf.bronze_schema,
                        "silver_schema": runner_conf.silver_schema,
                        "output_file_path": (
                            f"/Workspace{runner_conf.test_output_file_path}"
                        ),
                        "run_id": runner_conf.run_id,
                    },
                ),
            ),
        ]
        return self.ws.jobs.create(
            name=f"sdp-meta-multi-source-cdc-demo-{runner_conf.run_id}",
            environments=sdp_meta_environments,
            tasks=tasks,
        )


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    demo_runner = SDPMETAMultiSourceCDCDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = demo_runner.init_runner_conf()
    demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
