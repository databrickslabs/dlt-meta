"""
This script is used to launch the DLT-META Databricks Techsummit Demo. It contains classes and methods
to initialize the runner configuration, create and launch the workflow, and perform other necessary tasks.

Classes:
- TechsummitRunnerConf: Dataclass to store the configuration parameters for the TechsummitRunner.
- DLTMETATechSummitDemo: Class to run the DLT-META Databricks Techsummit Demo.

Methods:
- init_runner_conf(): Initializes the TechsummitRunnerConf object with the provided configuration parameters.
- init_sdp_meta_runner_conf(runner_conf): Initializes the DLT-META runner configuration by uploading the necessary files
  and creating the required schemas and volumes.
- run(runner_conf): Runs the DLT-META Techsummit Demo by calling the necessary methods in the correct order.
- launch_workflow(runner_conf): Launches the workflow for the Techsummit Demo by creating the necessary tasks and
  submitting the job.
- create_techsummit_demo_workflow(runner_conf): Creates the workflow for the Techsummit Demo by defining the tasks
  and their dependencies.

Functions:
- main(): Entry method to run the integration tests.

Note: This script requires certain command line arguments to be provided in order to run successfully.
"""

import uuid
import traceback
import webbrowser
from databricks.sdk.service import jobs, compute
from dataclasses import dataclass
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


@dataclass
class TechsummitRunnerConf(SDPMetaRunnerConf):
    """
    A dataclass to store the configuration parameters for the TechsummitRunner.

    Attributes:
    - table_count: The number of tables to be generated.
    - table_column_count: The number of columns in each table.
    - table_data_rows_count: The number of rows of data in each table.
    - worker_nodes: The number of worker nodes to be used.
    """
    table_count: str = None
    table_column_count: str = None
    table_data_rows_count: str = None
    worker_nodes: str = None


class DLTMETATechSummitDemo(SDPMETARunner):
    """
    A class to run the DLT-META Databricks Techsummit Demo.

    Attributes:
    - args: Command line arguments.
    - workspace_client: Databricks workspace client.
    - base_dir: Base directory.
    """
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def _is_incremental(self) -> bool:
        """True when --run_id is supplied, implying incremental mode."""
        return bool(self.args.get("run_id"))

    def init_runner_conf(self) -> TechsummitRunnerConf:
        """
        Initializes the TechsummitRunnerConf object with the provided configuration parameters.
        When --run_id is supplied the existing demo resources are reused (incremental mode).

        Returns:
        - runner_conf: The initialized TechsummitRunnerConf object.
        """
        if self._is_incremental():
            run_id = self.args["run_id"]
        else:
            run_id = uuid.uuid4().hex
        runner_conf = TechsummitRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_demo_{run_id}",
            runners_full_local_path='demo/notebooks/techsummit_runners',
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/sdp_meta_techsummit_demo/{run_id}",
            int_tests_dir="demo",
            env="prod",
            table_count=str(self.args.get('table_count') or "100"),
            table_column_count=str(self.args.get('table_column_count') or "5"),
            table_data_rows_count=str(self.args.get('table_data_rows_count') or "10"),
        )
        if self.args['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args['uc_catalog_name']
            runner_conf.uc_volume_name = f"{self.args['uc_catalog_name']}_volume_{run_id}"
        if self._is_incremental():
            self._resolve_incremental_conf(runner_conf)
        return runner_conf

    def _resolve_incremental_conf(self, runner_conf: TechsummitRunnerConf):
        """
        Populate uc_catalog_name (if not supplied) and bronze/silver pipeline IDs
        by inspecting the existing setup job and pipelines for this run_id.
        """
        setup_job_name = f"sdp-meta-techsummit-demo-{runner_conf.run_id}"
        print(f"Looking up setup job '{setup_job_name}'...")
        setup_job = next(
            (j for j in self.ws.jobs.list(name=setup_job_name) if j.settings.name == setup_job_name),
            None,
        )
        if not setup_job:
            raise ValueError(
                f"Setup job '{setup_job_name}' not found. "
                "Ensure the original setup run completed successfully."
            )
        print(f"  Found job_id={setup_job.job_id}")
        job_details = self.ws.jobs.get(job_id=setup_job.job_id)

        if not runner_conf.uc_catalog_name:
            # Derive uc_catalog_name from the onboarding_job task's "database" parameter,
            # which is stored as "{uc_catalog_name}.{dlt_meta_schema}"
            onboarding_task = next(
                (t for t in job_details.settings.tasks if t.task_key == "onboarding_job"),
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
            runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_volume_{runner_conf.run_id}"
            print(f"  Derived uc_catalog_name={runner_conf.uc_catalog_name}")

        # Always derive uc_volume_path from catalog/schema/volume names —
        # initialize_uc_resources is not called in incremental mode so it must be set here.
        runner_conf.uc_volume_path = (
            f"/Volumes/{runner_conf.uc_catalog_name}/"
            f"{runner_conf.sdp_meta_schema}/{runner_conf.uc_volume_name}/"
        )

        # Inherit table generation params from the setup job so incremental runs
        # generate the same number of tables/columns/rows as the original setup.
        gen_task = next(
            (t for t in job_details.settings.tasks if t.task_key == "generate_data"),
            None,
        )
        if gen_task and gen_task.notebook_task and gen_task.notebook_task.base_parameters:
            p = gen_task.notebook_task.base_parameters
            runner_conf.table_count        = p.get("table_count",        runner_conf.table_count)
            runner_conf.table_column_count = p.get("table_column_count", runner_conf.table_column_count)
            runner_conf.table_data_rows_count = p.get("table_data_rows_count", runner_conf.table_data_rows_count)
            print(
                f"  Inherited from setup: table_count={runner_conf.table_count}, "
                f"table_column_count={runner_conf.table_column_count}, "
                f"table_data_rows_count={runner_conf.table_data_rows_count}"
            )

        # Extract pipeline IDs from the setup job's task definitions — faster and
        # avoids list_pipelines() whose filter= parameter chokes on hyphens in names.
        print(f"Extracting pipeline IDs from setup job tasks...")
        for t in job_details.settings.tasks:
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

    def create_bronze_silver_dlt(self, runner_conf: SDPMetaRunnerConf):
        runner_conf.bronze_pipeline_id = self.create_sdp_meta_pipeline(
            f"sdp-meta-bronze-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
        )

        runner_conf.silver_pipeline_id = self.create_sdp_meta_pipeline(
            f"sdp-meta-silver-{runner_conf.run_id}",
            "silver",
            "A1",
            runner_conf.silver_schema,
            runner_conf,
        )

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the DLT-META Techsummit Demo by calling the necessary methods in the correct order.
        When --run_id is supplied, runs in incremental mode: generates additional data and
        re-triggers the existing job without recreating any resources.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            if self._is_incremental():
                self._run_incremental(runner_conf)
            else:
                self.init_sdp_meta_runner_conf(runner_conf)
                self.create_bronze_silver_dlt(runner_conf)
                self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

    def _run_incremental(self, runner_conf: TechsummitRunnerConf):
        """
        Generate additional data into the existing UC Volume and re-trigger the DLT pipelines.
        The incremental job is created on first use and reused on subsequent calls.
        The bronze/silver DLT pipelines use AutoLoader (cloudFiles) and automatically
        pick up the new files on each run.
        """
        incremental_job_name = f"sdp-meta-techsummit-demo-incremental-{runner_conf.run_id}"
        existing_job = next(
            (j for j in self.ws.jobs.list() if j.settings.name == incremental_job_name),
            None,
        )
        if existing_job:
            incremental_job = existing_job
        else:
            incremental_job = self.create_incremental_workflow(runner_conf)
            print(f"Incremental job created. job_id={incremental_job.job_id}")
        self.ws.jobs.run_now(job_id=incremental_job.job_id)
        url = f"{self.ws.config.host}/jobs/{incremental_job.job_id}?o={self.ws.get_workspace_id()}"
        webbrowser.open(url)
        print(f"Incremental run triggered. job_id={incremental_job.job_id}, url={url}")

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        """
        Launches the workflow for the Techsummit Demo by creating the necessary tasks and submitting the job.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.
        """
        created_job = self.create_techsummit_demo_workflow(runner_conf)
        self.open_job_url(runner_conf, created_job)
        profile = self.args.get("profile") or "DEFAULT"
        print(
            f"\nSetup complete!"
            f"\n  run_id : {runner_conf.run_id}"
            f"\nTo load incremental data, run:"
            f"\n  python demo/launch_techsummit_demo.py --profile={profile} --run_id={runner_conf.run_id}"
        )

    def create_techsummit_demo_workflow(self, runner_conf: TechsummitRunnerConf):
        """
        Creates the workflow for the Techsummit Demo by defining the tasks and their dependencies.

        Parameters:
        - runner_conf: The TechsummitRunnerConf object containing the runner configuration parameters.

        Returns:
        - created_job: The created job object.
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
        tasks = [
            jobs.Task(
                task_key="generate_data",
                description="Generate Test Data and Onboarding Files",
                timeout_seconds=0,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{runner_conf.runners_nb_path}/runners/data_generator.py",
                    base_parameters={
                        "base_input_path": runner_conf.uc_volume_path,
                        "table_column_count": runner_conf.table_column_count,
                        "table_count": runner_conf.table_count,
                        "table_data_rows_count": runner_conf.table_data_rows_count,
                        "uc_catalog_name": runner_conf.uc_catalog_name,
                        "dlt_meta_schema": runner_conf.sdp_meta_schema,
                        "bronze_schema": runner_conf.bronze_schema,
                        "silver_schema": runner_conf.silver_schema,
                    }
                ),
            ),
        ]

        tasks.extend([
            jobs.Task(
                task_key="onboarding_job",
                description="Sets up metadata tables for DLT-META",
                depends_on=[jobs.TaskDependency(task_key="generate_data")],
                environment_key="dl_meta_int_env",
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="databricks_labs_sdp_meta",
                    entry_point="run",
                    named_parameters={
                        "onboard_layer": "bronze_silver",
                        "database": f"{runner_conf.uc_catalog_name}.{runner_conf.sdp_meta_schema}",
                        "onboarding_file_path": f"{runner_conf.uc_volume_path}/conf/onboarding.json",
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": f"{runner_conf.uc_volume_path}/data/dlt_spec/silver",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{runner_conf.uc_volume_path}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": runner_conf.env,
                        "uc_enabled": "True" if runner_conf.uc_catalog_name else "False"
                    }
                )
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                )
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                )
            )
        ])

        return self.ws.jobs.create(
            name=f"sdp-meta-techsummit-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=tasks,
        )

    def create_incremental_workflow(self, runner_conf: TechsummitRunnerConf):
        """
        Creates a companion job for incremental data loads. This job can be re-run from the
        Databricks UI (or CLI) at any time to append new data and re-trigger the DLT pipelines.
        It does not recreate any resources — it reuses the existing volume, schemas, and pipelines.
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
        tasks = [
            jobs.Task(
                task_key="generate_incremental_data",
                description="Append new data rows to existing source tables",
                timeout_seconds=0,
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{runner_conf.runners_nb_path}/runners/data_generator.py",
                    base_parameters={
                        "base_input_path": runner_conf.uc_volume_path,
                        "table_column_count": runner_conf.table_column_count,
                        "table_count": runner_conf.table_count,
                        "table_data_rows_count": runner_conf.table_data_rows_count,
                        "uc_catalog_name": runner_conf.uc_catalog_name,
                        "dlt_meta_schema": runner_conf.sdp_meta_schema,
                        "bronze_schema": runner_conf.bronze_schema,
                        "silver_schema": runner_conf.silver_schema,
                        "mode": "incremental",
                    },
                ),
            ),
            jobs.Task(
                task_key="bronze_dlt",
                depends_on=[jobs.TaskDependency(task_key="generate_incremental_data")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.bronze_pipeline_id
                ),
            ),
            jobs.Task(
                task_key="silver_dlt",
                depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                pipeline_task=jobs.PipelineTask(
                    pipeline_id=runner_conf.silver_pipeline_id
                ),
            ),
        ]
        return self.ws.jobs.create(
            name=f"sdp-meta-techsummit-demo-incremental-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=tasks,
        )


techsummit_args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
                       "--uc_catalog_name": "provide databricks uc_catalog name, \
                                            this is required to create volume, schema, table",
                       "--table_count": "table_count",
                       "--table_column_count": "table_column_count",
                       "--table_data_rows_count": "table_data_rows_count",
                       "--run_id": "existing run_id to resume; presence implies incremental mode"
                       }

techsummit_mandatory_args = ["uc_catalog_name"]


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    dltmeta_techsummit_demo_runner = DLTMETATechSummitDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_techsummit_demo_runner.init_runner_conf()
    dltmeta_techsummit_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
