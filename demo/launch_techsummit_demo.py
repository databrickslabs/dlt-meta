"""
This script is used to launch the DLT-META Databricks Techsummit Demo. It contains classes and methods
to initialize the runner configuration, create and launch the workflow, and perform other necessary tasks.

Classes:
- TechsummitRunnerConf: Dataclass to store the configuration parameters for the TechsummitRunner.
- DLTMETATechSummitDemo: Class to run the DLT-META Databricks Techsummit Demo.

Methods:
- init_runner_conf(): Initializes the TechsummitRunnerConf object with the provided configuration parameters.
- init_dltmeta_runner_conf(runner_conf): Initializes the DLT-META runner configuration by uploading the necessary files
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
from databricks.sdk.service import jobs, compute
from dataclasses import dataclass
from src.install import WorkspaceInstaller
from src.__about__ import __version__
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


@dataclass
class TechsummitRunnerConf(DLTMetaRunnerConf):
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


class DLTMETATechSummitDemo(DLTMETARunner):
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

    def init_runner_conf(self) -> TechsummitRunnerConf:
        """
        Initializes the TechsummitRunnerConf object with the provided configuration parameters.

        Returns:
        - runner_conf: The initialized TechsummitRunnerConf object.
        """
        run_id = uuid.uuid4().hex
        runner_conf = TechsummitRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_full_local_path='demo/notebooks/techsummit_runners',
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/dlt_meta_techsummit_demo/{run_id}",
            int_tests_dir="demo",
            env="prod",
            table_count=(
                self.args.__dict__['table_count']
                if 'table_count' in self.args and self.args.__dict__['table_count']
                else "100"
            ),
            table_column_count=(
                self.args.__dict__['table_column_count']
                if 'table_column_count' in self.args and self.args.__dict__['table_column_count']
                else "5"
            ),
            table_data_rows_count=(self.args.__dict__['table_data_rows_count']
                                   if 'table_data_rows_count' in self.args
                                   and self.args.__dict__['table_data_rows_count']
                                   else "10"),
        )
        if self.args['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args['uc_catalog_name']
            runner_conf.uc_volume_name = f"{self.args['uc_catalog_name']}_volume_{run_id}"
        return runner_conf

    def create_bronze_silver_dlt(self, runner_conf: DLTMetaRunnerConf):
        runner_conf.bronze_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-bronze-{runner_conf.run_id}",
            "bronze",
            "A1",
            runner_conf.bronze_schema,
            runner_conf,
        )

        runner_conf.silver_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-silver-{runner_conf.run_id}",
            "silver",
            "A1",
            runner_conf.silver_schema,
            runner_conf,
        )

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META Techsummit Demo by calling the necessary methods in the correct order.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        """
        Launches the workflow for the Techsummit Demo by creating the necessary tasks and submitting the job.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        created_job = self.create_techsummit_demo_workflow(runner_conf)
        self.open_job_url(runner_conf, created_job)

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
                    client=f"dlt_meta_int_test_{__version__}",
                    dependencies=[runner_conf.remote_whl_path],
                ),
            )
        ]
        return self.ws.jobs.create(
            name=f"dlt-meta-techsummit-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=[
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
                            "dlt_meta_schema": runner_conf.dlt_meta_schema,
                            "bronze_schema": runner_conf.bronze_schema,
                            "silver_schema": runner_conf.silver_schema,
                        }
                    )

                ),
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for DLT-META",
                    depends_on=[jobs.TaskDependency(task_key="generate_data")],
                    environment_key="dl_meta_int_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}",
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/conf/onboarding.json",
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
            ]
        )


techsummit_args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
                       "--uc_catalog_name": "provide databricks uc_catalog name, \
                                            this is required to create volume, schema, table",
                       "--table_count": "table_count",
                       "--table_column_count": "table_column_count",
                       "--table_data_rows_count": "table_data_rows_count"
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
