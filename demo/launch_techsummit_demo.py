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
import webbrowser
from databricks.sdk.service import jobs
from databricks.sdk.service.catalog import VolumeType, SchemasAPI
from databricks.sdk.service.workspace import ImportFormat
from dataclasses import dataclass
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    cloud_node_type_id_dict,
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
        print(f"run_id={run_id}")
        runner_conf = TechsummitRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_full_local_path='./demo/dbc/tech_summit_dlt_meta_runners.dbc',
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/dlt_meta_techsummit_demo/{run_id}",
            node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            dbr_version=self.args.__dict__['dbr_version'],
            env="prod",
            table_count=self.args.__dict__['table_count'] if self.args.__dict__['table_count'] else "100",
            table_column_count=(self.args.__dict__['table_column_count'] if self.args.__dict__['table_column_count']
                                else "5"),
            table_data_rows_count=(self.args.__dict__['table_data_rows_count']
                                   if self.args.__dict__['table_data_rows_count'] else "10"),
            worker_nodes=self.args.__dict__['worker_nodes'] if self.args.__dict__['worker_nodes'] else "4",
            source=self.args.__dict__['source'],
            onboarding_file_path='demo/conf/onboarding.json'
        )
        if self.args.__dict__['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args.__dict__['uc_catalog_name']
            runner_conf.uc_volume_name = f"{self.args.__dict__['uc_catalog_name']}_volume_{run_id}"
        return runner_conf

    def init_dltmeta_runner_conf(self, runner_conf: DLTMetaRunnerConf):
        """
        Initializes the DLT-META runner configuration by uploading the necessary files and creating the required
        schemas and volumes.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        fp = open(runner_conf.runners_full_local_path, "rb")
        self.ws.workspace.mkdirs(runner_conf.runners_nb_path)
        self.ws.workspace.upload(path=f"{runner_conf.runners_nb_path}/runners",
                                 format=ImportFormat.DBC, content=fp.read())
        if runner_conf.uc_catalog_name:
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.dlt_meta_schema,
                                                  comment="dlt_meta framework schema")
            volume_info = self.ws.volumes.create(catalog_name=runner_conf.uc_catalog_name,
                                                 schema_name=runner_conf.dlt_meta_schema,
                                                 name=runner_conf.uc_volume_name,
                                                 volume_type=VolumeType.MANAGED)
            runner_conf.volume_info = volume_info
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.bronze_schema,
                                                  comment="bronze_schema")
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.silver_schema,
                                                  comment="silver_schema")

        self.build_and_upload_package(runner_conf)  # comment this line before merging to master

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META Techsummit Demo by calling the necessary methods in the correct order.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.create_cluster(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
        # finally:
        #     self.clean_up(runner_conf)

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        """
        Launches the workflow for the Techsummit Demo by creating the necessary tasks and submitting the job.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        created_job = self.create_techsummit_demo_workflow(runner_conf)
        print(created_job)
        runner_conf.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. run_id={created_job.job_id}")
        run_by_id = self.ws.jobs.run_now(job_id=created_job.job_id)
        url = f"{self.ws.config.host}/jobs/{runner_conf.job_id}/runs/{run_by_id}?o={self.ws.get_workspace_id()}/"
        webbrowser.open(url)
        print(f"Job launched with url={url}")

    def create_techsummit_demo_workflow(self, runner_conf: TechsummitRunnerConf):
        """
        Creates the workflow for the Techsummit Demo by defining the tasks and their dependencies.

        Parameters:
        - runner_conf: The TechsummitRunnerConf object containing the runner configuration parameters.

        Returns:
        - created_job: The created job object.
        """
        database, dlt_lib = self.init_db_dltlib(runner_conf)
        return self.ws.jobs.create(
            name=f"dlt-meta-techsummit-demo-{runner_conf.run_id}",
            tasks=[
                jobs.Task(
                    task_key="generate_data",
                    description="Generate Test Data and Onboarding Files",
                    existing_cluster_id=runner_conf.cluster_id,
                    timeout_seconds=0,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/data_generator",
                        base_parameters={
                            "base_input_path": runner_conf.dbfs_tmp_path,
                            "table_column_count": runner_conf.table_column_count,
                            "table_count": runner_conf.table_count,
                            "table_data_rows_count": runner_conf.table_data_rows_count,
                            "dlt_meta_schema": runner_conf.dlt_meta_schema,
                        }
                    )

                ),
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for DLT-META",
                    depends_on=[jobs.TaskDependency(task_key="generate_data")],
                    existing_cluster_id=runner_conf.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{runner_conf.dbfs_tmp_path}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "False"  # if runner_conf.uc_catalog_name else "False"
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
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
                       "--source": "provide --source=cloudfiles",
                       "--uc_catalog_name": "provide databricks uc_catalog name, \
                                            this is required to create volume, schema, table",
                       "--cloud_provider_name": "cloud_provider_name",
                       "--dbr_version": "dbr_version",
                       "--dbfs_path": "dbfs_path",
                       "--worker_nodes": "worker_nodes",
                       "--table_count": "table_count",
                       "--table_column_count": "table_column_count",
                       "--table_data_rows_count": "table_data_rows_count"
                       }

techsummit_mandatory_args = ["source", "cloud_provider_name", "dbr_version", "dbfs_path"]


def main():
    """Entry method to run the integration tests."""
    args = process_arguments(techsummit_args_map, techsummit_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_techsummit_demo_runner = DLTMETATechSummitDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_techsummit_demo_runner.init_runner_conf()
    dltmeta_techsummit_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
