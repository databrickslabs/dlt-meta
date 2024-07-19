import uuid
import webbrowser
from databricks.sdk.service import jobs
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    cloud_node_type_id_dict,
    get_workspace_api_client,
    process_arguments
)


class DLTMETADAISDemo(DLTMETARunner):
    """
    A class to run DLT-META DAIS DEMO.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - base_dir: base directory
    """
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def init_runner_conf(self) -> DLTMetaRunnerConf:
        """
        Initialize the runner configuration.

        Returns:
        - runner_conf: DLTMetaRunnerConf object
        """
        run_id = uuid.uuid4().hex
        runner_conf = DLTMetaRunnerConf(
            run_id=run_id,
            username=self._my_username(self.ws),
            dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
            int_tests_dir="file:./demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_dais_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_dais_demo_{run_id}",
            runners_nb_path=f"/Users/{self._my_username(self.ws)}/dlt_meta_dais_demo/{run_id}",
            node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            dbr_version=self.args.__dict__['dbr_version'],
            cloudfiles_template="demo/conf/onboarding.template",
            env="prod",
            source=self.args.__dict__['source'],
            runners_full_local_path='./demo/dbc/dais_dlt_meta_runners.dbc',
            onboarding_file_path='demo/conf/onboarding.json'
        )
        if self.args.__dict__['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args.__dict__['uc_catalog_name']
            runner_conf.uc_volume_name = f"{self.args.__dict__['uc_catalog_name']}_volume_{run_id}"

        return runner_conf

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Run the DLT-META DAIS DEMO.

        Args:
        - runner_conf: DLTMetaRunnerConf object
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
        Launch the workflow for DLT-META DAIS DEMO.

        Args:
        - runner_conf: DLTMetaRunnerConf object
        """
        created_job = self.create_daisdemo_workflow(runner_conf)
        runner_conf.job_id = created_job.job_id
        url = f"{self.ws.config.host}/jobs/{created_job.job_id}?o={self.ws.get_workspace_id()}"
        self.ws.jobs.run_now(job_id=created_job.job_id)
        webbrowser.open(url)
        print(f"Job created successfully. job_id={created_job.job_id}, url={url}")

    def create_daisdemo_workflow(self, runner_conf: DLTMetaRunnerConf):
        """
        Create the workflow for DLT-META DAIS DEMO.

        Args:
        - runner_conf: DLTMetaRunnerConf object

        Returns:
        - created_job: created job object
        """
        database, dlt_lib = self.init_db_dltlib(runner_conf)
        return self.ws.jobs.create(
            name=f"dltmeta_dais_demo-{runner_conf.run_id}",
            tasks=[
                jobs.Task(
                    task_key="setup_dlt_meta_pipeline_spec",
                    description="test",
                    existing_cluster_id=runner_conf.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path": f"{runner_conf.dbfs_tmp_path}/demo/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": (
                                f"{runner_conf.dbfs_tmp_path}/demo/resources/data/dlt_spec/silver"
                            ),
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": (
                                f"{runner_conf.dbfs_tmp_path}/demo/resources/data/dlt_spec/bronze"
                            ),
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False"
                        }
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_initial_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                ),
                jobs.Task(
                    task_key="load_incremental_data",
                    description="Load Incremental Data",
                    depends_on=[jobs.TaskDependency(task_key="silver_initial_run")],
                    existing_cluster_id=runner_conf.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/load_incremental_data",
                        base_parameters={
                            "dbfs_tmp_path": runner_conf.dbfs_tmp_path
                        }
                    )
                ),

                jobs.Task(
                    task_key="bronze_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="load_incremental_data")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_incremental_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                )
            ]
        )


dais_args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
                 "--source": "provide source. Supported values are cloudfiles, eventhub, kafka",
                 "--uc_catalog_name": "provide databricks uc_catalog name, \
                     this is required to create volume, schema, table",
                 "--cloud_provider_name": "provide cloud provider name. Supported values are aws , azure , gcp",
                 "--dbr_version": "Provide databricks runtime spark version e.g 11.3.x-scala2.12",
                 "--dbfs_path": "Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/"}

dais_mandatory_args = ["source", "cloud_provider_name",
                       "dbr_version", "dbfs_path"]


def main():
    """Entry method to run integration tests."""
    args = process_arguments(dais_args_map, dais_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_dais_demo_runner = DLTMETADAISDemo(args, workspace_client, "demo")
    runner_conf = dltmeta_dais_demo_runner.init_runner_conf()
    dltmeta_dais_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
