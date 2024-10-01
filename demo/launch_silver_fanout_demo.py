
import uuid
from databricks.sdk.service import jobs, compute
from src.install import WorkspaceInstaller
from src.__about__ import __version__
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class DLTMETATSilverFanoutDemo(DLTMETARunner):
    """
    Represents the DLT-META Silver Fanout Demo.

    This class is responsible for running the DLT-META Silver Fanout Demo, which includes setting up metadata tables,
    creating clusters, launching workflows, and more.

    Attributes:
    - args: The command-line arguments passed to the script.
    - ws: The Databricks workspace object.
    - base_dir: The base directory of the project.

    Methods:
    - run: Runs the DLT-META Silver Fanout Demo.
    - init_runner_conf: Initializes the runner configuration for running integration tests.
    - launch_workflow: Launches the workflow for the DLT-META Silver Fanout Demo.
    - create_sfo_workflow_spec: Creates the workflow for the DLT-META Silver Fanout Demo by defining the tasks
                                and their dependencies.
    """

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META Silver Fanout Demo.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)

    def init_runner_conf(self) -> DLTMetaRunnerConf:
        """
        Initialize the runner configuration for running integration tests.

        Returns:
        -------
        DLTMetaRunnerConf
            The initialized runner configuration.
        """
        run_id = uuid.uuid4().hex
        runner_conf = DLTMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="file:./demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_fout_demo/{run_id}",
            runners_full_local_path='./demo/dbc/silver_fout_runners.dbc',
            source="cloudFiles",
            # node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            # dbr_version=self.args.__dict__['dbr_version'],
            cloudfiles_template="demo/conf/onboarding_cars.template",
            onboarding_fanout_templates="demo/conf/onboarding_fanout_cars.template",
            onboarding_file_path="demo/conf/onboarding_cars.json",
            onboarding_fanout_file_path="demo/conf/onboarding_fanout_cars.json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args.__dict__['uc_catalog_name']
        runner_conf.uc_volume_name = f"{runner_conf.uc_catalog_name}_dlt_meta_fout_demo_{run_id}"
        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_sfo_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)

    def create_sfo_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """
        Creates the workflow for the DLT-META Silver Fanout Demo by defining the tasks and their dependencies.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.

        Returns:
        - created_job: The created job object.
        """
        database, dlt_lib = self.init_db_dltlib(runner_conf)
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_sfo_demo_env",
                spec=compute.Environment(client=f"dlt_meta_int_test_{__version__}",
                                         # dependencies=[f"dlt_meta=={__version__}"],
                                         dependencies=["dlt_meta==0.0.8"]
                                         )
            )
        ]
        return self.ws.jobs.create(
            name=f"dlt-silver-fanout-demo-{runner_conf.run_id}",
            environments=dltmeta_environments,
            tasks=[
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for DLT-META",
                    # existing_cluster_id=runner_conf.cluster_id,
                    environment_key="dl_meta_sfo_demo_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_file_path}",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True"
                        },
                    ),
                    # libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="onboard_silverfanout_job",
                    description="Sets up metadata tables for DLT-META",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    # existing_cluster_id=runner_conf.cluster_id,
                    environment_key="dl_meta_sfo_demo_env",
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{runner_conf.uc_volume_path}/{runner_conf.onboarding_fanout_file_path}",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "overwrite": "False",
                            "env": runner_conf.env,
                            "uc_enabled": "True"
                        },
                    ),
                    # libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboard_silverfanout_job")],
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


sfo_args_map = {
    "--profile": "provide databricks cli profile name, if not provide databricks_host and token",
    "--uc_catalog_name": "provide databricks uc_catalog name, this is required to create volume, schema, table"
}

sfo_mandatory_args = ["uc_catalog_name"]


def main():
    args = process_arguments(sfo_args_map, sfo_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_afam_demo_runner = DLTMETATSilverFanoutDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
