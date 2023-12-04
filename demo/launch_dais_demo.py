import uuid
# import sys
# sys.path.insert(0, './integration_tests')
from databricks.sdk.service import jobs
# from databricks.sdk.service.compute import DataSecurityMode, RuntimeEngine
from run_integration_tests import (
    DLTMETAIntegrationTestRunner,
    TestInfo,
    cloud_node_type_id_dict,
    get_workspace_api_client,
    process_arguments
)


class DLTMETADAISDemo(DLTMETAIntegrationTestRunner):
    """
    A class to run DLT-META DAIS DEMO.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - test_info: test information
    """
    def __init__(self, args, workspace_client, base_dir):
        self.args = args
        self.workspace_client = workspace_client
        self.base_dir = base_dir
    
    def init_test_info(self) -> TestInfo:
        run_id = uuid.uuid4().hex
        test_info = TestInfo(run_id=run_id,
                             username=self.args.__dict__['username'],
                             # onboarding_file_path=self.args.__dict__['onboarding_file_path'],
                             dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
                             int_tests_dir="file:./", #"file:./demo",  # "file:./dlt_meta_demo",
                             dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
                             bronze_schema=f"dlt_meta_bronze_dais_demo_{run_id}",
                             silver_schema=f"dlt_meta_silver_dais_demo_{run_id}",
                             runners_nb_path=f"/Users/{self.args.__dict__['username']}/dlt_meta_dais_demo/{run_id}",
                             node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
                             dbr_version=self.args.__dict__['dbr_version'],
                             cloudfiles_template="conf/onboarding.template",
                             env="prod",
                             source=self.args.__dict__['source'],
                             runners_full_local_path=f'./dbc/dais_dlt_meta_runners.dbc'
                             )
        if self.args.__dict__['uc_catalog_name']:
            test_info.uc_catalog_name = self.args.__dict__['uc_catalog_name']
            test_info.uc_volume_name = f"{self.args.__dict__['uc_catalog_name']}_volume_{run_id}"

        return test_info

    def run(self, test_info: TestInfo):
        try:
            self.init_test_setup(test_info)
            self.create_bronze_silver_dlt(test_info)
            self.create_cluster(test_info)
            self.launch_workflow(test_info)
        except Exception as e:
            print(e)
        # finally:
        #     self.clean_up(test_info)
            
    def launch_workflow(self, test_info: TestInfo):
        created_job = self.create_daisdemo_workflow(test_info)
        test_info.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. run_id={created_job.job_id}")
        run_by_id = self.workspace_client.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
            
    def create_daisdemo_workflow(self, test_info: TestInfo):
        database, dlt_lib = self.init_db_dltlib(test_info)
        return self.workspace_client.jobs.create(
            name=f"dltmeta_dais_demo-{test_info.run_id}",
            tasks=[
                jobs.Task(
                    task_key="setup_dlt_meta_pipeline_spec",
                    description="test",
                    # new_cluster=compute.ClusterSpec(
                    #     spark_version=test_info.dbr_version,
                    #     node_type_id=test_info.node_type_id,
                    #     num_workers=2,
                    #     data_security_mode=DataSecurityMode.SINGLE_USER,
                    #     runtime_engine=RuntimeEngine.STANDARD
                    #     # "spark_version": test_info.dbr_version,
                    #     # "num_workers": "2",
                    #     # "node_type_id": test_info.node_type_id,
                    #     # "data_security_mode": "SINGLE_USER",
                    #     # "runtime_engine": "STANDARD"
                    # ),
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path": f"{test_info.dbfs_tmp_path}/demo/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": (
                                f"{test_info.dbfs_tmp_path}/demo/resources/data/dlt_spec/silver"
                            ),
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": (
                                f"{test_info.dbfs_tmp_path}/demo/resources/data/dlt_spec/bronze"
                            ),
                            "overwrite": "True",
                            "env": test_info.env,
                            "uc_enabled": "True" if test_info.uc_catalog_name else "False"
                        }
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_initial_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_initial_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.silver_pipeline_id
                    )
                ),
                jobs.Task(
                    task_key="load_incremental_data",
                    description="Load Incremental Data",
                    depends_on=[jobs.TaskDependency(task_key="silver_initial_run")],
                    existing_cluster_id=test_info.cluster_id,
                    # new_cluster=compute.ClusterSpec(
                    #     spark_version=test_info.dbr_version,
                    #     node_type_id=test_info.node_type_id,
                    #     num_workers=2,
                    #     data_security_mode=DataSecurityMode.SINGLE_USER,
                    #     runtime_engine=RuntimeEngine.STANDARD
                    #     # "spark_version": test_info.dbr_version,
                    #     # "num_workers": "2",
                    #     # "node_type_id": test_info.node_type_id,
                    #     # "data_security_mode": "SINGLE_USER",
                    #     # "runtime_engine": "STANDARD"
                    # ),
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info.runners_nb_path}/runners/load_incremental_data",
                        base_parameters={
                            "dbfs_tmp_path": test_info.dbfs_tmp_path
                        }
                    )
                ),

                jobs.Task(
                    task_key="bronze_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="load_incremental_data")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_incremental_run",
                    depends_on=[jobs.TaskDependency(task_key="bronze_incremental_run")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.silver_pipeline_id
                    )
                )
            ]
        )


dais_args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
                 "--username": "provide databricks username, this is required to upload runners notebook",
                #  "--onboarding_file_path": "provide onboarding file path",
                 "--source": "provide source. Supported values are cloudfiles, eventhub, kafka",
                 "--uc_catalog_name": "provide databricks uc_catalog name, \
                     this is required to create volume, schema, table",
                 "--cloud_provider_name": "provide cloud provider name. Supported values are aws , azure , gcp",
                 "--dbr_version": "Provide databricks runtime spark version e.g 11.3.x-scala2.12",
                 "--dbfs_path": "Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/"}

dais_mandatory_args = ["username", "source", "uc_catalog_name", "cloud_provider_name",
                       "dbr_version", "dbfs_path"]


def main():
    """Entry method to run integration tests."""
    args = process_arguments(dais_args_map, dais_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_dais_demo_runner = DLTMETADAISDemo(args, workspace_client, "demo")
    test_info = dltmeta_dais_demo_runner.init_test_info()
    dltmeta_dais_demo_runner.run(test_info)


if __name__ == "__main__":
    main()
