import uuid
from databricks.sdk.service import jobs
from databricks.sdk.service.catalog import VolumeType, SchemasAPI
from databricks.sdk.service.workspace import ImportFormat
from dataclasses import dataclass
from run_integration_tests import (
    DLTMETAIntegrationTestRunner,
    TestInfo,
    cloud_node_type_id_dict,
    get_workspace_api_client,
    process_arguments
)


@dataclass
class TechsummitTestInfo(TestInfo):
    table_count: str = None
    table_column_count: str = None
    table_data_rows_count: str = None
    worker_nodes: str = None


class DLTMETATechSummitDemo(DLTMETAIntegrationTestRunner):
    """
    A class to run DLT-META Databricks Techsummit DEMO.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - test_info: test information
    """
    def __init__(self, args, workspace_client, base_dir):
        self.args = args
        self.workspace_client = workspace_client
        self.base_dir = base_dir
    
    def init_test_info(self) -> TechsummitTestInfo:
        run_id = uuid.uuid4().hex
        print(f"run_id={run_id}")
        test_info = TechsummitTestInfo(
            run_id=run_id,
            dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_full_local_path='./dbc/tech_summit_dlt_meta_runners.dbc',
            runners_nb_path=f"/Users/{self.args.__dict__['username']}/dlt_meta_techsummit_demo/{run_id}",
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
        )
        if self.args.__dict__['uc_catalog_name']:
            test_info.uc_catalog_name = self.args.__dict__['uc_catalog_name']
            test_info.uc_volume_name = f"{self.args.__dict__['uc_catalog_name']}_volume_{run_id}"
        return test_info

    def init_test_setup(self, test_info: TestInfo):
        fp = open(test_info.runners_full_local_path, "rb")
        self.workspace_client.workspace.mkdirs(test_info.runners_nb_path)
        self.workspace_client.workspace.upload(path=f"{test_info.runners_nb_path}/runners",
                                               format=ImportFormat.DBC, content=fp.read())
        if test_info.uc_catalog_name:
            SchemasAPI(self.workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                                name=test_info.dlt_meta_schema,
                                                                comment="dlt_meta framework schema")
            volume_info = self.workspace_client.volumes.create(catalog_name=test_info.uc_catalog_name,
                                                               schema_name=test_info.dlt_meta_schema,
                                                               name=test_info.uc_volume_name,
                                                               volume_type=VolumeType.MANAGED)
            test_info.volume_info = volume_info
            SchemasAPI(self.workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                                name=test_info.bronze_schema,
                                                                comment="bronze_schema")
            SchemasAPI(self.workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                                name=test_info.silver_schema,
                                                                comment="silver_schema")

        self.build_and_upload_package(test_info)  # comment this line before merging to master

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
        created_job = self.create_techsummit_demo_workflow(test_info)
        print(created_job)
        test_info.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. run_id={created_job.job_id}")
        run_by_id = self.workspace_client.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
        
    def create_techsummit_demo_workflow(self, test_info: TechsummitTestInfo):
        database, dlt_lib = self.init_db_dltlib(test_info)
        return self.workspace_client.jobs.create(
            name=f"dlt-meta-dais-demo-{test_info.run_id}",
            tasks=[
                jobs.Task(
                    task_key="generate_data",
                    description="Generate Test Data and Onboarding Files",
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info.runners_nb_path}/runners/data_generator",
                        base_parameters={
                            "base_input_path": test_info.dbfs_tmp_path,
                            "table_column_count": test_info.table_column_count,
                            "table_count": test_info.table_count,
                            "table_data_rows_count": test_info.table_data_rows_count,
                            "dlt_meta_schema": test_info.dlt_meta_schema,
                        }
                    )

                ),
                jobs.Task(
                    task_key="onboarding_job",
                    description="Sets up metadata tables for DLT-META",
                    depends_on=[jobs.TaskDependency(task_key="generate_data")],
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{test_info.dbfs_tmp_path}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": test_info.env,
                            "uc_enabled": "False"  # if test_info.uc_catalog_name else "False"
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_dlt",
                    depends_on=[jobs.TaskDependency(task_key="onboarding_job")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_dlt",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.silver_pipeline_id
                    )
                )
            ]
        )


techsummit_args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
                       "--username": "provide databricks username, this is required to upload runners notebook",
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

techsummit_mandatory_args = ["username", "source", "cloud_provider_name", "dbr_version", "dbfs_path"]


def main():
    """Entry method to run integration tests."""
    args = process_arguments(techsummit_args_map, techsummit_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_techsummit_demo_runner = DLTMETATechSummitDemo(args, workspace_client, "demo")
    test_info = dltmeta_techsummit_demo_runner.init_test_info()
    dltmeta_techsummit_demo_runner.run(test_info)


if __name__ == "__main__":
    main()
