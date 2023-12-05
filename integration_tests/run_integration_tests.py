""" A script to run integration tests for DLT-Meta."""
import os
import uuid
import glob
import subprocess
import argparse
from io import BytesIO
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.catalog import VolumeType, SchemasAPI, VolumeInfo

import json

cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


@dataclass
class TestInfo:
    """
    A class to hold information required for running integration tests.

    Attributes:
    -----------
    run_id : str
        The ID of the test run.
    username : str, optional
        The username to use for the test run.
    uc_catalog_name : str, optional
        The name of the unified catalog to use for the test run.
    onboarding_file_path : str, optional
        The path to the onboarding file to use for the test run.
    dbfs_tmp_path : str, optional
        The temporary DBFS path to use for the test run.
    uc_volume_name : str, optional
        The name of the unified volume to use for the test run.
    int_tests_dir : str, optional
        The directory containing the integration tests.
    dlt_meta_schema : str, optional
        The name of the DLT meta schema to use for the test run.
    bronze_schema : str, optional
        The name of the bronze schema to use for the test run.
    silver_schema : str, optional
        The name of the silver schema to use for the test run.
    runners_nb_path : str, optional
        The path to the runners notebook.
    runners_full_local_path : str, optional
        The full local path to the runners notebook.
    source : str, optional
        The source to use for the test run.
    cloudfiles_template : str, optional
        The cloudfiles template to use for the test run.
    eventhub_template : str, optional
        The eventhub template to use for the test run.
    kafka_template : str, optional
        The Kafka template to use for the test run.
    env : str, optional
        The environment to use for the test run.
    whl_path : str, optional
        The path to the whl file to use for the test run.
    volume_info : VolumeInfo, optional
        The volume information to use for the test run.
    uc_volume_path : str, optional
        The path to the unified volume to use for the test run.
    uc_target_whl_path : str, optional
        The path to the unified catalog target whl file to use for the test run.
    dbfs_whl_path : str, optional
        The path to the DBFS whl file to use for the test run.
    node_type_id : str, optional
        The node type ID to use for the test run.
    dbr_version : str, optional
        The Databricks runtime version to use for the test run.
    bronze_pipeline_id : str, optional
        The ID of the bronze pipeline to use for the test run.
    silver_pipeline_id : str, optional
        The ID of the silver pipeline to use for the test run.
    cluster_id : str, optional
        The ID of the cluster to use for the test run.
    job_id : str, optional
        The ID of the job to use for the test run.
    """
    run_id: str
    username: str = None
    run_name: str = None
    uc_catalog_name: str = None
    onboarding_file_path: str = None
    dbfs_tmp_path: str = None
    uc_volume_name: str = None
    int_tests_dir: str = None
    dlt_meta_schema: str = None
    bronze_schema: str = None
    silver_schema: str = None
    runners_nb_path: str = None
    runners_full_local_path: str = None
    source: str = None
    cloudfiles_template: str = None
    eventhub_template: str = None
    kafka_template: str = None
    env: str = None
    whl_path: str = None
    volume_info: VolumeInfo = None
    uc_volume_path: str = None
    uc_target_whl_path: str = None
    dbfs_whl_path: str = None
    node_type_id: str = None
    dbr_version: str = None
    bronze_pipeline_id: str = None
    silver_pipeline_id: str = None
    cluster_id: str = None
    job_id: str = None


class DLTMETAIntegrationTestRunner:
    """
    A class to run integration tests for DLT-Meta.

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
                             # run_name=self.args.__dict__['run_name'],
                             dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
                             int_tests_dir="file:./",
                             dlt_meta_schema=f"dlt_meta_dataflowspecs_it_{run_id}",
                             bronze_schema=f"dlt_meta_bronze_it_{run_id}",
                             silver_schema=f"dlt_meta_silver_it_{run_id}",
                             runners_nb_path=f"/Users/{self.args.__dict__['username']}/dlt_meta_int_tests/{run_id}",
                             source=self.args.__dict__['source'],
                             node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
                             dbr_version=self.args.__dict__['dbr_version'],
                             cloudfiles_template="conf/cloudfiles-onboarding.template",
                             eventhub_template="conf/eventhub-onboarding.template",
                             kafka_template="conf/kafka-onboarding.template",
                             env="it"
                             )
        if self.args.__dict__['uc_catalog_name']:
            test_info.uc_catalog_name = self.args.__dict__['uc_catalog_name']
            test_info.uc_volume_name = f"{self.args.__dict__['uc_catalog_name']}_volume_{run_id}"

        runners_full_local_path = None
        if test_info.source.lower() == "cloudfiles":
            runners_full_local_path = './cloud_files_runners.dbc'
        elif test_info.source.lower() == "eventhub":
            runners_full_local_path = './eventhub_runners.dbc'
        elif test_info.source.lower() == "kafka":
            runners_full_local_path = './kafka_runners.dbc'
        else:
            raise Exception("Supported source not found in argument")
        test_info.runners_full_local_path = runners_full_local_path
        return test_info

    def build_and_upload_package(self, test_info: TestInfo):
        """Build and upload python package."""
        child = subprocess.Popen(
            ["pip3", "wheel", "-w", "dist", "../", "--no-deps"]
        )
        exit_code = child.wait()
        if exit_code != 0:
            raise Exception("Non-zero exitcode: %s" % (exit_code))
        else:
            whl_path = glob.glob("dist/*.whl")[0]
            whl_fp = open(whl_path, "rb")
            whl_name = os.path.basename(whl_path)
            if test_info.uc_catalog_name:
                uc_target_whl_path = None
                uc_target_whl_path = (
                    f"/Volumes/{test_info.volume_info.catalog_name}/"
                    f"{test_info.volume_info.schema_name}/{test_info.volume_info.name}/{whl_name}"
                )
                self.workspace_client.files.upload(
                    file_path=uc_target_whl_path,
                    contents=BytesIO(whl_fp.read()),
                    overwrite=True
                )
                test_info.uc_volume_path = (
                    f"/Volumes/{test_info.volume_info.catalog_name}/"
                    f"{test_info.volume_info.schema_name}/{test_info.volume_info.name}/"
                )
                test_info.uc_target_whl_path = uc_target_whl_path
            else:
                dbfs_whl_path = f"{test_info.dbfs_tmp_path}/{self.base_dir}/whl/{whl_name}"
                self.workspace_client.dbfs.upload(dbfs_whl_path, BytesIO(whl_fp.read()), overwrite=True)
                test_info.dbfs_whl_path = dbfs_whl_path

    def create_dlt_meta_pipeline(self,
                                 pipeline_name: str,
                                 layer: str,
                                 target_schema: str,
                                 test_info: TestInfo
                                 ):
        configuration = {
            "layer": layer,
            f"{layer}.group": "A1",
        }
        """Create DLT pipeline."""
        created = None
        if test_info.uc_catalog_name:
            configuration["dlt_meta_whl"] = test_info.uc_target_whl_path
            configuration[f"{layer}.dataflowspecTable"] = (
                f"{test_info.uc_catalog_name}.{test_info.dlt_meta_schema}.{layer}_dataflowspec_cdc"
            )
            created = self.workspace_client.pipelines.create(
                catalog=test_info.uc_catalog_name,
                name=pipeline_name,
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=f"{test_info.runners_nb_path}/runners/init_dlt_meta_pipeline"
                        )
                    )
                ],
                target=target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=4)]
            )
        else:
            file_dbfs_path = f"/{test_info.dbfs_whl_path}".replace(":", "")
            configuration["dlt_meta_whl"] = file_dbfs_path
            configuration[f"{layer}.dataflowspecTable"] = (
                f"{test_info.dlt_meta_schema}.{layer}_dataflowspec_cdc"
            )

            created = self.workspace_client.pipelines.create(
                name=pipeline_name,
                # serverless=True,
                channel="PREVIEW",
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=f"{test_info.runners_nb_path}/runners/init_dlt_meta_pipeline"
                        )
                    )
                ],
                target=target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=4)]

            )
        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def create_cloudfiles_workflow_spec(self, test_info: TestInfo):
        database, dlt_lib = self.init_db_dltlib(test_info)
        return self.workspace_client.jobs.create(
            name=f"dlt-meta-integration-test-{test_info.run_id}",
            tasks=[
                jobs.Task(
                    task_key="setup_dlt_meta_pipeline_spec",
                    description="test",
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{test_info.dbfs_tmp_path}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": test_info.env,
                            "uc_enabled": "True" if test_info.uc_catalog_name else "False"
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.silver_pipeline_id
                    )
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="silver_dlt_pipeline")],
                    existing_cluster_id=test_info.cluster_id,
                    notebook_task=jobs.NotebookTask(notebook_path=f"{test_info.runners_nb_path}/runners/validate",
                                                    base_parameters={
                                                        "uc_catalog_name": f"{test_info.uc_catalog_name}",
                                                        "bronze_schema": f"{test_info.bronze_schema}",
                                                        "silver_schema": f"{test_info.silver_schema}",
                                                        "dbfs_tmp_path": test_info.dbfs_tmp_path,
                                                        "uc_volume_path": test_info.uc_volume_path,
                                                        "run_id": test_info.run_id
                                                    })
                ),
            ]
        )

    def init_db_dltlib(self, test_info: TestInfo):
        database = None
        dlt_lib = []
        if test_info.uc_catalog_name:
            database = f"{test_info.uc_catalog_name}.{test_info.dlt_meta_schema}"
            dlt_lib.append(jobs.compute.Library(whl=test_info.uc_target_whl_path))
        else:
            database = test_info.dlt_meta_schema
            dlt_lib.append(jobs.compute.Library(whl=test_info.dbfs_whl_path))
        return database, dlt_lib

    def create_eventhub_workflow_spec(self, test_info: TestInfo):
        """Create Job specification."""
        database, dlt_lib = self.init_db_dltlib()
        return self.workspace_client.jobs.create(
            name=f"dlt-meta-integration-test-{test_info['run_id']}",
            tasks=[
                jobs.Task(
                    task_key="setup_dlt_meta_pipeline_spec",
                    description="test",
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{test_info['dbfs_tmp_path']}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": test_info['env']
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="publish_events",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    existing_cluster_id=test_info['cluster_id'],
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info.unners_nb_path}/runners/publish_events",
                        base_parameters={
                            "eventhub_name": self.args.__getattribute__("eventhub_name"),
                            "eventhub_namespace": self.args.__getattribute__("eventhub_namespace"),
                            "eventhub_secrets_scope_name": self.args.__getattribute__("eventhub_secrets_scope_name"),
                            "eventhub_accesskey_name": self.args.__getattribute__("eventhub_producer_accesskey_name"),
                            "eventhub_input_data":
                            f"/{test_info.dbfs_file_path}/{self.base_dir}/resources/data/iot/iot.json"
                        }
                    )
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="publish_events")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    existing_cluster_id=test_info.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info['runners_nb_path']}/runners/validate",
                        base_parameters={
                            "run_id": test_info.run_id,
                            "uc_catalog_name": test_info.uc_catalog_name,
                            "uc_volume_path": test_info.uc_volume_path,
                            "bronze_schema": test_info.bronze_schema
                        }
                    )
                ),
            ]
        )

    def create_kafka_workflow_spec(self, test_info: TestInfo):
        """Create Job specification."""
        database, dlt_lib = self.init_db_dltlib()
        return self.workspace_client.jobs.create(
            name=f"dlt-meta-integration-test-{test_info.run_id}",
            tasks=[
                jobs.Task(
                    task_key="setup_dlt_meta_pipeline_spec",
                    description="test",
                    existing_cluster_id=test_info.cluster_id,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                            "onboard_layer": "bronze_silver",
                            "database": database,
                            "onboarding_file_path":
                            f"{test_info.dbfs_tmp_path}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{test_info.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": test_info.env
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="publish_events",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    existing_cluster_id=test_info.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info.runners_nb_path}/runners/publish_events",
                        base_parameters={
                            "kafka_topic": self.args.__getattribute__("kafka_topic_name"),
                            "kafka_broker": self.args.__getattribute__("kafka_broker"),
                            "kafka_input_data": f"/{test_info.dbfs_file_path}"
                                                "/{self.base_dir}/resources/data/iot/iot.json"
                        }
                    )
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="publish_events")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=test_info.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    existing_cluster_id=test_info.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{test_info.runners_nb_path}/runners/validate",
                        base_parameters={
                            "run_id": test_info.run_id,
                            "uc_catalog_name": test_info.uc_catalog_name,
                            "uc_volume_path": test_info.uc_volume_path,
                            "bronze_schema": test_info.bronze_schema
                        }
                    )
                ),
            ]
        )

    def generate_onboarding_file(self, test_info: TestInfo):
        """Generate onboarding file from template."""
        source = test_info.source.lower()
        if source == "cloudfiles":
            self.create_cloudfiles_onboarding(test_info)
        elif source == "eventhub":
            self.create_eventhub_onboarding(test_info)
        elif source == "kafka":
            self.create_kafka_onboarding(test_info)

    def create_kafka_onboarding(self, test_info: TestInfo):
        """Create kafka onboarding file."""
        with open(f"{test_info.kafka_template}") as f:
            onboard_obj = json.load(f)
        kafka_topic = self.args.__getattribute__("kafka_topic_name").lower()
        kafka_bootstrap_servers = self.args.__getattribute__("kafka_broker").lower()
        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=test_info.dbfs_tmp_path)
                        if 'kafka_topic' in source_value:
                            data_flow[key][source_key] = source_value.format(kafka_topic=kafka_topic)
                        if 'kafka_bootstrap_servers' in source_value:
                            data_flow[key][source_key] = source_value.format(
                                kafka_bootstrap_servers=kafka_bootstrap_servers)
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=test_info.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=test_info.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if test_info.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=test_info.uc_catalog_name,
                            bronze_schema=test_info.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{test_info.run_id}",
                            bronze_schema=""
                        ).replace(".", "")
        with open(f"./{self.base_dir}/conf/onboarding.json", "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def create_eventhub_onboarding(self, test_info: TestInfo):
        """Create eventhub onboarding file."""
        with open(f"{test_info.eventhub_template}") as f:
            onboard_obj = json.load(f)
        eventhub_name = self.args.__getattribute__("eventhub_name").lower()
        eventhub_accesskey_name = self.args.__getattribute__("eventhub_consumer_accesskey_name").lower()
        eventhub_secrets_scope_name = self.args.__getattribute__("eventhub_secrets_scope_name").lower()
        eventhub_namespace = self.args.__getattribute__("eventhub_namespace").lower()
        eventhub_port = self.args.__getattribute__("eventhub_port").lower()
        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=test_info.dbfs_tmp_path)
                        if 'eventhub_name' in source_value:
                            data_flow[key][source_key] = source_value.format(eventhub_name=eventhub_name)
                        if 'eventhub_accesskey_name' in source_value:
                            data_flow[key][source_key] = source_value.format(
                                eventhub_accesskey_name=eventhub_accesskey_name)
                        if 'eventhub_secrets_scope_name' in source_value:
                            data_flow[key][source_key] = source_value.format(
                                eventhub_secrets_scope_name=eventhub_secrets_scope_name)
                        if 'eventhub_nmspace' in source_value:
                            data_flow[key][source_key] = source_value.format(eventhub_nmspace=eventhub_namespace)
                        if 'eventhub_port' in source_value:
                            data_flow[key][source_key] = source_value.format(eventhub_port=eventhub_port)
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=test_info.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=test_info.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if test_info.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=test_info.uc_catalog_name,
                            bronze_schema=test_info.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{test_info.run_id}",
                            bronze_schema=""
                        ).replace(".", "")

        with open("conf/onboarding.json", "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def create_cloudfiles_onboarding(self, test_info: TestInfo):
        """Create onboarding file for cloudfiles as source."""
        with open(f"{test_info.cloudfiles_template}") as f:
            onboard_obj = json.load(f)

        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=test_info.dbfs_tmp_path)
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=test_info.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=test_info.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if test_info.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=test_info.uc_catalog_name,
                            bronze_schema=test_info.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{test_info.run_id}",
                            bronze_schema=""
                        ).replace(".", "")

                elif 'uc_catalog_name' in value and 'silver_schema' in value:
                    if test_info.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=test_info.uc_catalog_name,
                            silver_schema=test_info.silver_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"silver_{test_info.run_id}",
                            silver_schema=""
                        ).replace(".", "")

        with open("conf/onboarding.json", "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def init_test_setup(self, test_info: TestInfo):
        self.generate_onboarding_file(test_info)
        print("int_tests_dir: ", test_info.int_tests_dir)
        print(f"uploading to {test_info.dbfs_tmp_path}/{self.base_dir}/")
        self.workspace_client.dbfs.create(path=test_info.dbfs_tmp_path + f"/{self.base_dir}/", overwrite=True)
        self.workspace_client.dbfs.copy(test_info.int_tests_dir,
                                        test_info.dbfs_tmp_path + f"/{self.base_dir}/",
                                        overwrite=True, recursive=True)
        print(f"uploading to {test_info.dbfs_tmp_path}/{self.base_dir}/ complete!!!")
        fp = open(test_info.runners_full_local_path, "rb")
        print(f"uploading to {test_info.runners_nb_path} started")
        self.workspace_client.workspace.mkdirs(test_info.runners_nb_path)
        self.workspace_client.workspace.upload(path=f"{test_info.runners_nb_path}/runners",
                                               format=ImportFormat.DBC, content=fp.read())
        print(f"uploading to {test_info.runners_nb_path} complete!!!")
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
        self.build_and_upload_package(test_info)

    def create_cluster(self, test_info: TestInfo):
        print("Cluster creation started...")
        if test_info.uc_catalog_name:
            mode = compute.DataSecurityMode.SINGLE_USER
            spark_confs = {}
        else:
            mode = compute.DataSecurityMode.LEGACY_SINGLE_USER
            spark_confs = {}
        clstr = self.workspace_client.clusters.create(
            cluster_name=f"dlt-meta-integration-test-{test_info.run_id}",
            spark_version=test_info.dbr_version,
            node_type_id=test_info.node_type_id,
            driver_node_type_id=test_info.node_type_id,
            num_workers=2,
            spark_conf=spark_confs,
            autotermination_minutes=30,
            spark_env_vars={
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            data_security_mode=mode
        ).result()
        print(f"Cluster creation finished. cluster_id={clstr.cluster_id}")
        test_info.cluster_id = clstr.cluster_id

    def run(self, test_info: TestInfo):
        try:
            self.init_test_setup(test_info)
            self.create_bronze_silver_dlt(test_info)
            self.create_cluster(test_info)            
            self.launch_workflow(test_info)
            self.download_test_results(test_info)
        except Exception as e:
            print(e)
        finally:
            print("Cleaning up...")
            self.clean_up(test_info)

    def download_test_results(self, test_info: TestInfo):
        download_response = self.workspace_client.files.download(
            f"{test_info.uc_volume_path}/integration_test_output.csv")
        output = print(download_response.contents.read().decode("utf-8"))
        with open(f"integration_test_output_{test_info.run_id}.csv", 'w') as f:
            f.write(output)

    def create_bronze_silver_dlt(self, test_info: TestInfo):
        test_info.bronze_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-integration-test-bronze-{test_info.run_id}",
            "bronze",
            test_info.bronze_schema,
            test_info)

        if test_info.source and test_info.source.lower() == "cloudfiles":
            test_info.silver_pipeline_id = self.create_dlt_meta_pipeline(
                f"dlt-meta-integration-test-silver-{test_info.run_id}",
                "silver",
                test_info.silver_schema,
                test_info)

    def launch_workflow(self, test_info: TestInfo):
        if test_info.source.lower() == "cloudfiles":
            created_job = self.create_cloudfiles_workflow_spec(test_info)
        elif test_info.source.lower() == "eventhub":
            created_job = self.create_eventhub_workflow_spec(test_info)
        elif test_info.source.lower() == "kafka":
            created_job = self.create_kafka_workflow_spec(test_info)
        test_info.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. run_id={created_job.job_id}")
        run_by_id = self.workspace_client.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
        return created_job

    def clean_up(self, test_info: TestInfo):
        print("Cleaning up...")
        if test_info.job_id:
            self.workspace_client.jobs.delete(test_info.job_id)
        if test_info.bronze_pipeline_id:
            self.workspace_client.pipelines.delete(test_info.bronze_pipeline_id)
        if test_info.silver_pipeline_id:
            self.workspace_client.pipelines.delete(test_info.silver_pipeline_id)
        if test_info.cluster_id:
            self.workspace_client.clusters.delete(test_info.cluster_id)
        if test_info.dbfs_tmp_path:
            self.workspace_client.dbfs.delete(test_info.dbfs_tmp_path, recursive=True)
        if test_info.uc_catalog_name:
            test_schema_list = [test_info.dlt_meta_schema, test_info.bronze_schema, test_info.silver_schema]
            schema_list = self.workspace_client.schemas.list(catalog_name=test_info.uc_catalog_name)
            for schema in schema_list:
                if schema.name in test_schema_list:
                    print(f"Deleting schema: {schema.name}")
                    vol_list = self.workspace_client.volumes.list(
                        catalog_name=test_info.uc_catalog_name,
                        schema_name=schema.name
                    )
                    for vol in vol_list:
                        print(f"Deleting volume:{vol.full_name}")
                        self.workspace_client.volumes.delete(vol.full_name)
                    tables_list = self.workspace_client.tables.list(
                        catalog_name=test_info.uc_catalog_name,
                        schema_name=schema.name
                    )
                    for table in tables_list:
                        print(f"Deleting table:{table.full_name}")
                        self.workspace_client.tables.delete(table.full_name)
                    self.workspace_client.schemas.delete(schema.full_name)
        print("Cleaning up complete!!!")


def get_workspace_api_client(profile=None) -> WorkspaceClient:
    """Get api client with config."""
    if profile:
        workspace_client = WorkspaceClient(profile=profile)
    else:
        workspace_client = WorkspaceClient(host=input('Databricks Workspace URL: '), token=input('Token: '))
        workspace_client.files.upload
    return workspace_client


args_map = {"--profile": "provide databricks cli profile name, if not provide databricks_host and token",
            "--username": "provide databricks username, this is required to upload runners notebook",
            "--uc_catalog_name": "provide databricks uc_catalog name, this is required to create volume, schema, table",
            "--cloud_provider_name": "provide cloud provider name. Supported values are aws , azure , gcp",
            "--dbr_version": "Provide databricks runtime spark version e.g 11.3.x-scala2.12",
            "--dbfs_path": "Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/",
            "--source": "Provide source type e.g --source=cloudfiles",
            "--eventhub_name": "Provide eventhub_name e.g --eventhub_name=iot",
            "--eventhub_producer_accesskey_name": "Provide access key that has write permission on the eventhub",
            "--eventhub_consumer_accesskey_name": "Provide access key that has read permission on the eventhub",
            "--eventhub_secrets_scope_name": "Provide eventhub_secrets_scope_name e.g \
                        --eventhub_secrets_scope_name=eventhubs_creds",
            "--eventhub_namespace": "Provide eventhub_namespace e.g --eventhub_namespace=topic-standard",
            "--eventhub_port": "Provide eventhub_port e.g --eventhub_port=9093",
            "--kafka_topic_name": "Provide kafka topic name e.g --kafka_topic_name=iot",
            "--kafka_broker": "Provide kafka broker e.g --127.0.0.1:9092"
            }

mandatory_args = [
    "username", "uc_catalog_name", "cloud_provider_name",
    "dbr_version", "source", "dbfs_path"
]


def main():
    """Entry method to run integration tests."""
    args = process_arguments(args_map, mandatory_args)
    post_arg_processing(args)
    workspace_client = get_workspace_api_client(args.profile)
    integration_test_runner = DLTMETAIntegrationTestRunner(args, workspace_client, "integration_tests")
    test_info = integration_test_runner.init_test_info()
    integration_test_runner.run(test_info)


def process_arguments(args_map, mandatory_args):
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    for key, value in args_map.items():
        parser.add_argument(key, help=value)

    args = parser.parse_args()
    check_mandatory_arg(args, mandatory_args)
    supported_cloud_providers = ["aws", "azure", "gcp"]

    cloud_provider_name = args.__getattribute__("cloud_provider_name")
    if cloud_provider_name.lower() not in supported_cloud_providers:
        raise Exception("Invalid value for --cloud_provider_name! Supported values are aws, azure, gcp")
    return args


def post_arg_processing(args):
    """Post processing of arguments."""
    supported_sources = ["cloudfiles", "eventhub", "kafka"]
    source = args.__getattribute__("source")
    if source.lower() not in supported_sources:
        raise Exception("Invalid value for --source! Supported values: --source=cloudfiles")
    if source.lower() == "eventhub":
        eventhub_madatory_args = ["eventhub_name", "eventhub_producer_accesskey_name",
                                  "eventhub_consumer_accesskey_name", "eventhub_secrets_scope_name",
                                  "eventhub_namespace", "eventhub_port"]
        check_mandatory_arg(args, eventhub_madatory_args)
    if source.lower() == "kafka":
        kafka_madatory_args = ["kafka_topic_name", "kafka_broker"]
        check_mandatory_arg(args, kafka_madatory_args)
    print(f"Parsing argument complete. args={args}")


def check_mandatory_arg(args, mandatory_args):
    """Check mandatory argument present."""
    for mand_arg in mandatory_args:
        if args.__dict__[f'{mand_arg}'] is None:
            raise Exception(f"Please provide '--{mand_arg}'")


if __name__ == "__main__":
    main()
