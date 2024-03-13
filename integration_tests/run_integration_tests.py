""" A script to run integration tests for DLT-Meta."""

# Import necessary modules
import uuid
import argparse
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.catalog import SchemasAPI, VolumeInfo
from src.install import WorkspaceInstaller

import json

# Dictionary mapping cloud providers to node types
cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}

DLT_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install {remote_wheel}
# dbutils.library.restartPython()
# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""


@dataclass
class DLTMetaRunnerConf:
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
    remote_whl_path: str = None
    dbfs_whl_path: str = None
    node_type_id: str = None
    dbr_version: str = None
    bronze_pipeline_id: str = None
    silver_pipeline_id: str = None
    cluster_id: str = None
    job_id: str = None
    test_output_file_path: str = None


class DLTMETARunner:
    """
    A class to run integration tests for DLT-Meta.

    Attributes:
    - args: command line arguments
    - workspace_client: Databricks workspace client
    - runner_conf: test information
    """
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

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
            dbfs_tmp_path=f"{self.args.__dict__['dbfs_path']}/{run_id}",
            int_tests_dir="file:./integration_tests",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_it_{run_id}",
            bronze_schema=f"dlt_meta_bronze_it_{run_id}",
            silver_schema=f"dlt_meta_silver_it_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_int_tests/{run_id}",
            source=self.args.__dict__['source'],
            node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            dbr_version=self.args.__dict__['dbr_version'],
            cloudfiles_template="integration_tests/conf/cloudfiles-onboarding.template",
            eventhub_template="integration_tests/conf/eventhub-onboarding.template",
            kafka_template="integration_tests/conf/kafka-onboarding.template",
            onboarding_file_path="integration_tests/conf/onboarding.json",
            env="it",
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/dlt_meta_int_tests/"
                f"{run_id}/integration-test-output.csv"
            )

        )
        if self.args.__dict__['uc_catalog_name']:
            runner_conf.uc_catalog_name = self.args.__dict__['uc_catalog_name']

        runners_full_local_path = None

        if runner_conf.source.lower() == "cloudfiles":
            runners_full_local_path = './integration_tests/dbc/cloud_files_runners.dbc'
        elif runner_conf.source.lower() == "eventhub":
            runners_full_local_path = './integration_tests/dbc/eventhub_runners.dbc'
        elif runner_conf.source.lower() == "kafka":
            runners_full_local_path = './integration_tests/dbc/kafka_runners.dbc'
        else:
            raise Exception("Supported source not found in argument")
        runner_conf.runners_full_local_path = runners_full_local_path
        return runner_conf

    def _install_folder(self):
        return f"/Users/{self._my_username()}/dlt-meta"

    def _my_username(self, ws):
        if not hasattr(ws, "_me"):
            _me = ws.current_user.me()
        return _me.user_name

    def build_and_upload_package(self, runner_conf: DLTMetaRunnerConf):
        """
        Build and upload the Python package.

        Parameters:
        ----------
        runner_conf : DLTMetaRunnerConf
            The runner configuration.

        Raises:
        ------
        Exception
            If the build process fails.
        """
        runner_conf.remote_whl_path = f"/Workspace{self.wsi._upload_wheel()}"

    def create_dlt_meta_pipeline(self,
                                 pipeline_name: str,
                                 layer: str,
                                 target_schema: str,
                                 runner_conf: DLTMetaRunnerConf
                                 ):
        """
        Create a DLT pipeline.

        Parameters:
        ----------
        pipeline_name : str
            The name of the pipeline.
        layer : str
            The layer of the pipeline.
        target_schema : str
            The target schema of the pipeline.
        runner_conf : DLTMetaRunnerConf
            The runner configuration.

        Returns:
        -------
        str
            The ID of the created pipeline.

        Raises:
        ------
        Exception
            If the pipeline creation fails.
        """
        configuration = {
            "layer": layer,
            f"{layer}.group": "A1",
            "dlt_meta_whl": runner_conf.remote_whl_path,
        }
        created = None
        if runner_conf.uc_catalog_name:
            configuration[f"{layer}.dataflowspecTable"] = (
                f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}.{layer}_dataflowspec_cdc"
            )
            created = self.ws.pipelines.create(
                catalog=runner_conf.uc_catalog_name,
                name=pipeline_name,
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=f"{runner_conf.runners_nb_path}/runners/init_dlt_meta_pipeline"
                        )
                    )
                ],
                target=target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=4)]
            )
        else:
            configuration[f"{layer}.dataflowspecTable"] = (
                f"{runner_conf.dlt_meta_schema}.{layer}_dataflowspec_cdc"
            )
            created = self.ws.pipelines.create(
                name=pipeline_name,
                # serverless=True,
                channel="PREVIEW",
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=f"{runner_conf.runners_nb_path}/runners/init_dlt_meta_pipeline"
                        )
                    )
                ],
                target=target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=4)]

            )
        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def create_cloudfiles_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """
        Create the CloudFiles workflow specification.

        Parameters:
        ----------
        runner_conf : DLTMetaRunnerConf
            The runner configuration.

        Returns:
        -------
        Job
            The created job.

        Raises:
        ------
        Exception
            If the job creation fails.
        """
        database, dlt_lib = self.init_db_dltlib(runner_conf)
        return self.ws.jobs.create(
            name=f"dlt-meta-integration-test-{runner_conf.run_id}",
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
                            "onboarding_file_path":
                            f"{runner_conf.dbfs_tmp_path}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": runner_conf.env,
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False"
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="silver_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.silver_pipeline_id
                    )
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="silver_dlt_pipeline")],
                    existing_cluster_id=runner_conf.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/validate",
                        base_parameters={
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False",
                            "uc_catalog_name": f"{runner_conf.uc_catalog_name}",
                            "bronze_schema": f"{runner_conf.bronze_schema}",
                            "silver_schema": f"{runner_conf.silver_schema}",
                            "output_file_path": f"/Workspace{runner_conf.test_output_file_path}",
                            "run_id": runner_conf.run_id
                        }
                    )

                ),
            ]
        )

    def init_db_dltlib(self, runner_conf: DLTMetaRunnerConf):
        database = None
        dlt_lib = []
        if runner_conf.uc_catalog_name:
            database = f"{runner_conf.uc_catalog_name}.{runner_conf.dlt_meta_schema}"
            dlt_lib.append(jobs.compute.Library(whl=runner_conf.remote_whl_path))
        else:
            database = runner_conf.dlt_meta_schema
            dlt_lib.append(jobs.compute.Library(whl=runner_conf.remote_whl_path.replace("/Workspace", "dbfs:")))
        return database, dlt_lib

    def create_eventhub_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """Create Job specification."""
        database, dlt_lib = self.init_db_dltlib()
        return self.ws.jobs.create(
            name=f"dlt-meta-integration-test-{runner_conf['run_id']}",
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
                            "onboarding_file_path":
                            f"{runner_conf['dbfs_tmp_path']}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/dltmeta/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{runner_conf.dbfs_tmp_path}/dltmeta/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": runner_conf['env']
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="publish_events",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    existing_cluster_id=runner_conf['cluster_id'],
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.unners_nb_path}/runners/publish_events",
                        base_parameters={
                            "eventhub_name": self.args.__getattribute__("eventhub_name"),
                            "eventhub_namespace": self.args.__getattribute__("eventhub_namespace"),
                            "eventhub_secrets_scope_name": self.args.__getattribute__("eventhub_secrets_scope_name"),
                            "eventhub_accesskey_name": self.args.__getattribute__("eventhub_producer_accesskey_name"),
                            "eventhub_input_data":
                            f"/{runner_conf.dbfs_file_path}/{self.base_dir}/resources/data/iot/iot.json"
                        }
                    )
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="publish_events")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    existing_cluster_id=runner_conf.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf['runners_nb_path']}/runners/validate",
                        base_parameters={
                            "run_id": runner_conf.run_id,
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False",
                            "uc_catalog_name": runner_conf.uc_catalog_name,
                            "bronze_schema": runner_conf.bronze_schema,
                            "output_file_path": f"/Workspace{runner_conf.test_output_file_path}"
                        }
                    )
                ),
            ]
        )

    def create_kafka_workflow_spec(self, runner_conf: DLTMetaRunnerConf):
        """Create Job specification."""
        database, dlt_lib = self.init_db_dltlib()
        return self.ws.jobs.create(
            name=f"dlt-meta-integration-test-{runner_conf.run_id}",
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
                            "onboarding_file_path":
                            f"{runner_conf.dbfs_tmp_path}/{self.base_dir}/conf/onboarding.json",
                            "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                            "silver_dataflowspec_path": f"{self._install_folder()}/dltmeta/data/dlt_spec/silver",
                            "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                            "import_author": "Ravi",
                            "version": "v1",
                            "bronze_dataflowspec_path": f"{self._install_folder()}/dltmeta/data/dlt_spec/bronze",
                            "overwrite": "True",
                            "env": runner_conf.env
                        },
                    ),
                    libraries=dlt_lib
                ),
                jobs.Task(
                    task_key="publish_events",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="setup_dlt_meta_pipeline_spec")],
                    existing_cluster_id=runner_conf.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/publish_events",
                        base_parameters={
                            "kafka_topic": self.args.__getattribute__("kafka_topic_name"),
                            "kafka_broker": self.args.__getattribute__("kafka_broker"),
                            "kafka_input_data": f"/{runner_conf.dbfs_file_path}"
                                                "/{self.base_dir}/resources/data/iot/iot.json"
                        }
                    )
                ),
                jobs.Task(
                    task_key="bronze_dlt_pipeline",
                    depends_on=[jobs.TaskDependency(task_key="publish_events")],
                    pipeline_task=jobs.PipelineTask(
                        pipeline_id=runner_conf.bronze_pipeline_id
                    ),
                ),
                jobs.Task(
                    task_key="validate_results",
                    description="test",
                    depends_on=[jobs.TaskDependency(task_key="bronze_dlt_pipeline")],
                    existing_cluster_id=runner_conf.cluster_id,
                    notebook_task=jobs.NotebookTask(
                        notebook_path=f"{runner_conf.runners_nb_path}/runners/validate",
                        base_parameters={
                            "run_id": runner_conf.run_id,
                            "uc_enabled": "True" if runner_conf.uc_catalog_name else "False",
                            "uc_catalog_name": runner_conf.uc_catalog_name,
                            "bronze_schema": runner_conf.bronze_schema,
                            "output_file_path": f"/Workspace{runner_conf.test_output_file_path}"
                        }
                    )
                ),
            ]
        )

    def generate_onboarding_file(self, runner_conf: DLTMetaRunnerConf):
        """Generate onboarding file from template."""
        source = runner_conf.source.lower()
        if source == "cloudfiles":
            self.create_cloudfiles_onboarding(runner_conf)
        elif source == "eventhub":
            self.create_eventhub_onboarding(runner_conf)
        elif source == "kafka":
            self.create_kafka_onboarding(runner_conf)

    def create_kafka_onboarding(self, runner_conf: DLTMetaRunnerConf):
        """Create kafka onboarding file."""
        with open(f"{runner_conf.kafka_template}") as f:
            onboard_obj = json.load(f)
        kafka_topic = self.args.__getattribute__("kafka_topic_name").lower()
        kafka_bootstrap_servers = self.args.__getattribute__("kafka_broker").lower()
        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=runner_conf.dbfs_tmp_path)
                        if 'kafka_topic' in source_value:
                            data_flow[key][source_key] = source_value.format(kafka_topic=kafka_topic)
                        if 'kafka_bootstrap_servers' in source_value:
                            data_flow[key][source_key] = source_value.format(
                                kafka_bootstrap_servers=kafka_bootstrap_servers)
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=runner_conf.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=runner_conf.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if runner_conf.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=runner_conf.uc_catalog_name,
                            bronze_schema=runner_conf.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{runner_conf.run_id}",
                            bronze_schema=""
                        ).replace(".", "")
        with open(runner_conf.onboarding_file_path, "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def create_eventhub_onboarding(self, runner_conf: DLTMetaRunnerConf):
        """Create eventhub onboarding file."""
        with open(f"{runner_conf.eventhub_template}") as f:
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
                            data_flow[key][source_key] = source_value.format(dbfs_path=runner_conf.dbfs_tmp_path)
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
                    data_flow[key] = value.format(dbfs_path=runner_conf.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=runner_conf.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if runner_conf.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=runner_conf.uc_catalog_name,
                            bronze_schema=runner_conf.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{runner_conf.run_id}",
                            bronze_schema=""
                        ).replace(".", "")

        with open(runner_conf.onboarding_file_path, "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def create_cloudfiles_onboarding(self, runner_conf: DLTMetaRunnerConf):
        """Create onboarding file for cloudfiles as source."""
        with open(f"{runner_conf.cloudfiles_template}") as f:
            onboard_obj = json.load(f)

        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=runner_conf.dbfs_tmp_path)
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=runner_conf.dbfs_tmp_path)
                elif 'run_id' in value:
                    data_flow[key] = value.format(run_id=runner_conf.run_id)
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if runner_conf.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=runner_conf.uc_catalog_name,
                            bronze_schema=runner_conf.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"bronze_{runner_conf.run_id}",
                            bronze_schema=""
                        ).replace(".", "")

                elif 'uc_catalog_name' in value and 'silver_schema' in value:
                    if runner_conf.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=runner_conf.uc_catalog_name,
                            silver_schema=runner_conf.silver_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=f"silver_{runner_conf.run_id}",
                            silver_schema=""
                        ).replace(".", "")

        with open(runner_conf.onboarding_file_path, "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)

    def init_dltmeta_runner_conf(self, runner_conf: DLTMetaRunnerConf):
        self.generate_onboarding_file(runner_conf)
        print("int_tests_dir: ", runner_conf.int_tests_dir)
        print(f"uploading to {runner_conf.dbfs_tmp_path}/{self.base_dir}/")
        if runner_conf.uc_catalog_name:
            self.ws.dbfs.create(path=runner_conf.dbfs_tmp_path + f"/{self.base_dir}/", overwrite=True)
        else:
            try:
                self.ws.dbfs.mkdirs(runner_conf.dbfs_tmp_path + f"/{self.base_dir}/")
            except Exception as e:
                print(f"Error in creating directory {runner_conf.dbfs_tmp_path + f'/{self.base_dir}/'}")
                print(e)
                print(runner_conf.dbfs_tmp_path + f"/{self.base_dir}/ must be already present")
        self.ws.dbfs.copy(runner_conf.int_tests_dir,
                          runner_conf.dbfs_tmp_path + f"/{self.base_dir}/",
                          overwrite=True, recursive=True)
        print(f"uploading to {runner_conf.dbfs_tmp_path}/{self.base_dir}/ complete!!!")
        fp = open(runner_conf.runners_full_local_path, "rb")
        print(f"uploading to {runner_conf.runners_nb_path} started")
        self.ws.workspace.mkdirs(runner_conf.runners_nb_path)
        self.ws.workspace.upload(path=f"{runner_conf.runners_nb_path}/runners",
                                 format=ImportFormat.DBC, content=fp.read())
        print(f"uploading to {runner_conf.runners_nb_path} complete!!!")
        if runner_conf.uc_catalog_name:
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.dlt_meta_schema,
                                                  comment="dlt_meta framework schema")
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.bronze_schema,
                                                  comment="bronze_schema")
            SchemasAPI(self.ws.api_client).create(catalog_name=runner_conf.uc_catalog_name,
                                                  name=runner_conf.silver_schema,
                                                  comment="silver_schema")
        self.build_and_upload_package(runner_conf)

    def create_cluster(self, runner_conf: DLTMetaRunnerConf):
        print("Cluster creation started...")
        if runner_conf.uc_catalog_name:
            mode = compute.DataSecurityMode.USER_ISOLATION
            spark_confs = {}
        else:
            mode = compute.DataSecurityMode.NONE
            spark_confs = {}
        clstr = self.ws.clusters.create(
            cluster_name=f"dlt-meta-integration-test-{runner_conf.run_id}",
            spark_version=runner_conf.dbr_version,
            node_type_id=runner_conf.node_type_id,
            driver_node_type_id=runner_conf.node_type_id,
            num_workers=2,
            spark_conf=spark_confs,
            autotermination_minutes=30,
            spark_env_vars={
                "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
            },
            data_security_mode=mode
        ).result()
        print(f"Cluster creation finished. cluster_id={clstr.cluster_id}")
        runner_conf.cluster_id = clstr.cluster_id

    def run(self, runner_conf: DLTMetaRunnerConf):
        try:
            self.init_dltmeta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.create_cluster(runner_conf)
            self.launch_workflow(runner_conf)
            self.download_test_results(runner_conf)
        except Exception as e:
            print(e)
        finally:
            print("Cleaning up...")
    #        self.clean_up(runner_conf)

    def download_test_results(self, runner_conf: DLTMetaRunnerConf):
        ws_output_file = self.ws.workspace.download(runner_conf.test_output_file_path)
        with open(f"integration_test_output_{runner_conf.run_id}.csv", "wb") as output_file:
            output_file.write(ws_output_file.read())

    def create_bronze_silver_dlt(self, runner_conf: DLTMetaRunnerConf):
        runner_conf.bronze_pipeline_id = self.create_dlt_meta_pipeline(
            f"dlt-meta-integration-test-bronze-{runner_conf.run_id}",
            "bronze",
            runner_conf.bronze_schema,
            runner_conf)

        if runner_conf.source and runner_conf.source.lower() == "cloudfiles":
            runner_conf.silver_pipeline_id = self.create_dlt_meta_pipeline(
                f"dlt-meta-integration-test-silver-{runner_conf.run_id}",
                "silver",
                runner_conf.silver_schema,
                runner_conf)

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        if runner_conf.source.lower() == "cloudfiles":
            created_job = self.create_cloudfiles_workflow_spec(runner_conf)
        elif runner_conf.source.lower() == "eventhub":
            created_job = self.create_eventhub_workflow_spec(runner_conf)
        elif runner_conf.source.lower() == "kafka":
            created_job = self.create_kafka_workflow_spec(runner_conf)
        runner_conf.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. job_id={created_job.job_id}")
        run_by_id = self.ws.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
        return created_job

    def clean_up(self, runner_conf: DLTMetaRunnerConf):
        print("Cleaning up...")
        if runner_conf.job_id:
            self.ws.jobs.delete(runner_conf.job_id)
        if runner_conf.bronze_pipeline_id:
            self.ws.pipelines.delete(runner_conf.bronze_pipeline_id)
        if runner_conf.silver_pipeline_id:
            self.ws.pipelines.delete(runner_conf.silver_pipeline_id)
        if runner_conf.cluster_id:
            self.ws.clusters.delete(runner_conf.cluster_id)
        if runner_conf.dbfs_tmp_path:
            self.ws.dbfs.delete(runner_conf.dbfs_tmp_path, recursive=True)
        if runner_conf.uc_catalog_name:
            test_schema_list = [runner_conf.dlt_meta_schema, runner_conf.bronze_schema, runner_conf.silver_schema]
            schema_list = self.ws.schemas.list(catalog_name=runner_conf.uc_catalog_name)
            for schema in schema_list:
                if schema.name in test_schema_list:
                    print(f"Deleting schema: {schema.name}")
                    vol_list = self.ws.volumes.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema.name
                    )
                    for vol in vol_list:
                        print(f"Deleting volume:{vol.full_name}")
                        self.ws.volumes.delete(vol.full_name)
                    tables_list = self.ws.tables.list(
                        catalog_name=runner_conf.uc_catalog_name,
                        schema_name=schema.name
                    )
                    for table in tables_list:
                        print(f"Deleting table:{table.full_name}")
                        self.ws.tables.delete(table.full_name)
                    self.ws.schemas.delete(schema.full_name)
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
    "uc_catalog_name", "cloud_provider_name",
    "dbr_version", "source", "dbfs_path"
]


def main():
    """Entry method to run integration tests."""
    args = process_arguments(args_map, mandatory_args)
    post_arg_processing(args)
    workspace_client = get_workspace_api_client(args.profile)
    integration_test_runner = DLTMETARunner(args, workspace_client, "integration_tests")
    runner_conf = integration_test_runner.init_runner_conf()
    integration_test_runner.run(runner_conf)


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
