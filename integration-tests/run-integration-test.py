"""Inegration tests script."""
import os
import uuid
import glob
import subprocess
import argparse
from io import BytesIO
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.service import jobs
from databricks.sdk.service import compute
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service.catalog import VolumeType, SchemasAPI, VolumeInfo
import json

cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


def get_workspace_api_client(profile=None) -> WorkspaceClient:
    """Get api client with config."""
    if profile:
        workspace_client = WorkspaceClient(profile=profile)
    else:
        workspace_client = WorkspaceClient(host=input('Databricks Workspace URL: '), token=input('Token: '))
    return workspace_client


def build_and_upload_package(workspace_client, test_info):
    """Build and upload python package."""
    child = subprocess.Popen(
        ["pip3", "wheel", "-w", "dist", ".", "--no-deps"]
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
            workspace_client.files.upload(
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
            file_dbfs_path = f"/{test_info.dbfs_tmp_path}".replace(":", "")
            file_dbfs_path = f"{file_dbfs_path}/integration-tests/whl/{whl_name}"
            workspace_client.dbfs.upload(file_dbfs_path, BytesIO(whl_fp.read()), overwrite=True)            
            test_info.dbfs_whl_path = f"{test_info.dbfs_tmp_path}/integration-tests/whl/{whl_name}"
        return test_info


def create_cloudfiles_workflow_spec(workspace_client, test_info):
    database, dlt_lib = init_db_dltlib(test_info)
    return workspace_client.jobs.create(
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
                        f"{test_info.dbfs_tmp_path}/integration-tests/conf/dlt-meta/onboarding.json",
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


def init_db_dltlib(test_info):
    database = None
    dlt_lib = []
    if test_info.uc_catalog_name:
        database = f"{test_info.uc_catalog_name}.{test_info.dlt_meta_schema}"
        dlt_lib.append(jobs.compute.Library(whl=test_info.uc_target_whl_path))
    else:
        database = test_info.dlt_meta_schema
        dlt_lib.append(jobs.compute.Library(whl=test_info.dbfs_whl_path))
    return database, dlt_lib


def create_eventhub_workflow_spec(workspace_client, args, test_info):
    """Create Job specification."""
    database, dlt_lib = init_db_dltlib(test_info)
    return workspace_client.jobs.create(
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
                        f"{test_info['dbfs_tmp_path']}/integration-tests/conf/dlt-meta/onboarding.json",
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
                        "eventhub_name": args.__getattribute__("eventhub_name"),
                        "eventhub_namespace": args.__getattribute__("eventhub_namespace"),
                        "eventhub_secrets_scope_name": args.__getattribute__("eventhub_secrets_scope_name"),
                        "eventhub_accesskey_name": args.__getattribute__("eventhub_producer_accesskey_name"),
                        "eventhub_input_data":
                        f"/{test_info.dbfs_file_path}/integration-tests/resources/data/iot/iot.json"
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


def create_kafka_workflow_spec(workspace_client, args, test_info):
    """Create Job specification."""
    database, dlt_lib = init_db_dltlib(test_info)
    return workspace_client.jobs.create(
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
                        f"{test_info.dbfs_tmp_path}/integration-tests/conf/dlt-meta/onboarding.json",
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
                        "kafka_topic": args.__getattribute__("kafka_topic_name"),
                        "kafka_broker": args.__getattribute__("kafka_broker"),
                        "kafka_input_data": f"/{test_info.dbfs_file_path}"
                                            "/integration-tests/resources/data/iot/iot.json"
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


def create_dlt_meta_pipeline(
        workspace_client,
        pipeline_name: str,
        layer: str,
        target_schema: str,
        test_info
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
        created = workspace_client.pipelines.create(
            catalog=test_info.uc_catalog_name,
            name=pipeline_name,
            serverless=True,
            channel="PREVIEW",
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{test_info.runners_nb_path}/runners/init_dlt_meta_pipeline"
                    )
                )
            ],
            target=target_schema
        )
    else:
        configuration["dlt_meta_whl"] = test_info.dbfs_whl_path
        configuration[f"{layer}.dataflowspecTable"] = (
            f"{test_info.dlt_meta_schema}.{layer}_dataflowspec_cdc"
        )
        created = workspace_client.pipelines.create(
            name=pipeline_name,
            serverless=True,
            channel="PREVIEW",
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{test_info.runners_nb_path}/runners/init_dlt_meta_pipeline"
                    )
                )
            ],
            target=target_schema
        )
    if created is None:
        raise Exception("Pipeline creation failed")
    return created.pipeline_id


def generate_onboarding_file(args, test_info):
    """Generate onboarding file from template."""
    source = test_info.source.lower()
    if source == "cloudfiles":
        create_cloudfiles_onboarding(test_info)
    elif source == "eventhub":
        create_eventhub_onboarding(args, test_info)
    elif source == "kafka":
        create_kafka_onboarding(args, test_info)


def create_kafka_onboarding(args, test_info):
    """Create eventhub onboarding file."""
    with open(f"{test_info.kafka_template}") as f:
        onboard_obj = json.load(f)
    kafka_topic = args.__getattribute__("kafka_topic_name").lower()
    kafka_bootstrap_servers = args.__getattribute__("kafka_broker").lower()
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
    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def create_eventhub_onboarding(args, test_info):
    """Create eventhub onboarding file."""
    with open(f"{test_info.eventhub_template}") as f:
        onboard_obj = json.load(f)
    eventhub_name = args.__getattribute__("eventhub_name").lower()
    eventhub_accesskey_name = args.__getattribute__("eventhub_consumer_accesskey_name").lower()
    eventhub_secrets_scope_name = args.__getattribute__("eventhub_secrets_scope_name").lower()
    eventhub_namespace = args.__getattribute__("eventhub_namespace").lower()
    eventhub_port = args.__getattribute__("eventhub_port").lower()
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

    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def create_cloudfiles_onboarding(test_info):
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

    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


@dataclass
class TestInfo:
    """TestInfo class."""
    run_id: str
    username: str = None
    uc_catalog_name: str = None
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


def main():
    """Entry method to run integration tests."""
    args = process_arguments()
    workspace_client = get_workspace_api_client(args.profile)
    test_info = init_test_info(args)
    try:
        init_test_setup(args, workspace_client, test_info)
        # workspace_client.volumes.create
        test_info.bronze_pipeline_id = create_dlt_meta_pipeline(
            workspace_client,
            f"dlt-meta-integration-test-bronze-{test_info.run_id}",
            "bronze",
            test_info.bronze_schema,
            test_info
        )

        if test_info.source.lower() == "cloudfiles":
            test_info.silver_pipeline_id = create_dlt_meta_pipeline(
                workspace_client,
                f"dlt-meta-integration-test-silver-{test_info.run_id}",
                "silver",
                test_info.silver_schema,
                test_info
            )
        create_cluster(workspace_client, test_info)
        if test_info.source.lower() == "cloudfiles":
            created_job = create_cloudfiles_workflow_spec(workspace_client, test_info)
        elif test_info.source.lower() == "eventhub":
            created_job = create_eventhub_workflow_spec(workspace_client, args, test_info)
        elif test_info.source.lower() == "kafka":
            created_job = create_kafka_workflow_spec(workspace_client, args, test_info)
        test_info.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        print(f"Waiting for job to complete. run_id={created_job.job_id}")
        run_by_id = workspace_client.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
        download_response = workspace_client.files.download(f"{test_info.uc_volume_path}/integration-test-output.csv")
        output = print(download_response.contents.read().decode("utf-8"))
        with open(f"integration-test-output_{test_info.run_id}.csv", 'w') as f:
            f.write(output)
    except Exception as e:
        print(e)
    finally:
        clean_up(workspace_client, test_info)


def init_test_info(args) -> TestInfo:
    run_id = uuid.uuid4().hex
    test_info = TestInfo(run_id=run_id,
                         username=args.__dict__['username'],
                         dbfs_tmp_path=f"{args.__dict__['dbfs_path']}/{run_id}",
                         int_tests_dir="file:./integration-tests",
                         dlt_meta_schema=f"dlt_meta_dataflowspecs_it_{run_id}",
                         bronze_schema=f"dlt_meta_bronze_it_{run_id}",
                         silver_schema=f"dlt_meta_silver_it_{run_id}",
                         runners_nb_path=f"/Users/{args.__dict__['username']}/dlt-meta_int_tests/{run_id}",
                         source=args.__dict__['source'],
                         node_type_id=cloud_node_type_id_dict[args.__dict__['cloud_provider_name']],
                         dbr_version=args.__dict__['dbr_version'],
                         cloudfiles_template="integration-tests/conf/dlt-meta/cloudfiles-onboarding.template",
                         eventhub_template="integration-tests/conf/dlt-meta/eventhub-onboarding.template",
                         kafka_template="integration-tests/conf/dlt-meta/kafka-onboarding.template",
                         env="it"
                         )
    if args.__dict__['uc_catalog_name']:
        test_info.uc_catalog_name = args.__dict__['uc_catalog_name']
        test_info.uc_volume_name = f"{args.__dict__['uc_catalog_name']}_volume_{run_id}"

    runners_full_local_path = None
    if test_info.source.lower() == "cloudfiles":
        runners_full_local_path = './integration-tests/cloud_files_runners.dbc'
    elif test_info.source.lower() == "eventhub":
        runners_full_local_path = './integration-tests/eventhub_runners.dbc'
    elif test_info.source.lower() == "kafka":
        runners_full_local_path = './integration-tests/kafka_runners.dbc'
    else:
        raise Exception("Supported source not found in argument")
    test_info.runners_full_local_path = runners_full_local_path
    return test_info


def create_cluster(workspace_client, test_info):
    print("Clustrer creation started...")
    if test_info.uc_catalog_name:
        mode = compute.DataSecurityMode.USER_ISOLATION
        spark_confs = {}
    else:
        mode = compute.DataSecurityMode.LEGACY_SINGLE_USER
        spark_confs = {"spark.master": "local[*, 4]", "spark.databricks.cluster.profile": "singleNode"}
    clstr = workspace_client.clusters.create(
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
    print(f"Clustrer creation finished. cluster_id={clstr.cluster_id}")
    test_info.cluster_id = clstr.cluster_id
    return clstr


def init_test_setup(args, workspace_client, test_info):
    generate_onboarding_file(args, test_info)
    print(f"uploading to {test_info.dbfs_tmp_path}/integration-tests/")
    workspace_client.dbfs.create(path=test_info.dbfs_tmp_path + "/integration-tests/", overwrite=True)
    workspace_client.dbfs.copy(test_info.int_tests_dir, test_info.dbfs_tmp_path + "/integration-tests/", overwrite=True,
                               recursive=True)
    print(f"uploading to {test_info.dbfs_tmp_path}/integration-tests/ complete!!!")
    fp = open(test_info.runners_full_local_path, "rb")
    workspace_client.workspace.mkdirs(test_info.runners_nb_path)
    workspace_client.workspace.upload(path=f"{test_info.runners_nb_path}/runners",
                                           format=ImportFormat.DBC, content=fp.read()
                                      )
    if test_info.uc_catalog_name:
        SchemasAPI(workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                       name=test_info.dlt_meta_schema,
                                                       comment="dlt_meta framework schema")
        volume_info = workspace_client.volumes.create(catalog_name=test_info.uc_catalog_name,
                                                      schema_name=test_info.dlt_meta_schema,
                                                      name=test_info.uc_volume_name,
                                                      volume_type=VolumeType.MANAGED)
        test_info.volume_info = volume_info
        SchemasAPI(workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                       name=test_info.bronze_schema,
                                                       comment="bronze_schema")
        SchemasAPI(workspace_client.api_client).create(catalog_name=test_info.uc_catalog_name,
                                                       name=test_info.silver_schema,
                                                       comment="silver_schema")
    build_and_upload_package(workspace_client, test_info)
    return test_info


def process_arguments():
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile",
                        help="provide databricks cli profile name, if not provide databricks_host and token")
    parser.add_argument("--username",
                        help="provide databricks username, this is required to upload runners notebook")
    parser.add_argument("--uc_catalog_name",
                        help="provide databricks uc_catalog name, this is required to create volume, schema, table")
    parser.add_argument("--cloud_provider_name",
                        help="provide cloud provider name. Supported values are aws , azure , gcp")
    parser.add_argument("--dbr_version", help="Provide databricks runtime spark version e.g 11.3.x-scala2.12")
    parser.add_argument("--dbfs_path",
                        help="Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/")
    parser.add_argument("--source", help="Provide source type e.g --source=cloudfiles")
    parser.add_argument("--eventhub_name", help="Provide eventhub_name e.g --eventhub_name=iot")
    parser.add_argument("--eventhub_producer_accesskey_name",
                        help="""Provide access key that has write permission on the eventhub
                        e.g --eventhub_producer_accesskey_name=iotProducerAccessKey""")
    parser.add_argument("--eventhub_consumer_accesskey_name",
                        help="""Provide access key that has read permission on the eventhub
                        e.g --eventhub_consumer_accesskey_name=iotConsumerAccessKey""")
    parser.add_argument("--eventhub_secrets_scope_name",
                        help="Provide eventhub_secrets_scope_name e.g --eventhub_secrets_scope_name=eventhubs_creds")
    parser.add_argument("--eventhub_namespace",
                        help="Provide eventhub_namespace e.g --eventhub_namespace=topic-standard")
    parser.add_argument("--eventhub_port", help="Provide eventhub_port e.g --eventhub_port=9093")
    parser.add_argument("--kafka_topic_name", help="Provide kafka topic name e.g --kafka_topic_name=iot")
    parser.add_argument("--kafka_broker", help="Provide kafka broker e.g --127.0.0.1:9092")

    args = parser.parse_args()
    mandatory_args = ["username", "uc_catalog_name", "cloud_provider_name", "dbr_version", "source", "dbfs_path"]
    check_mandatory_arg(args, mandatory_args)

    supported_cloud_providers = ["aws", "azure", "gcp"]
    supported_sources = ["cloudfiles", "eventhub", "kafka"]

    cloud_provider_name = args.__getattribute__("cloud_provider_name")
    if cloud_provider_name.lower() not in supported_cloud_providers:
        raise Exception("Invalid value for --cloud_provider_name! Supported values are aws, azure, gcp")

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
    return args


def clean_up(workspace_client, test_info):
    print("Cleaning up...")
    if test_info.job_id:
        workspace_client.jobs.delete(test_info.job_id)
    if test_info.bronze_pipeline_id:
        workspace_client.pipelines.delete(test_info.bronze_pipeline_id)
    if test_info.silver_pipeline_id:
        workspace_client.pipelines.delete(test_info.silver_pipeline_id)
    if test_info.cluster_id:
        workspace_client.clusters.delete(test_info.cluster_id)
    if test_info.dbfs_tmp_path:
        workspace_client.dbfs.delete(test_info.dbfs_tmp_path, recursive=True)
    if test_info.uc_catalog_name:
        test_schema_list = [test_info.dlt_meta_schema, test_info.bronze_schema, test_info.silver_schema]
        schema_list = workspace_client.schemas.list(catalog_name=test_info.uc_catalog_name)
        for schema in schema_list:
            if schema.name in test_schema_list:
                print(f"Deleting schema: {schema.name}")
                vol_list = workspace_client.volumes.list(catalog_name=test_info.uc_catalog_name, schema_name=schema.name)
                for vol in vol_list:
                    print(f"Deleting volume:{vol.full_name}")
                    workspace_client.volumes.delete(vol.full_name)
                tables_list = workspace_client.tables.list(catalog_name=test_info.uc_catalog_name, schema_name=schema.name)
                for table in tables_list:
                    print(f"Deleting table:{table.full_name}")
                    workspace_client.tables.delete(table.full_name)
                workspace_client.schemas.delete(schema.full_name)
    print("Cleaning up complete!!!")


def check_mandatory_arg(args, mandatory_args):
    """Check mandatory argument present."""
    for mand_arg in mandatory_args:
        if args.__dict__[f'{mand_arg}'] is None:
            raise Exception(f"Please provide '--{mand_arg}'")


if __name__ == "__main__":
    main()
