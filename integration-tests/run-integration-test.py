"""Inegration tests script."""
import os
import time
import uuid
import glob
import subprocess
import argparse
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.configure.config import _get_api_client
from databricks_cli.configure.provider import EnvironmentVariableConfigProvider
from databricks_cli.sdk import JobsService, DbfsService, DeltaPipelinesService, WorkspaceService
import base64
import json


def get_api_client():
    """Get api client with config."""
    config = EnvironmentVariableConfigProvider().get_config()
    api_client = _get_api_client(config, command_name="labs_dlt-meta")
    return api_client


def build_and_upload_package(dbfs_service, tmp_path):
    """Build and upload python package."""
    child = subprocess.Popen(["pip3", "wheel", "-w", "dist", ".", "--no-deps"])
    exit_code = child.wait()
    if exit_code != 0:
        raise Exception("Non-zero exitcode: %s" % (exit_code))
    else:
        whl_path = glob.glob("dist/*.whl")[0]
        whl_name = os.path.basename(whl_path)
        dbfs_path = f"{tmp_path}/{whl_name}"
        dbfs_service.put(dbfs_path, src_path=whl_path, overwrite=True)
        file_dbfs_path = f"/{dbfs_path}".replace(":", "")
        return dbfs_path, file_dbfs_path


cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


def create_cloudfiles_workflow_spec(job_spec_dict):
    """Create Job specification."""
    job_spec = {
        "run_name": f"dlt-meta-integration-test-{job_spec_dict['run_id']}",
        "tasks": [
            {
                "task_key": "setup_dlt_meta_pipeline_spec",
                "description": "Sets up metadata tables for DLT-META",
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "python_wheel_task": {
                    "package_name": "dlt_meta",
                    "entry_point": "run",
                    "named_parameters": {
                        "onboard_layer": "bronze_silver",
                        "database": job_spec_dict['database'],
                        "onboarding_file_path": f"{job_spec_dict['dbfs_tmp_path']}/integration-tests/conf/dlt-meta/onboarding.json",
                        "silver_dataflowspec_table": "silver_dataflowspec_cdc",
                        "silver_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/silver",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": job_spec_dict['env']
                    },
                },
                "libraries": [
                    {"whl": job_spec_dict['whl_path']}
                ]
            },
            {
                "task_key": "bronze",
                "depends_on": [
                    {
                        "task_key": "setup_dlt_meta_pipeline_spec"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['bronze_pipeline_id']
                }
            },
            {
                "task_key": "silver",
                "depends_on": [
                    {
                        "task_key": "bronze"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['silver_pipeline_id']
                }
            },
            {
                "task_key": "Validate",
                "description": "Validate Tables",
                "depends_on": [
                    {
                        "task_key": "silver"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/validate",
                    "base_parameters": {
                        "database": job_spec_dict['database'],
                        "dbfs_tmp_path": job_spec_dict['dbfs_tmp_path'],
                        "run_id": job_spec_dict['run_id']
                    }
                }

            }
        ]
    }
    print(job_spec)
    return job_spec


def create_eventhub_workflow_spec(args, job_spec_dict):
    """Create Job specification."""
    dbfs_file_path = job_spec_dict['dbfs_tmp_path'].replace(":", "")
    job_spec = {
        "run_name": f"dlt-meta-integration-test-{job_spec_dict['run_id']}",
        "tasks": [
            {
                "task_key": "setup_dlt_meta_pipeline_spec",
                "description": "Sets up metadata tables for DLT-META",
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "python_wheel_task": {
                    "package_name": "dlt_meta",
                    "entry_point": "run",
                    "named_parameters": {
                        "onboard_layer": "bronze",
                        "database": job_spec_dict['database'],
                        "onboarding_file_path": f"{job_spec_dict['dbfs_tmp_path']}/integration-tests/conf/dlt-meta/onboarding.json",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": job_spec_dict['env']
                    },
                },
                "libraries": [
                    {"whl": job_spec_dict['whl_path']}
                ]
            },
            {
                "task_key": "publish_events",
                "description": "Publish Events to Eventhub",
                "depends_on": [
                    {
                        "task_key": "setup_dlt_meta_pipeline_spec"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/publish_events",
                    "base_parameters": {
                        "eventhub_name": args.__getattribute__("eventhub_name"),
                        "eventhub_namespace": args.__getattribute__("eventhub_namespace"),
                        "eventhub_secrets_scope_name": args.__getattribute__("eventhub_secrets_scope_name"),
                        "eventhub_accesskey_name": args.__getattribute__("eventhub_producer_accesskey_name"),
                        "eventhub_input_data": f"/{dbfs_file_path}/integration-tests/resources/data/iot/iot.json"
                    }
                }
            },
            {
                "task_key": "bronze_process",
                "depends_on": [
                    {
                        "task_key": "publish_events"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['bronze_pipeline_id']
                }
            },
            {
                "task_key": "Validate",
                "description": "Validate Tables",
                "depends_on": [
                    {
                        "task_key": "bronze_process"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/validate",
                    "base_parameters": {
                        "database": f"bronze_{job_spec_dict['run_id']}",
                        "dbfs_tmp_path": job_spec_dict['dbfs_tmp_path'],
                        "run_id": job_spec_dict['run_id']
                    }
                }

            }
        ]
    }
    return job_spec


def create_kafka_workflow_spec(args, job_spec_dict):
    """Create Job specification."""
    dbfs_file_path = job_spec_dict['dbfs_tmp_path'].replace(":", "")
    job_spec = {
        "run_name": f"dlt-meta-integration-test-{job_spec_dict['run_id']}",
        "tasks": [
            {
                "task_key": "setup_dlt_meta_pipeline_spec",
                "description": "Sets up metadata tables for DLT-META",
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "python_wheel_task": {
                    "package_name": "dlt_meta",
                    "entry_point": "run",
                    "named_parameters": {
                        "onboard_layer": "bronze",
                        "database": job_spec_dict['database'],
                        "onboarding_file_path": f"{job_spec_dict['dbfs_tmp_path']}/integration-tests/conf/dlt-meta/onboarding.json",
                        "bronze_dataflowspec_table": "bronze_dataflowspec_cdc",
                        "import_author": "Ravi",
                        "version": "v1",
                        "bronze_dataflowspec_path": f"{job_spec_dict['dbfs_tmp_path']}/data/dlt_spec/bronze",
                        "overwrite": "True",
                        "env": job_spec_dict['env']
                    },
                },
                "libraries": [
                    {"whl": job_spec_dict['whl_path']}
                ]
            },
            {
                "task_key": "publish_events",
                "description": "Publish Events to Eventhub",
                "depends_on": [
                    {
                        "task_key": "setup_dlt_meta_pipeline_spec"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/publish_events",
                    "base_parameters": {
                        "kafka_name": args.__getattribute__("kafka_topic_name"),
                        "kafka_broker": args.__getattribute__("kafka_broker"),
                        "kafka_input_data": f"/{dbfs_file_path}/integration-tests/resources/data/iot/iot.json"
                    }
                }
            },
            {
                "task_key": "bronze_process",
                "depends_on": [
                    {
                        "task_key": "publish_events"
                    }
                ],
                "pipeline_task": {
                    "pipeline_id": job_spec_dict['bronze_pipeline_id']
                }
            },
            {
                "task_key": "Validate",
                "description": "Validate Tables",
                "depends_on": [
                    {
                        "task_key": "bronze_process"
                    }
                ],
                "new_cluster": {
                    "spark_version": job_spec_dict['dbr_version'],
                    "num_workers": 0,
                    "node_type_id": job_spec_dict['node_type_id'],
                    "data_security_mode": "LEGACY_SINGLE_USER",
                    "runtime_engine": "STANDARD",
                    "spark_conf": {
                        "spark.master": "local[*, 4]",
                        "spark.databricks.cluster.profile": "singleNode",
                    },
                    "custom_tags": {
                        "ResourceClass": "SingleNode",
                    }
                },
                "notebook_task": {
                    "notebook_path": f"{job_spec_dict['runners_nb_path']}/runners/validate",
                    "base_parameters": {
                        "database": f"bronze_{job_spec_dict['run_id']}",
                        "dbfs_tmp_path": job_spec_dict['dbfs_tmp_path'],
                        "run_id": job_spec_dict['run_id']
                    }
                }

            }
        ]
    }
    return job_spec


def create_dlt_meta_pipeline(
        pipeline_service: DeltaPipelinesService,
        runners_nb_path,
        run_id, configuration={}):
    """Create DLT pipeline."""
    return pipeline_service.create(
        name=f"dlt-meta-{configuration['layer']}-integration-test-{run_id}",
        clusters=[
            {
                "label": "default",
                "num_workers": 1
            }
        ],
        configuration=configuration,
        libraries=[
            {
                "notebook": {
                    "path": f"{runners_nb_path}/runners/init_dlt_meta_pipeline"
                }
            }
        ],
        target=f"{configuration['layer']}_{run_id}"
    )['pipeline_id']


class JobSubmitRunner():
    """Job Runner class."""

    def __init__(self, job_client: JobsService, job_dict):
        """Init method."""
        self.job_dict = job_dict
        self.job_client = job_client

    def submit(self):
        """Submit job."""
        return self.job_client.submit_run(**self.job_dict)

    def monitor(self, run_id):
        """Monitor job using runId."""
        while True:
            self.run_res = self.job_client.get_run(run_id)
            self.run_url = self.run_res["run_page_url"]
            self.run_life_cycle_state = self.run_res['state']['life_cycle_state']
            self.run_result_state = self.run_res['state'].get('result_state')
            self.run_state_message = self.run_res['state'].get('state_message')

            if self.run_life_cycle_state in ['PENDING', 'RUNNING', 'TERMINATING']:
                print("Job still running current life Cycle State is " + self.run_life_cycle_state)
            elif self.run_life_cycle_state in ['TERMINATED']:
                print("Job terminated")

                if self.run_result_state in ['SUCCESS']:
                    print("Job Succeeded")
                    print(f"Run URL {self.run_url}")
                    break
                else:
                    print("Job failed with the state of " + self.run_result_state)
                    print(self.run_state_message)
                    break
            else:
                print(
                    "Job was either Skipped or had Internal error please check the job ui and run in Databricks to see why")
                print(self.run_state_message)
                break

            time.sleep(20)


def generate_onboarding_file(args, dbfs_tmp_path, run_id):
    """Generate onboarding file from template."""
    cloudfiles_template = "integration-tests/conf/dlt-meta/cloudfiles-onboarding.template"
    eventhub_template = "integration-tests/conf/dlt-meta/eventhub-onboarding.template"
    kafka_template = "integration-tests/conf/dlt-meta/kafka-onboarding.template"

    source = args.__getattribute__("source").lower()
    if source == "cloudfiles":
        create_cloudfiles_onboarding(cloudfiles_template, dbfs_tmp_path, run_id)
    elif source == "eventhub":
        create_eventhub_onboarding(args, eventhub_template, dbfs_tmp_path, run_id)
    elif source == "kafka":
        create_kafka_onboarding(args, kafka_template, dbfs_tmp_path, run_id)


def create_kafka_onboarding(args, kafka_template, dbfs_tmp_path, run_id):
    """Create eventhub onboarding file."""
    with open(f"{kafka_template}") as f:
        onboard_obj = json.load(f)
    kafka_topic = args.__getattribute__("kafka_topic_name").lower()
    kafka_bootstrap_servers = args.__getattribute__("kafka_broker").lower()
    for data_flow in onboard_obj:
        for key, value in data_flow.items():
            if key == "source_details":
                for source_key, source_value in value.items():
                    if 'dbfs_path' in source_value:
                        data_flow[key][source_key] = source_value.format(dbfs_path=dbfs_tmp_path)
                    if 'kafka_topic' in source_value:
                        data_flow[key][source_key] = source_value.format(kafka_topic=kafka_topic)
                    if 'kafka_bootstrap_servers' in source_value:
                        data_flow[key][source_key] = source_value.format(kafka_bootstrap_servers=kafka_bootstrap_servers)
            if 'dbfs_path' in value:
                data_flow[key] = value.format(dbfs_path=dbfs_tmp_path)
            elif 'run_id' in value:
                data_flow[key] = value.format(run_id=run_id)
    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def create_eventhub_onboarding(args, eventhub_template, dbfs_tmp_path, run_id):
    """Create eventhub onboarding file."""
    with open(f"{eventhub_template}") as f:
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
                        data_flow[key][source_key] = source_value.format(dbfs_path=dbfs_tmp_path)
                    if 'eventhub_name' in source_value:
                        data_flow[key][source_key] = source_value.format(eventhub_name=eventhub_name)
                    if 'eventhub_accesskey_name' in source_value:
                        data_flow[key][source_key] = source_value.format(eventhub_accesskey_name=eventhub_accesskey_name)
                    if 'eventhub_secrets_scope_name' in source_value:
                        data_flow[key][source_key] = source_value.format(eventhub_secrets_scope_name=eventhub_secrets_scope_name)
                    if 'eventhub_nmspace' in source_value:
                        data_flow[key][source_key] = source_value.format(eventhub_nmspace=eventhub_namespace)
                    if 'eventhub_port' in source_value:
                        data_flow[key][source_key] = source_value.format(eventhub_port=eventhub_port)
            if 'dbfs_path' in value:
                data_flow[key] = value.format(dbfs_path=dbfs_tmp_path)
            elif 'run_id' in value:
                data_flow[key] = value.format(run_id=run_id)

    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def create_cloudfiles_onboarding(onboarding_file_template, dbfs_tmp_path, run_id):
    """Create onboarding file for cloudfiles as source."""
    with open(f"{onboarding_file_template}") as f:
        onboard_obj = json.load(f)

    for data_flow in onboard_obj:
        for key, value in data_flow.items():
            if key == "source_details":
                for source_key, source_value in value.items():
                    if 'dbfs_path' in source_value:
                        data_flow[key][source_key] = source_value.format(dbfs_path=dbfs_tmp_path)
            if 'dbfs_path' in value:
                data_flow[key] = value.format(dbfs_path=dbfs_tmp_path)
            elif 'run_id' in value:
                data_flow[key] = value.format(run_id=run_id)

    with open("integration-tests/conf/dlt-meta/onboarding.json", "w") as onboarding_file:
        json.dump(onboard_obj, onboarding_file)


def main():
    """Entry method to run integration tests."""
    args = process_arguments()

    api_client = get_api_client()
    username = api_client.perform_query("GET", "/preview/scim/v2/Me").get("userName")
    run_id = uuid.uuid4().hex
    dbfs_tmp_path = f"{args.__dict__['dbfs_path']}/{run_id}"
    database = f"dlt_meta_framework_it_{run_id}"
    int_tests = "./integration-tests"
    runners_nb_path = f"/Users/{username}/dlt-meta_int_tests/{run_id}"
    runners_full_local_path = None
    source = args.__dict__['source']
    if source.lower() == "cloudfiles":
        runners_full_local_path = './integration-tests/cloud_files_runners.dbc'
    elif source.lower() == "eventhub":
        runners_full_local_path = './integration-tests/eventhub_runners.dbc'
    elif source.lower() == "kafka":
        runners_full_local_path = './integration-tests/kafka_runners.dbc'
    else:
        raise Exception("Supported source not found in argument")

    dbfs_service = DbfsService(api_client)
    jobs_service = JobsService(api_client)
    workspace_service = WorkspaceService(api_client)
    pipeline_service = DeltaPipelinesService(api_client)
    try:
        generate_onboarding_file(args, dbfs_tmp_path, run_id)
        whl_path, file_whl_path = build_and_upload_package(dbfs_service, dbfs_tmp_path)
        DbfsApi(api_client).cp(True, True, int_tests, dbfs_tmp_path + "/integration-tests/")
        # os.remove(f"{dbfs_tmp_path}/integration-tests/conf/dlt-meta/onboarding.json")
        fp = open(runners_full_local_path, "rb")
        workspace_service.mkdirs(path=runners_nb_path)
        workspace_service.import_workspace(path=f"{runners_nb_path}/runners", format="DBC",
                                           content=base64.encodebytes(fp.read()).decode('utf-8'))
        bronze_pipeline_id = create_dlt_meta_pipeline(
            pipeline_service, runners_nb_path, run_id, configuration={
                "dlt_meta_whl": file_whl_path,
                "layer": "bronze",
                "bronze.group": "A1",
                "bronze.dataflowspecTable": f"{database}.bronze_dataflowspec_cdc"
            }
        )

        cloud_node_type_id_dict = {"aws": "i3.xlarge",
                                   "azure": "Standard_D3_v2",
                                   "gcp": "n1-highmem-4"
                                   }
        job_spec_dict = {"run_id": run_id,
                         "whl_path": whl_path,
                         "dbfs_tmp_path": dbfs_tmp_path,
                         "runners_nb_path": runners_nb_path,
                         "database": database,
                         "env": "it",
                         "bronze_pipeline_id": bronze_pipeline_id,
                         "node_type_id": cloud_node_type_id_dict[args.__dict__['cloud_provider_name']],
                         "dbr_version": args.__dict__['dbr_version']
                         }

        if source.lower() == "cloudfiles":
            silver_pipeline_id = create_dlt_meta_pipeline(
                pipeline_service, runners_nb_path, run_id, configuration={
                    "dlt_meta_whl": file_whl_path,
                    "layer": "silver",
                    "silver.group": "A1",
                    "silver.dataflowspecTable": f"{database}.silver_dataflowspec_cdc"
                }
            )
            job_spec_dict["silver_pipeline_id"] = silver_pipeline_id

        if source.lower() == "cloudfiles":
            job_spec = create_cloudfiles_workflow_spec(job_spec_dict)
        elif source.lower() == "eventhub":
            job_spec = create_eventhub_workflow_spec(args, job_spec_dict)
        elif source.lower() == "kafka":
            job_spec = create_kafka_workflow_spec(args, job_spec_dict)

        job_submit_runner = JobSubmitRunner(jobs_service, job_spec)

        job_run_info = job_submit_runner.submit()
        print(f"Run URL {job_run_info['run_id']}")

        job_submit_runner.monitor(job_run_info['run_id'])

    except Exception as e:
        print(e)

    finally:
        DbfsApi(api_client).cp(
            True, True,
            dbfs_tmp_path + "/integration-test-output.csv", f"./integration-test-output_{run_id}.csv"
        )
        pipeline_service.delete(bronze_pipeline_id)
        if source.lower() == "cloudfiles":
            pipeline_service.delete(silver_pipeline_id)
        dbfs_service.delete(dbfs_tmp_path, True)
        workspace_service.delete(runners_nb_path, True)
        try:
            os.remove("integration-tests/conf/dlt-meta/onboarding.json")
        except Exception as e:
            print(e)


def process_arguments():
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cloud_provider_name",
                        help="provide cloud provider name. Supported values are aws , azure , gcp")
    parser.add_argument("--dbr_version", help="Provide databricks runtime spark version e.g 11.3.x-scala2.12")
    parser.add_argument("--dbfs_path",
                        help="Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/")
    parser.add_argument("--source", help="Provide source type e.g --source=cloudfiles")
    parser.add_argument("--eventhub_name", help="Provide eventhub_name e.g --eventhub_name=iot")
    parser.add_argument("--eventhub_producer_accesskey_name", help="Provide access key that has write permission on the eventhub e.g --eventhub_producer_accesskey_name=iotProducerAccessKey")
    parser.add_argument("--eventhub_consumer_accesskey_name", help="Provide access key that has read permission on the eventhub  e.g --eventhub_consumer_accesskey_name=iotConsumerAccessKey")
    parser.add_argument("--eventhub_secrets_scope_name",
                        help="Provide eventhub_secrets_scope_name e.g --eventhub_secrets_scope_name=eventhubs_creds")
    parser.add_argument("--eventhub_namespace", help="Provide eventhub_namespace e.g --eventhub_namespace=topic-standard")
    parser.add_argument("--eventhub_port", help="Provide eventhub_port e.g --eventhub_port=9093")
    parser.add_argument("--kafka_topic_name", help="Provide kafka topic name e.g --kafka_topic_name=iot")
    parser.add_argument("--kafka_broker", help="Provide kafka broker e.g --127.0.0.1:9092")

    args = parser.parse_args()
    mandatory_args = ["cloud_provider_name", "dbr_version", "source", "dbfs_path"]
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
        eventhub_madatory_args = ["eventhub_name", "eventhub_producer_accesskey_name", "eventhub_consumer_accesskey_name", "eventhub_secrets_scope_name", "eventhub_namespace", "eventhub_port"]
        check_mandatory_arg(args, eventhub_madatory_args)
    if source.lower() == "kafka":
        kafka_madatory_args = ["kafka_topic_name", "kafka_broker"]
        check_mandatory_arg(args, kafka_madatory_args)
    print(f"Parsing argument complete. args={args}")
    return args


def check_mandatory_arg(args, mandatory_args):
    """Check mandatory argument present."""
    for mand_arg in mandatory_args:
        if args.__dict__[f'{mand_arg}'] is None:
            raise Exception(f"Please provide '--{mand_arg}'")


if __name__ == "__main__":
    main()
