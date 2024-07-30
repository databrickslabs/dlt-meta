
import uuid
import webbrowser
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    cloud_node_type_id_dict,
    get_workspace_api_client,
    process_arguments
)


class DLTMETAFEHDemo(DLTMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META Append Flow Autoloader Demo by calling the necessary methods in the correct order.

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
            int_tests_dir="file:./demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_demo/{run_id}",
            source="eventhub",
            node_type_id=cloud_node_type_id_dict[self.args.__dict__['cloud_provider_name']],
            dbr_version=self.args.__dict__['dbr_version'],
            eventhub_template="demo/conf/eventhub-onboarding.template",
            onboarding_file_path="demo/conf/onboarding.json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args.__dict__['uc_catalog_name']
        runner_conf.runners_full_local_path = './demo/dbc/afam_eventhub_runners.dbc'
        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_eventhub_workflow_spec(runner_conf)
        runner_conf.job_id = created_job.job_id
        print(f"Job created successfully. job_id={created_job.job_id}, started run...")
        webbrowser.open(f"{self.ws.config.host}/jobs/{created_job.job_id}?o={self.ws.get_workspace_id()}")
        print(f"Waiting for job to complete. job_id={created_job.job_id}")
        run_by_id = self.ws.jobs.run_now(job_id=created_job.job_id).result()
        print(f"Job run finished. run_id={run_by_id}")
        return created_job


afam_args_map = {
    "--profile": "provide databricks cli profile name, if not provide databricks_host and token",
    "--uc_catalog_name": "provide databricks uc_catalog name, this is required to create volume, schema, table",
    "--cloud_provider_name": "provide cloud provider name. Supported values are aws , azure , gcp",
    "--dbr_version": "Provide databricks runtime spark version e.g 11.3.x-scala2.12",
    "--dbfs_path": "Provide databricks workspace dbfs path where you want run integration tests \
                        e.g --dbfs_path=dbfs:/tmp/DLT-META/",
    "--eventhub_name": "Provide eventhub_name e.g --eventhub_name=iot",
    "--eventhub_name_append_flow": "Provide eventhub_name_append_flow e.g --eventhub_name_append_flow=iot_af",
    "--eventhub_producer_accesskey_name": "Provide access key that has write permission on the eventhub",
    "--eventhub_consumer_accesskey_name": "Provide access key that has read permission on the eventhub",
    "--eventhub_secrets_scope_name": "Provide eventhub_secrets_scope_name e.g \
                --eventhub_secrets_scope_name=eventhubs_creds",
    "--eventhub_accesskey_secret_name": "Provide eventhub_accesskey_secret_name e.g \
                -eventhub_accesskey_secret_name=RootManageSharedAccessKey",
    "--eventhub_namespace": "Provide eventhub_namespace e.g --eventhub_namespace=topic-standard",
    "--eventhub_port": "Provide eventhub_port e.g --eventhub_port=9093",
}

afeh_mandatory_args = ["uc_catalog_name", "cloud_provider_name", "dbr_version", "dbfs_path", "eventhub_name",
                       "eventhub_name_append_flow", "eventhub_producer_accesskey_name",
                       "eventhub_consumer_accesskey_name", "eventhub_secrets_scope_name",
                       "eventhub_namespace", "eventhub_port"]


def main():
    args = process_arguments(afam_args_map, afeh_mandatory_args)
    workspace_client = get_workspace_api_client(args.profile)
    dltmeta_afam_demo_runner = DLTMETAFEHDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
