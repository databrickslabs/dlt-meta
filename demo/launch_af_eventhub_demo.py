
import uuid
import traceback
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
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
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.clean_up(runner_conf)

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
            int_tests_dir="demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_demo/{run_id}",
            source="eventhub",
            eventhub_template="demo/conf/eventhub-onboarding.template",
            onboarding_file_path="demo/conf/onboarding.json",
            env="demo",
            # eventhub provided args
            eventhub_name=self.args["eventhub_name"],
            eventhub_name_append_flow=self.args["eventhub_name_append_flow"],
            eventhub_producer_accesskey_name=self.args[
                "eventhub_consumer_accesskey_name"
            ],
            eventhub_consumer_accesskey_name=self.args[
                "eventhub_consumer_accesskey_name"
            ],
            eventhub_accesskey_secret_name=self.args["eventhub_accesskey_secret_name"],
            eventhub_secrets_scope_name=self.args["eventhub_secrets_scope_name"],
            eventhub_namespace=self.args["eventhub_namespace"],
            eventhub_port=self.args["eventhub_port"]
        )
        runner_conf.uc_catalog_name = self.args['uc_catalog_name']
        runner_conf.runners_full_local_path = 'demo/notebooks/afam_eventhub_runners'
        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)
        return created_job


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args['profile'])
    dltmeta_afam_demo_runner = DLTMETAFEHDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
