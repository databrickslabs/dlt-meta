
import uuid
import traceback
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class SDPMETAFEHDemo(SDPMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the SDP-META Append Flow Eventhub Demo by calling the necessary methods in the correct order.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.init_sdp_meta_runner_conf(runner_conf)
            self.create_bronze_silver_dlt(runner_conf)
            self.launch_workflow(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        """
        Initialize the runner configuration for running integration tests.

        Returns:
        -------
        SDPMetaRunnerConf
            The initialized runner configuration.
        """
        run_id = uuid.uuid4().hex
        runner_conf = SDPMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_demo/{run_id}",
            source="eventhub",
            eventhub_template="demo/conf/json/eventhub-onboarding.template",
            onboarding_file_path="demo/conf/json/onboarding.json",
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
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

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)
        return created_job


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args['profile'])
    sdp_meta_afam_demo_runner = SDPMETAFEHDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = sdp_meta_afam_demo_runner.init_runner_conf()
    sdp_meta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
