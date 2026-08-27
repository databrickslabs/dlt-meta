
import uuid
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)
import traceback


class SDPMETASinkDemo(SDPMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the SDP-META Sink Demo by calling the necessary methods in the correct order.

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
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_demo/{run_id}",
            source="kafka",
            kafka_template="demo/conf/json/kafka-sink-onboarding.template",
            kafka_source_topic=self.args["kafka_source_topic"],
            kafka_source_servers_secrets_scope_name=self.args["kafka_source_servers_secrets_scope_name"],
            kafka_source_servers_secrets_scope_key=self.args["kafka_source_servers_secrets_scope_key"],
            kafka_sink_topic=self.args["kafka_sink_topic"],
            kafka_sink_servers_secret_scope_name=self.args["kafka_sink_servers_secret_scope_name"],
            kafka_sink_servers_secret_scope_key=self.args["kafka_sink_servers_secret_scope_key"],
            env="demo",
            onboarding_file_path="demo/conf/json/onboarding.json",
            onboarding_file_format=self.args.get("onboarding_file_format") or "json",
            runners_full_local_path='./demo/notebooks/dlt_sink_runners/',
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_demo/"
                f"{run_id}/demo-output.csv"
            ),
        )

        return runner_conf

    def launch_workflow(self, runner_conf: SDPMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    sdp_meta_afam_demo_runner = SDPMETASinkDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = sdp_meta_afam_demo_runner.init_runner_conf()
    sdp_meta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
