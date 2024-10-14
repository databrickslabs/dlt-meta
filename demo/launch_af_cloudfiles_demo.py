
import uuid
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)
import traceback


class DLTMETAFCFDemo(DLTMETARunner):

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
            uc_catalog_name=self.args["uc_catalog_name"],
            int_tests_dir="demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_demo/{run_id}",
            source="cloudfiles",
            cloudfiles_template="demo/conf/cloudfiles-onboarding.template",
            cloudfiles_A2_template="demo/conf/cloudfiles-onboarding_A2.template",
            onboarding_file_path="demo/conf/onboarding.json",
            onboarding_A2_file_path="demo/conf/onboarding_A2.json",
            env="demo",
            runners_full_local_path='./demo/notebooks/afam_cloudfiles_runners/',
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/dlt_meta_demo/"
                f"{run_id}/demo-output.csv"
            ),
        )

        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    dltmeta_afam_demo_runner = DLTMETAFCFDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
