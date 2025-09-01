
import uuid
import traceback
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)


class ApplyChangesFromSnapshotDemo(DLTMETARunner):
    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META Apply Changes from Snapshot Demo by calling the necessary methods in the correct order.

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
        # finally:
        #     self.clean_up(runner_conf)

    def init_runner_conf(self) -> DLTMetaRunnerConf:
        run_id = uuid.uuid4().hex
        runner_conf = DLTMetaRunnerConf(
            run_id=run_id,
            username=self.wsi._my_username,
            int_tests_dir="demo",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_demo/{run_id}",
            runners_full_local_path="demo/notebooks/snapshot_runners",
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/dlt_meta_demo/"
                f"{run_id}/dlt_meta_acfs_demo-output.csv"
            ),
            source="snapshot",
            snapshot_template="demo/conf/snapshot-onboarding.template",
            onboarding_file_path="demo/conf/onboarding.json",
            env="demo"
        )
        runner_conf.uc_catalog_name = self.args['uc_catalog_name']
        if '-' in runner_conf.uc_catalog_name or runner_conf.uc_catalog_name[0].isdigit():
            print("\nERROR: 'uc_catalog_name' can only contain ASCII letters ('a' - 'z', 'A' - 'Z'), digits ('0' - '9'), and underbar ('_'). Must also not start with a digit. Exiting.")
            exit(1)
        return runner_conf

    def launch_workflow(self, runner_conf: DLTMetaRunnerConf):
        created_job = self.create_workflow_spec(runner_conf)
        self.open_job_url(runner_conf, created_job)


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    dltmeta_afam_demo_runner = ApplyChangesFromSnapshotDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
