
import uuid
import json
import yaml
from src.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    DLTMETARunner,
    DLTMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)
import traceback


class DLTMETADABDemo(DLTMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: DLTMetaRunnerConf):
        """
        Runs the DLT-META DAB Demo by calling the necessary methods in the correct order.

        Parameters:
        - runner_conf: The DLTMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.initialize_uc_resources(runner_conf)
            self.generate_var_people_yml(runner_conf)  # Generate var_people.yml
            self.generate_onboarding_files(runner_conf)
            self.upload_files_to_databricks(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

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
            int_tests_dir="demo/dabs",
            dlt_meta_schema=f"dlt_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"dlt_meta_bronze_demo_{run_id}",
            silver_schema=f"dlt_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/dlt_meta_demo/{run_id}",
            source="cloudfiles",
            env="demo",
            cloudfiles_template="demo/dabs/conf/onboarding_bronze_silver_people.template",
            onboarding_file_path="demo/dabs/conf/onboarding_bronze_silver_people.json",
            onboarding_fanout_templates="demo/dabs/conf/onboarding_silver_fanout_people.template",
            onboarding_fanout_file_path="demo/dabs/conf/onboarding_silver_fanout_people.json",
            runners_full_local_path='./demo/dabs/notebooks/',
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/dlt_meta_demo/"
                f"{run_id}/demo-output.csv"
            ),
        )
        if '-' in runner_conf.uc_catalog_name or runner_conf.uc_catalog_name[0].isdigit():
            print(
                "\nERROR: 'uc_catalog_name' can only contain ASCII letters ('a' - 'z', 'A' - 'Z'),"
                " digits ('0' - '9'), and underbar ('_'). Must also not start with a digit. Exiting."
            )
            exit(1)

        return runner_conf

    def generate_var_people_yml(self, runner_conf: DLTMetaRunnerConf):
        """Generate var_people.yml with configuration from runner_conf."""
        uc_vol_full_path = f"{runner_conf.uc_volume_path}{runner_conf.int_tests_dir}"
        var_people_content = {
            "variables": {
                "dev_catalog_name": {
                    "description": "The catalog name for development environment",
                    "type": "string",
                    "default": runner_conf.uc_catalog_name
                },
                "prod_catalog_name": {
                    "description": "The catalog name for production environment",
                    "type": "string",
                    "default": runner_conf.uc_catalog_name
                },
                "dlt_meta_schema": {
                    "description": "The schema name for the pipelines",
                    "type": "string",
                    "default": f"{runner_conf.dlt_meta_schema}"
                },
                "bronze_schema": {
                    "description": "The schema name for the bronze pipelines",
                    "type": "string",
                    "default": f"{runner_conf.bronze_schema}"
                },
                "silver_schema": {
                    "description": "The schema name for the silver pipelines",
                    "type": "string",
                    "default": f"{runner_conf.silver_schema}"
                },
                "photon_enabled": {
                    "description": "Whether Photon is enabled for the pipelines",
                    "type": "bool",
                    "default": True
                },
                "serverless_enabled": {
                    "description": "Whether serverless mode is enabled for the pipelines",
                    "type": "bool",
                    "default": True
                },
                "bronze_dataflowspecTable": {
                    "description": "The table name for the bronze data flow specification",
                    "type": "string",
                    "default": "bronze_dataflowspec_table"
                },
                "silver_dataflowspecTable": {
                    "description": "The table name for the silver data flow specification",
                    "type": "string",
                    "default": "silver_dataflowspec_table"
                },
                "author": {
                    "description": "The author of the import",
                    "type": "string",
                    "default": runner_conf.username
                },
                "people_onboarding_file_path": {
                    "description": "The path to the onboarding file for people",
                    "type": "string",
                    "default": f"{uc_vol_full_path}/conf/onboarding_bronze_silver_people.json"
                },
                "people_fanout_onboarding_file_path": {
                    "description": "The path to the onboarding file for people",
                    "type": "string",
                    "default": f"{uc_vol_full_path}/conf/onboarding_silver_fanout_people.json"
                },
                "dummy_param": {
                    "description": "A dummy parameter for testing purposes",
                    "type": "string",
                    "default": "Hello Bronze 2"
                },
                "version": {
                    "description": "The version of the data flow specification",
                    "type": "string",
                    "default": "v1"
                },
                "uc_enabled": {
                    "description": "Whether Unity Catalog is enabled for the pipelines",
                    "type": "bool",
                    "default": True
                },
                "dev_development_enabled": {
                    "description": "Whether development mode is enabled for the pipelines",
                    "type": "bool",
                    "default": True
                },
                "prod_development_enabled": {
                    "description": "Whether production development mode is enabled for the pipelines",
                    "type": "bool",
                    "default": False
                }
            }
        }
        var_people_path = f"{runner_conf.int_tests_dir}/resources/var_people.yml"
        with open(var_people_path, "w") as f:
            yaml.dump(var_people_content, f, sort_keys=False, default_flow_style=False)
        print(f"Generated var_people.yml at {var_people_path}")

    def generate_onboarding_files(self, runner_conf: DLTMetaRunnerConf):
        string_subs = {
            "{uc_volume_path}": runner_conf.uc_volume_path,
            "{uc_catalog_name}": runner_conf.uc_catalog_name,
            "{bronze_schema}": runner_conf.bronze_schema,
            "{silver_schema}": runner_conf.silver_schema,
        }
        if runner_conf.cloudfiles_template:
            with open(f"{runner_conf.cloudfiles_template}", "r") as f:
                onboard_json = f.read()

            for key, val in string_subs.items():
                val = "" if val is None else val  # Ensure val is a string
                onboard_json = onboard_json.replace(key, val)
            with open(runner_conf.onboarding_file_path, "w") as onboarding_file:
                json.dump(json.loads(onboard_json), onboarding_file, indent=4)

        if runner_conf.onboarding_fanout_templates:
            template = runner_conf.onboarding_fanout_templates
            with open(f"{template}", "r") as f:
                onboard_json = f.read()

            for key, val in string_subs.items():
                onboard_json = onboard_json.replace(key, val)

            with open(runner_conf.onboarding_fanout_file_path, "w") as onboarding_file:
                json.dump(json.loads(onboard_json), onboarding_file, indent=4)


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    dltmeta_afam_demo_runner = DLTMETADABDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = dltmeta_afam_demo_runner.init_runner_conf()
    dltmeta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
