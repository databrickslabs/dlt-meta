
import os
import uuid
import json
import yaml
from databricks.labs.sdp_meta.install import WorkspaceInstaller
from integration_tests.run_integration_tests import (
    SDPMETARunner,
    SDPMetaRunnerConf,
    get_workspace_api_client,
    process_arguments
)
import traceback


class SDPMETADABDemo(SDPMETARunner):

    def __init__(self, args, ws, base_dir):
        self.args = args
        self.ws = ws
        self.wsi = WorkspaceInstaller(ws)
        self.base_dir = base_dir

    def run(self, runner_conf: SDPMetaRunnerConf):
        """
        Runs the SDP-META DAB Demo by calling the necessary methods in the correct order.

        Parameters:
        - runner_conf: The SDPMetaRunnerConf object containing the runner configuration parameters.
        """
        try:
            self.initialize_uc_resources(runner_conf)
            self.generate_onboarding_files(runner_conf)
            # upload_files_to_databricks builds the local sdp-meta wheel and
            # uploads it to the run's UC volume, populating
            # runner_conf.remote_whl_path. We need that path to be present
            # before generate_var_people_yml writes the sdp_meta_wheel_path
            # variable, since the DAB job + pipelines reference it as
            # ${var.sdp_meta_wheel_path}. (sdp-meta is not on PyPI yet, so
            # the demo installs from the on-volume wheel.)
            self.upload_files_to_databricks(runner_conf)
            self.generate_var_people_yml(runner_conf)
        except Exception as e:
            print(e)
            traceback.print_exc()
        # finally:
        #     self.clean_up(runner_conf)

    def init_runner_conf(self) -> SDPMetaRunnerConf:
        """
        Initialize the runner configuration for running the DAB demo.

        Honors ``--onboarding_file_format`` (``json`` default, or ``yaml``/``yml``).
        All template/output paths are seeded with the JSON variant; when the
        format is ``yaml`` the dataclass post-init rewrites every relevant path
        through ``SDPMetaRunnerConf._to_yaml_variant`` (``/json/`` -> ``/yml/``,
        ``.template`` -> ``.template.yml``, ``.json`` -> ``.yml``), so we never
        duplicate path strings here.

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
            int_tests_dir="demo/dabs",
            sdp_meta_schema=f"sdp_meta_dataflowspecs_demo_{run_id}",
            bronze_schema=f"sdp_meta_bronze_demo_{run_id}",
            silver_schema=f"sdp_meta_silver_demo_{run_id}",
            runners_nb_path=f"/Users/{self.wsi._my_username}/sdp_meta_demo/{run_id}",
            source="cloudfiles",
            env="demo",
            onboarding_file_format=(
                self.args["onboarding_file_format"]
                if self.args.get("onboarding_file_format")
                else "json"
            ),
            cloudfiles_template="demo/dabs/conf/json/onboarding_bronze_silver_people.template",
            onboarding_file_path="demo/dabs/conf/json/onboarding_bronze_silver_people.json",
            onboarding_fanout_templates="demo/dabs/conf/json/onboarding_silver_fanout_people.template",
            onboarding_fanout_file_path="demo/dabs/conf/json/onboarding_silver_fanout_people.json",
            runners_full_local_path='./demo/dabs/notebooks/',
            test_output_file_path=(
                f"/Users/{self.wsi._my_username}/sdp_meta_demo/"
                f"{run_id}/demo-output.csv"
            ),
        )

        return runner_conf

    def generate_var_people_yml(self, runner_conf: SDPMetaRunnerConf):
        """Generate var_people.yml with configuration from runner_conf.

        The ``people_*_onboarding_file_path`` defaults are derived from the
        runner-rendered output paths (``runner_conf.onboarding_file_path`` /
        ``onboarding_fanout_file_path``) so they automatically pick up the
        ``.json`` vs ``.yml`` extension and the ``conf/json/`` vs ``conf/yml/``
        bucket the runner has resolved for the chosen
        ``--onboarding_file_format``. The DAB job task then passes the right
        on-volume path to ``OnboardDataflowspec``.
        """
        uc_vol_full_path = f"{runner_conf.uc_volume_path}{runner_conf.int_tests_dir}"
        # Pick the on-volume path that matches the local rendered output
        # (e.g. demo/dabs/conf/yml/onboarding_bronze_silver_people.yml).
        people_onboarding_basename = os.path.basename(runner_conf.onboarding_file_path)
        people_onboarding_subdir = os.path.basename(
            os.path.dirname(runner_conf.onboarding_file_path)
        )
        people_fanout_basename = os.path.basename(runner_conf.onboarding_fanout_file_path)
        people_fanout_subdir = os.path.basename(
            os.path.dirname(runner_conf.onboarding_fanout_file_path)
        )
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
                "sdp_meta_schema": {
                    "description": "The schema name for the pipelines",
                    "type": "string",
                    "default": f"{runner_conf.sdp_meta_schema}"
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
                    "default": (
                        f"{uc_vol_full_path}/conf/{people_onboarding_subdir}/"
                        f"{people_onboarding_basename}"
                    )
                },
                "people_fanout_onboarding_file_path": {
                    "description": "The path to the onboarding file for people",
                    "type": "string",
                    "default": (
                        f"{uc_vol_full_path}/conf/{people_fanout_subdir}/"
                        f"{people_fanout_basename}"
                    )
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
                },
                "sdp_meta_wheel_path": {
                    "description": (
                        "On-volume path to the locally-built sdp-meta wheel "
                        "uploaded by demo/generate_dabs_resources.py. Referenced "
                        "by the onboarding job's serverless dependency list and "
                        "by the pipeline notebooks via spark.conf.get('sdp_meta_whl'). "
                        "Used because the package is not yet published to PyPI."
                    ),
                    "type": "string",
                    "default": runner_conf.remote_whl_path
                }
            }
        }
        var_people_path = f"{runner_conf.int_tests_dir}/resources/var_people.yml"
        with open(var_people_path, "w") as f:
            yaml.dump(var_people_content, f, sort_keys=False, default_flow_style=False)
        print(f"Generated var_people.yml at {var_people_path}")

    def generate_onboarding_files(self, runner_conf: SDPMetaRunnerConf):
        """Render onboarding template(s) into the chosen format.

        Each (template, output) pair is read as text, gets ``{placeholder}``
        tokens substituted, and is then parsed + re-emitted in the format
        implied by the output file's extension. JSON mode writes ``.json``,
        YAML mode writes ``.yml`` — both via the runner-resolved
        ``runner_conf.onboarding_file_path`` / ``onboarding_fanout_file_path``
        so the bucket (``conf/json/`` vs ``conf/yml/``) and extension stay
        consistent with what ``SDPMetaRunnerConf._to_yaml_variant`` produced.
        """
        string_subs = {
            "{uc_volume_path}": runner_conf.uc_volume_path,
            "{uc_catalog_name}": runner_conf.uc_catalog_name,
            "{bronze_schema}": runner_conf.bronze_schema,
            "{silver_schema}": runner_conf.silver_schema,
        }
        pairs = []
        if runner_conf.cloudfiles_template:
            pairs.append((runner_conf.cloudfiles_template, runner_conf.onboarding_file_path))
        if runner_conf.onboarding_fanout_templates:
            pairs.append(
                (runner_conf.onboarding_fanout_templates, runner_conf.onboarding_fanout_file_path)
            )

        for template_path, output_path in pairs:
            with open(template_path, "r") as f:
                rendered = f.read()
            for key, val in string_subs.items():
                val = "" if val is None else val
                rendered = rendered.replace(key, val)

            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            lower_output = output_path.lower()
            if lower_output.endswith((".yml", ".yaml")):
                # Parse YAML to validate after substitution, then re-emit
                # canonically so the on-disk file is well-formed YAML rather
                # than just the substituted template text.
                data = yaml.safe_load(rendered)
                with open(output_path, "w") as f:
                    yaml.safe_dump(data, f, sort_keys=False, default_flow_style=False)
            else:
                with open(output_path, "w") as f:
                    json.dump(json.loads(rendered), f, indent=4)
            print(f"Generated {output_path} from {template_path}")


def main():
    args = process_arguments()
    workspace_client = get_workspace_api_client(args["profile"])
    sdp_meta_afam_demo_runner = SDPMETADABDemo(args, workspace_client, "demo")
    print("initializing complete")
    runner_conf = sdp_meta_afam_demo_runner.init_runner_conf()
    sdp_meta_afam_demo_runner.run(runner_conf)


if __name__ == "__main__":
    main()
