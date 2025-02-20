"""Main entry point for the CLI."""

import logging
import json
import os
import sys
import uuid
import webbrowser
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import SchemasAPI, VolumeType
from src import __about__
from src.install import WorkspaceInstaller

logger = logging.getLogger('databricks.labs.dltmeta')


DLT_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install dlt-meta=={version}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""

cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


@dataclass
class OnboardCommand:
    """Class representing the onboarding command."""
    onboarding_file_path: str
    onboarding_files_dir_path: str
    onboard_layer: str
    env: str
    import_author: str
    version: str
    dlt_meta_schema: str
    dbfs_path: str = None
    cloud: str = None
    dbr_version: str = None
    serverless: bool = True
    bronze_schema: str = None
    silver_schema: str = None
    uc_enabled: bool = False
    uc_catalog_name: str = None
    uc_volume_path: str = None
    overwrite: bool = True
    bronze_dataflowspec_table: str = "bronze_dataflowspec"
    silver_dataflowspec_table: str = "silver_dataflowspec"
    bronze_dataflowspec_path: str = None
    silver_dataflowspec_path: str = None
    update_paths: bool = True

    def __post_init__(self):
        if not self.onboarding_file_path or self.onboarding_file_path == "":
            raise ValueError("onboarding_file_path is required")
        if not self.onboarding_files_dir_path or self.onboarding_files_dir_path == "":
            raise ValueError("onboarding_files_dir_path is required")
        if not self.onboard_layer or self.onboard_layer == "":
            raise ValueError("onboard_layer is required")
        if self.onboard_layer.lower() not in ["bronze", "silver", "bronze_silver"]:
            raise ValueError("onboard_layer must be one of bronze, silver, bronze_silver")
        # if self.uc_enabled == "":
        #     raise ValueError("uc_enabled is required, please set to True or False")
        if not self.uc_enabled and not self.dbfs_path:
            raise ValueError("dbfs_path is required")
        if not self.serverless:
            if not self.cloud:
                raise ValueError("cloud is required")
            if not self.dbr_version:
                raise ValueError("dbr_version is required")
        if self.onboard_layer and self.onboard_layer.lower() == "bronze_silver":
            if not self.uc_enabled:
                if not self.bronze_dataflowspec_path or self.silver_dataflowspec_path == "":
                    raise ValueError("bronze_dataflowspec_path is required")
                if not self.silver_dataflowspec_path or self.silver_dataflowspec_path == "":
                    raise ValueError("silver_dataflowspec_path is required")
        elif self.onboard_layer.lower() == "bronze":
            if not self.uc_enabled:
                if not self.bronze_dataflowspec_path:
                    raise ValueError("bronze_dataflowspec_path is required")
        elif self.onboard_layer.lower() == "silver":
            if not self.silver_dataflowspec_table:
                raise ValueError("silver_dataflowspec_table is required")
            if not self.uc_enabled:
                if not self.silver_dataflowspec_path:
                    raise ValueError("silver_dataflowspec_path is required")
        if not self.dlt_meta_schema:
            raise ValueError("dlt_meta_schema is required")
        if not self.overwrite:
            raise ValueError("overwrite is required")
        if not self.import_author:
            raise ValueError("import_author is required")
        if not self.version:
            raise ValueError("version is required")
        if not self.env:
            raise ValueError("env is required")


@dataclass
class DeployCommand:
    """Class representing the deploy command."""
    layer: str
    pipeline_name: str
    dlt_target_schema: str
    onboard_bronze_group: str = None
    onboard_silver_group: str = None
    dlt_meta_bronze_schema: str = None
    dlt_meta_silver_schema: str = None
    dataflowspec_bronze_table: str = None
    dataflowspec_silver_table: str = None
    num_workers: int = None
    uc_catalog_name: str = None
    dataflowspec_bronze_path: str = None
    dataflowspec_silver_path: str = None
    uc_enabled: bool = False
    serverless: bool = False
    dbfs_path: str = None

    def __post_init__(self):
        if self.uc_enabled and not self.uc_catalog_name:
            raise ValueError("uc_catalog_name is required")
        if not self.serverless and not self.num_workers:
            raise ValueError("num_workers is required")
        if not self.layer:
            raise ValueError("layer is required")
        if self.layer in ["bronze", "bronze_silver"]:
            if not self.onboard_bronze_group:
                raise ValueError("onboard_bronze_group is required")
            if self.uc_enabled and not self.dataflowspec_bronze_table:
                raise ValueError("dataflowspec_bronze_table is required")
            if not self.uc_enabled and not self.dataflowspec_bronze_path:
                raise ValueError("dataflowspec_bronze_path is required")
        if self.layer in ["silver", "bronze_silver"]:
            if not self.onboard_silver_group:
                raise ValueError("onboard_silver_group is required")
            if self.uc_enabled and not self.dataflowspec_silver_table:
                raise ValueError("dataflowspec_silver_table is required")
            if not self.uc_enabled and not self.dataflowspec_silver_path:
                raise ValueError("dataflowspec_silver_path is required")
        if not self.pipeline_name:
            raise ValueError("pipeline_name is required")
        if not self.dlt_target_schema:
            raise ValueError("dlt_target_schema is required")


class DLTMeta:
    """Class representing the DLT-META CLI."""

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
        self._wsi = WorkspaceInstaller(ws)
        self.version = __about__.__version__

    def _my_username(self):
        if not hasattr(self._ws, "_me"):
            _me = self._ws.current_user.me()
        else:
            _me = self._ws._me
        return _me.user_name

    def copy_to_uc_volume(self, src, dst):
        main_dir = src.replace('file:', '')
        base_dir_name = os.path.basename(os.path.normpath(main_dir))
        for root, dirs, files in os.walk(main_dir):
            for filename in files:
                target_dir = root[root.index(main_dir) + len(main_dir):len(root)]
                uc_volume_path = f"{dst}/{base_dir_name}/{target_dir}/{filename}".replace("//", "/")
                contents = open(os.path.join(root, filename), "rb")
                self._ws.files.upload(file_path=uc_volume_path, contents=contents, overwrite=True)

    def copy_to_dbfs(self, src, dst):
        dst = dst.replace('//', '/')
        main_dir = src.replace('file:', '')
        main_dir = main_dir.replace('//', '/')
        base_dir_name = None
        if main_dir.endswith('/'):
            base_dir_name = main_dir[:-1]
        if base_dir_name is None:
            base_dir_name = main_dir[main_dir.rfind('/') + 1:]
        else:
            base_dir_name = base_dir_name[base_dir_name.rfind('/') + 1:]
        for root, dirs, files in os.walk(main_dir):
            for filename in files:
                target_dir = root[root.index(main_dir) + len(main_dir):len(root)]
                dbfs_path = f"{dst}/{base_dir_name}/{target_dir}/{filename}"
                contents = open(os.path.join(root, filename), "rb")
                logger.info(
                    f"local_path={os.path.join(root, filename)} "
                    f"dbfs_path={dst}/{base_dir_name}/{target_dir}/{filename}"
                )
                self._ws.dbfs.upload(dbfs_path, contents, overwrite=True)

    def create_uc_volume(self, uc_catalog_name, dlt_meta_schema):
        volume_info = self._ws.volumes.create(
            catalog_name=uc_catalog_name,
            schema_name=dlt_meta_schema,
            name=dlt_meta_schema,
            volume_type=VolumeType.MANAGED,
        )
        return f"/Volumes/{volume_info.catalog_name}/{volume_info.schema_name}/{volume_info.name}/"

    def onboard(self, cmd: OnboardCommand):
        """launch the onboarding job."""
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        ob_file = open(cmd.onboarding_file_path, "rb")

        if cmd.uc_enabled:
            self.create_uc_schema(cmd.uc_catalog_name, cmd.dlt_meta_schema)
            cmd.uc_volume_path = self.create_uc_volume(cmd.uc_catalog_name, cmd.dlt_meta_schema)
            self.update_ws_onboarding_paths(cmd)
            self.copy_to_uc_volume(cmd.onboarding_files_dir_path, cmd.uc_volume_path + "/dltmeta_conf/")
            logger.info(f"uploading to  {cmd.uc_volume_path}/dltmeta_conf complete!!!")
        else:
            self._ws.dbfs.mkdirs(f"{cmd.dbfs_path}/dltmeta_conf/")
            self._ws.dbfs.upload(f"{cmd.dbfs_path}/dltmeta_conf/{onboarding_filename}", ob_file, overwrite=True)
            self.update_ws_onboarding_paths(cmd)
            onboarding_filename = os.path.basename(cmd.onboarding_file_path)
            self.copy_to_dbfs(cmd.onboarding_files_dir_path, cmd.dbfs_path + "/dltmeta_conf/")
            logger.info(f"uploading to  {cmd.dbfs_path}/dltmeta_conf complete!!!")
        created_job = self.create_onnboarding_job(cmd)
        logger.info(f"Waiting for job to complete. job_id={created_job.job_id}")
        run = self._ws.jobs.run_now(job_id=created_job.job_id)
        msg = (
            "DLT-META Onboarding Job(job_id={}) "
            "launched with run_id={}, Please check the job status in databricks workspace jobs tab"
        ).format(created_job.job_id, run.run_id)
        logger.info(msg)
        webbrowser.open(f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}")

    def create_uc_schema(self, uc_catalog_name, dlt_meta_schema):
        try:
            SchemasAPI(self._ws.api_client).get(full_name=f"{uc_catalog_name}.{dlt_meta_schema}")
        except Exception:
            msg = (
                "Schema {catalog}.{schema} not found. "
                "Creating schema={schema}"
            ).format(catalog=uc_catalog_name, schema=dlt_meta_schema)
            logger.info(msg)
            SchemasAPI(self._ws.api_client).create(
                catalog_name=uc_catalog_name,
                name=dlt_meta_schema,
                comment="dlt_meta framework schema"
            )

    def create_onnboarding_job(self, cmd: OnboardCommand):
        """Create the onboarding job."""
        if cmd.serverless:
            cluster_spec = None
        else:
            cluster_spec = compute.ClusterSpec(
                spark_version=cmd.dbr_version,
                num_workers=1,
                driver_node_type_id=cloud_node_type_id_dict[cmd.cloud],
                node_type_id=cloud_node_type_id_dict[cmd.cloud],
                data_security_mode=compute.DataSecurityMode.SINGLE_USER
                if cmd.uc_enabled else compute.DataSecurityMode.LEGACY_SINGLE_USER,
                spark_conf={},
                spark_env_vars={
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                }
            )
        named_parameters = self._get_onboarding_named_parameters(cmd)
        dltmeta_environments = [
            jobs.JobEnvironment(
                environment_key="dl_meta_cli_env",
                spec=compute.Environment(client="1",
                                         dependencies=[f"dlt-meta=={self.version}"]
                                         )
            )
        ]
        return self._ws.jobs.create(
            name="dlt_meta_onboarding_job",
            environments=None if not cmd.serverless else dltmeta_environments,
            tasks=[
                jobs.Task(
                    task_key="dlt_meta_onbarding_task",
                    description="test",
                    new_cluster=cluster_spec if not cmd.serverless else None,
                    environment_key="dl_meta_cli_env" if cmd.serverless else None,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters=named_parameters,
                    ),
                    libraries=[
                        jobs.compute.Library(
                            pypi=compute.PythonPyPiLibrary(package=f"dlt-meta=={self.version}")
                        )
                    ] if not cmd.serverless else None,
                ),
            ]
        )

    def _get_onboarding_named_parameters(self, cmd: OnboardCommand):
        named_parameters = {
            "onboard_layer": cmd.onboard_layer,
            "database":
                f"{cmd.uc_catalog_name}.{cmd.dlt_meta_schema}"
                if cmd.uc_enabled else cmd.dlt_meta_schema,
            "import_author": cmd.import_author,
            "version": cmd.version,
            "overwrite": "True" if cmd.overwrite else "False",
            "env": cmd.env,
            "uc_enabled": "True" if cmd.uc_enabled else "False"
        }
        if cmd.uc_enabled:
            named_parameters["onboarding_file_path"] = f"{cmd.uc_volume_path}/dltmeta_conf/{cmd.onboarding_file_path}"
        else:
            named_parameters["onboarding_file_path"] = f"{cmd.dbfs_path}/dltmeta_conf/{cmd.onboarding_file_path}"
        if cmd.onboard_layer == "bronze_silver":
            named_parameters["bronze_dataflowspec_table"] = cmd.bronze_dataflowspec_table
            named_parameters["silver_dataflowspec_table"] = cmd.silver_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["bronze_dataflowspec_path"] = cmd.bronze_dataflowspec_path
                named_parameters["silver_dataflowspec_path"] = cmd.silver_dataflowspec_path
        elif cmd.onboard_layer == "bronze":
            named_parameters["bronze_dataflowspec_table"] = cmd.bronze_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["bronze_dataflowspec_path"] = cmd.bronze_dataflowspec_path
        elif cmd.onboard_layer == "silver":
            named_parameters["silver_dataflowspec_table"] = cmd.silver_dataflowspec_table
            if not cmd.uc_enabled:
                named_parameters["silver_dataflowspec_path"] = cmd.silver_dataflowspec_path
        return named_parameters

    def _install_folder(self):
        return f"/Users/{self._my_username()}/dlt-meta"

    def _create_dlt_meta_pipeline(self, cmd: DeployCommand):
        """Create the DLT-META pipeline."""
        runner_notebook_py = DLT_META_RUNNER_NOTEBOOK.format(version=self.version).encode("utf8")
        runner_notebook_path = f"{self._install_folder()}/init_dlt_meta_pipeline.py"
        try:
            self._ws.workspace.mkdirs(self._install_folder())
        except DatabricksError as e:
            logger.error(e)
        self._ws.workspace.upload(runner_notebook_path, runner_notebook_py, overwrite=True)
        configuration = {
            "layer": cmd.layer,
        }
        if cmd.layer in ["bronze", "silver", "bronze_silver"]:
            if cmd.layer in ["bronze", "bronze_silver"]:
                configuration["bronze.group"] = cmd.onboard_bronze_group
                if cmd.uc_catalog_name:
                    configuration["bronze.dataflowspecTable"] = (
                        f"{cmd.uc_catalog_name}.{cmd.dlt_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
                else:
                    configuration["bronze.dataflowspecTable"] = (
                        f"{cmd.dlt_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
            if cmd.layer in ["silver", "bronze_silver"]:
                configuration["silver.group"] = cmd.onboard_silver_group
                if cmd.uc_catalog_name:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.uc_catalog_name}.{cmd.dlt_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
                    )
                else:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.dlt_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
                    )
        else:
            raise ValueError("layer must be one of bronze, silver, bronze_silver ")
        created = None
        configuration["version"] = self.version
        if cmd.uc_catalog_name:
            created = self._ws.pipelines.create(catalog=cmd.uc_catalog_name,
                                                name=cmd.pipeline_name,
                                                configuration=configuration,
                                                libraries=[
                                                    PipelineLibrary(
                                                        notebook=NotebookLibrary(
                                                            path=runner_notebook_path
                                                        )
                                                    )
                                                ],
                                                schema=cmd.dlt_target_schema,  # for DPM
                                                # target=cmd.dlt_target_schema,
                                                clusters=[pipelines.PipelineCluster(label="default",
                                                                                    num_workers=cmd.num_workers)]
                                                if not cmd.serverless else None,
                                                serverless=cmd.serverless if cmd.uc_enabled else None,
                                                channel="PREVIEW" if cmd.serverless else None
                                                )
        else:
            created = self._ws.pipelines.create(
                name=cmd.pipeline_name,
                configuration=configuration,
                libraries=[
                    PipelineLibrary(
                        notebook=NotebookLibrary(
                            path=runner_notebook_path
                        )
                    )
                ],
                target=cmd.dlt_target_schema,
                clusters=[pipelines.PipelineCluster(label="default", num_workers=cmd.num_workers)]
            )
        if created is None:
            raise Exception("Pipeline creation failed")
        return created.pipeline_id

    def deploy(self, cmd: DeployCommand):
        pipeline_id = self._create_dlt_meta_pipeline(cmd)
        update_response = self._ws.pipelines.start_update(pipeline_id=pipeline_id)
        msg = (
            f"dlt-meta pipeline={pipeline_id} created and launched with update_id={update_response.update_id}, "
            "Please check the pipeline status in databricks workspace under workflows -> Delta Live Tables tab"
        )
        logger.info(msg)
        webbrowser.open(f"{self._ws.config.host}/#joblist/pipelines/{pipeline_id}?o={self._ws.get_workspace_id()}/")

    def _load_onboard_config(self) -> OnboardCommand:
        onboard_cmd_dict = {}
        onboard_cmd_dict["uc_enabled"] = self._wsi._choice(
            "Run onboarding with unity catalog enabled?", ['True', 'False'])
        onboard_cmd_dict["uc_enabled"] = True if onboard_cmd_dict["uc_enabled"] == "True" else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["dbfs_path"] = None
            onboard_cmd_dict["uc_catalog_name"] = self._wsi._question(
                "Provide unity catalog name")
        else:
            onboard_cmd_dict["dbfs_path"] = self._wsi._question(
                "Provide dbfs path", default=f"dbfs:/dlt-meta_cli_demo_{uuid.uuid4().hex}")
        onboard_cmd_dict["serverless"] = self._wsi._choice(
            "Run onboarding with serverless?", ['True', 'False'])
        onboard_cmd_dict["serverless"] = True if onboard_cmd_dict["serverless"] == 'True' else False
        if onboard_cmd_dict["serverless"]:
            onboard_cmd_dict["cloud"] = None
            onboard_cmd_dict["dbr_version"] = None
        else:
            onboard_cmd_dict["cloud"] = self._wsi._choice(
                "Provide cloud provider name", ['aws', 'azure', 'gcp'])
            onboard_cmd_dict["dbr_version"] = self._wsi._question(
                "Provide databricks runtime version", default=self._ws.clusters.select_spark_version(latest=True))
        onboard_cmd_dict["onboarding_file_path"] = self._wsi._question(
            "Provide onboarding file path", default='demo/conf/onboarding.template')
        cwd = os.getcwd()
        onboarding_files_dir_path = self._wsi._question(
            "Provide onboarding files local directory", default=f'{cwd}/demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = f"file:/{onboarding_files_dir_path}"
        onboard_cmd_dict["dlt_meta_schema"] = self._wsi._question(
            "Provide dlt meta schema name", default=f'dlt_meta_dataflowspecs_{uuid.uuid4().hex}')
        onboard_cmd_dict["bronze_schema"] = self._wsi._question(
            "Provide dlt meta bronze layer schema name", default=f'dltmeta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = self._wsi._question(
            "Provide dlt meta silver layer schema name", default=f'dltmeta_silver_{uuid.uuid4().hex}')
        onboard_cmd_dict["onboard_layer"] = self._wsi._choice(
            "Provide dlt meta layer", ['bronze', 'silver', 'bronze_silver'])
        if onboard_cmd_dict["onboard_layer"] in ["bronze", "bronze_silver"]:
            onboard_cmd_dict["bronze_dataflowspec_table"] = self._wsi._question(
                "Provide bronze dataflow spec table name", default='bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflow spec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
        if onboard_cmd_dict["onboard_layer"] in ["silver", "bronze_silver"]:
            onboard_cmd_dict["silver_dataflowspec_table"] = self._wsi._question(
                "Provide silver dataflow spec table name", default='silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = self._wsi._question(
                    "Provide silver dataflow spec path", default=f'{self._install_folder()}/silver_dataflow_specs')
        onboard_cmd_dict["overwrite"] = self._wsi._choice(
            "Overwrite dataflow spec?", ['True', 'False'])
        onboard_cmd_dict["version"] = self._wsi._question(
            "Provide dataflow spec version", default='v1')
        onboard_cmd_dict["env"] = self._wsi._question(
            "Provide environment name", default='prod')
        onboard_cmd_dict["import_author"] = self._wsi._question(
            "Provide import author name", default=self._wsi._short_name)
        onboard_cmd_dict["update_paths"] = self._wsi._choice(
            "Update workspace/dbfs uc volume paths, unity catalog name, bronze/silver schema names in onboarding file?",
            ['True', 'False'])
        with open("onboarding_job_details.json", "w") as oc_file:
            json.dump(onboard_cmd_dict, oc_file, indent=4)
        cmd = OnboardCommand(**onboard_cmd_dict)

        return cmd

    def _load_deploy_config(self) -> DeployCommand:
        oc_job_details_json = None
        if os.path.isfile("onboarding_job_details.json"):
            with open("onboarding_job_details.json") as f:
                oc_job_details_json = f.read()
        load_from_ojd_json = False
        if oc_job_details_json:
            load_from_ojd_json_opt = self._wsi._choice(
                "onboarding_job_details.json Found! Do you want to use it for deployment?",
                ['Yes', 'No']
            )
            load_from_ojd_json = True if load_from_ojd_json_opt == "Yes" else False
        deploy_cmd_dict = {}
        if load_from_ojd_json:
            oc_job_details_json = json.loads(oc_job_details_json)
            deploy_cmd_dict["uc_enabled"] = self._wsi._choice(
                "Deploy DLT-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._wsi._question(
                    "Provide unity catalog name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy DLT-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide dlt meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] == "bronze" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dlt_meta_bronze_schema"] = oc_job_details_json["dlt_meta_schema"]
                    deploy_cmd_dict["dataflowspec_bronze_table"] = oc_job_details_json["bronze_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = oc_job_details_json["bronze_dataflowspec_path"]
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide dlt meta bronze onboard group")
            if deploy_cmd_dict["layer"] == "silver" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dlt_meta_silver_schema"] = oc_job_details_json["dlt_meta_schema"]
                    deploy_cmd_dict["dataflowspec_silver_table"] = oc_job_details_json["silver_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_silver_path"] = oc_job_details_json["silver_dataflowspec_path"]
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide dlt meta silver onboard group")
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        else:
            deploy_cmd_dict["uc_enabled"] = self._wsi._choice(
                "Deploy DLT-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._wsi._question(
                    "Provide unity catalog name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy DLT-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide dlt meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide dlt meta onboard bronze group")
                deploy_cmd_dict["dlt_meta_bronze_schema"] = self._wsi._question(
                    "Provide dlt_meta bronze dataflowspec schema name")
                deploy_cmd_dict["dataflowspec_bronze_table"] = self._wsi._question(
                    "Provide bronze dataflowspec table name", default='bronze_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = self._wsi._question(
                        "Provide bronze dataflowspec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide dlt meta silver onboard group")
                deploy_cmd_dict["dlt_meta_silver_schema"] = self._wsi._question(
                    "Provide dlt_meta silver dataflowspec schema name")
                deploy_cmd_dict["dataflowspec_silver_table"] = self._wsi._question(
                    "Provide silver dataflowspec table name", default='silver_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_path"] = self._wsi._question(
                        "Provide silver dataflowspec path",
                        default=f'{self._install_folder()}/silver_dataflow_specs')
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        layer = deploy_cmd_dict["layer"]
        deploy_cmd_dict["pipeline_name"] = self._wsi._question(
            "Provide dlt meta pipeline name", default=f"dlt_meta_{layer}_pipeline_{uuid.uuid4().hex}")
        deploy_cmd_dict["dlt_target_schema"] = self._wsi._question(
            "Provide dlt target schema name")
        return DeployCommand(**deploy_cmd_dict)

    def update_ws_onboarding_paths(self, cmd: OnboardCommand):
        """Create onboarding file for cloudfiles as source."""
        string_subs = {
            "{uc_volume_path}": f"{cmd.uc_volume_path}/dltmeta_conf/",
            "{uc_catalog_name}": cmd.uc_catalog_name,
            "{bronze_schema}": cmd.bronze_schema,
            "{silver_schema}": cmd.silver_schema,
        }
        with open(f"{cmd.onboarding_file_path}") as f:
            onboard_json = f.read()
            for key, val in string_subs.items():
                val = "" if val is None else val  # Ensure val is a string
                onboard_json = onboard_json.replace(key, val)
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        updated_ob_file_path = cmd.onboarding_file_path.replace(onboarding_filename, "onboarding.json")
        with open(updated_ob_file_path, "w") as onboarding_file:
            json.dump(json.loads(onboard_json), onboarding_file, indent=4)
        cmd.onboarding_file_path = updated_ob_file_path


def onboard(dltmeta: DLTMeta):
    logger.info("Please answer a couple of questions to for launching DLT META onboarding job")
    cmd = dltmeta._load_onboard_config()
    dltmeta.onboard(cmd)


def deploy(dltmeta: DLTMeta):
    logger.info("Please answer a couple of questions to for launching DLT META deployment job")
    cmd = dltmeta._load_deploy_config()
    dltmeta.deploy(cmd)


MAPPING = {
    "onboard": onboard,
    "deploy": deploy,
}


def main(raw):
    payload = json.loads(raw)
    command = payload["command"]
    if command not in MAPPING:
        msg = f"cannot find command: {command}"
        raise KeyError(msg)
    flags = payload["flags"]
    log_level = flags.pop("log_level")
    if log_level != "disabled":
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())
    version = __about__.__version__
    ws = WorkspaceClient(product='dlt-meta', product_version=version)
    dltmeta = DLTMeta(ws)
    MAPPING[command](dltmeta)


if __name__ == "__main__":
    main(*sys.argv[1:])
