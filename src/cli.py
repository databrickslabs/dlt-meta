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
from databricks.sdk.service.catalog import SchemasAPI
from src import __about__
from src.install import WorkspaceInstaller

logger = logging.getLogger('databricks.labs.dltmeta')


DLT_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install /Workspace{remote_wheel}
# dbutils.library.restartPython()
# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""

cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


@dataclass
class OnboardCommand:
    """Class representing the onboarding command."""
    dbr_version: str
    dbfs_path: str
    onboarding_file_path: str
    onboarding_files_dir_path: str
    onboard_layer: str
    env: str
    import_author: str
    version: str
    cloud: str
    dlt_meta_schema: str
    bronze_schema: str = None
    silver_schema: str = None
    uc_enabled: bool = False
    uc_catalog_name: str = None
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
        if self.uc_enabled == "":
            raise ValueError("uc_enabled is required, please set to True or False")
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
        # if not self.uc_enabled and not self.uc_catalog_name:
        #     raise ValueError("uc_catalog_name is required")
        if not self.dlt_meta_schema:
            raise ValueError("dlt_meta_schema is required")
        if not self.cloud:
            raise ValueError("cloud is required")
        if not self.dbfs_path:
            raise ValueError("dbfs_path is required")
        if not self.dbr_version:
            raise ValueError("dbr_version is required")
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
    onboard_group: str
    dlt_meta_schema: str
    dataflowspec_table: str
    pipeline_name: str
    dlt_target_schema: str
    num_workers: int = None
    uc_catalog_name: str = None
    dataflowspec_path: str = None
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
        if not self.onboard_group:
            raise ValueError("onboard_group is required")
        if not self.dataflowspec_table:
            raise ValueError("dataflowspec_table is required")
        if not self.uc_enabled and not self.dataflowspec_path:
            raise ValueError("dataflowspec_path is required")
        if not self.pipeline_name:
            raise ValueError("pipeline_name is required")
        if not self.dlt_target_schema:
            raise ValueError("dlt_target_schema is required")


class DLTMeta:
    """Class representing the DLT-META CLI."""

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
        self._wsi = WorkspaceInstaller(ws)

    def _my_username(self):
        if not hasattr(self._ws, "_me"):
            _me = self._ws.current_user.me()
        return _me.user_name

    def onboard(self, cmd: OnboardCommand):
        """Perform the onboarding process."""
        self.update_ws_onboarding_paths(cmd)
        if not self._ws.dbfs.exists(cmd.dbfs_path + "/dltmeta_conf/"):
            self._ws.dbfs.create(path=cmd.dbfs_path + "/dltmeta_conf/", overwrite=True)
        ob_file = open(cmd.onboarding_file_path, "rb")
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        self._ws.dbfs.upload(cmd.dbfs_path + f"/dltmeta_conf/{onboarding_filename}", ob_file, overwrite=True)
        self._ws.dbfs.copy(cmd.onboarding_files_dir_path,
                           cmd.dbfs_path + "/dltmeta_conf/",
                           overwrite=True, recursive=True)
        logger.info(f"uploading to  {cmd.dbfs_path}/dltmeta_conf complete!!!")
        if cmd.uc_enabled:
            try:
                SchemasAPI(self._ws.api_client).get(full_name=f"{cmd.uc_catalog_name}.{cmd.dlt_meta_schema}")
            except Exception:
                msg = (
                    "Schema {catalog}.{schema} not found. "
                    "Creating schema={schema}"
                ).format(catalog=cmd.uc_catalog_name, schema=cmd.dlt_meta_schema)
                logger.info(msg)
                SchemasAPI(self._ws.api_client).create(
                    catalog_name=cmd.uc_catalog_name,
                    name=cmd.dlt_meta_schema,
                    comment="dlt_meta framework schema"
                )
        created_job = self.create_onnboarding_job(cmd)
        logger.info(f"Waiting for job to complete. job_id={created_job.job_id}")
        run = self._ws.jobs.run_now(job_id=created_job.job_id)
        msg = (
            "DLT-META Onboarding Job(job_id={}) "
            "launched with run_id={}, Please check the job status in databricks workspace jobs tab"
        ).format(created_job.job_id, run.run_id)
        logger.info(msg)
        webbrowser.open(f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}")

    def create_onnboarding_job(self, cmd: OnboardCommand):
        """Create the onboarding job."""
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
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        remote_wheel = (
            f"/Workspace{self._wsi._upload_wheel()}"
            if cmd.uc_enabled
            else f"dbfs:{self._wsi._upload_wheel()}"
        )

        return self._ws.jobs.create(
            name="dlt_meta_onboarding_job",
            tasks=[
                jobs.Task(
                    task_key="dlt_meta_onbarding_task",
                    description="test",
                    new_cluster=cluster_spec,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="dlt_meta",
                        entry_point="run",
                        named_parameters={
                                    "onboard_layer": cmd.onboard_layer,
                                    "database":
                                        f"{cmd.uc_catalog_name}.{cmd.dlt_meta_schema}"
                                        if cmd.uc_enabled else cmd.dlt_meta_schema,
                                    "onboarding_file_path":
                                    f"{cmd.dbfs_path}/dltmeta_conf/{onboarding_filename}",
                                    "bronze_dataflowspec_table": cmd.bronze_dataflowspec_table,
                                    "bronze_dataflowspec_path": cmd.bronze_dataflowspec_path,
                                    "silver_dataflowspec_table": cmd.silver_dataflowspec_table,
                                    "silver_dataflowspec_path": cmd.silver_dataflowspec_path,
                                    "import_author": cmd.import_author,
                                    "version": cmd.version,
                                    "overwrite": "True" if cmd.overwrite else "False",
                                    "env": cmd.env,
                                    "uc_enabled": "True" if cmd.uc_enabled else "False"
                        },
                    ),
                    libraries=[jobs.compute.Library(whl=remote_wheel)]
                ),
            ]
        )

    def _install_folder(self):
        return f"/Users/{self._my_username()}/dlt-meta"

    def _create_dlt_meta_pipeline(self, cmd: DeployCommand):
        """Create the DLT-META pipeline."""
        whl_file_path = f"/Workspace{self._wsi._upload_wheel()}"
        runner_notebook_py = DLT_META_RUNNER_NOTEBOOK.format(remote_wheel=self._wsi._upload_wheel()).encode("utf8")
        runner_notebook_path = f"{self._install_folder()}/init_dlt_meta_pipeline.py"
        try:
            self._ws.workspace.mkdirs(self._install_folder())
        except DatabricksError as e:
            logger.error(e)
        self._ws.workspace.upload(runner_notebook_path, runner_notebook_py, overwrite=True)
        configuration = {
            "layer": cmd.layer,
            f"{cmd.layer}.group": cmd.onboard_group,
        }
        created = None
        if cmd.uc_catalog_name:
            configuration["dlt_meta_whl"] = whl_file_path
            configuration[f"{cmd.layer}.dataflowspecTable"] = (
                f"{cmd.uc_catalog_name}.{cmd.dlt_meta_schema}.{cmd.dataflowspec_table}"
            )
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
                                                target=cmd.dlt_target_schema,
                                                clusters=[pipelines.PipelineCluster(label="default",
                                                                                    num_workers=cmd.num_workers)]
                                                if not cmd.serverless else None,
                                                serverless=cmd.serverless if cmd.uc_enabled else None,
                                                channel="PREVIEW" if cmd.serverless else None
                                                )
        else:
            configuration["dlt_meta_whl"] = whl_file_path
            configuration[f"{cmd.layer}.dataflowspecTable"] = (
                f"{cmd.dlt_meta_schema}.{cmd.dataflowspec_table}"
            )
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
        onboard_cmd_dict["onboarding_file_path"] = self._wsi._question(
            "Provide onboarding file path", default='demo/conf/onboarding.template')
        onboarding_files_dir_path = self._wsi._question(
            "Provide onboarding files local directory", default='demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = f"file:./{onboarding_files_dir_path}"
        onboard_cmd_dict["dbfs_path"] = self._wsi._question(
            "Provide dbfs path", default="dbfs:/dlt-meta_cli_demo")
        onboard_cmd_dict["dbr_version"] = self._wsi._question(
            "Provide databricks runtime version", default=self._ws.clusters.select_spark_version(latest=True))
        onboard_cmd_dict["uc_enabled"] = self._wsi._choice(
            "Run onboarding with unity catalog enabled?", ['True', 'False'])
        onboard_cmd_dict["uc_enabled"] = True if onboard_cmd_dict["uc_enabled"] == 'True' else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["uc_catalog_name"] = self._wsi._question(
                "Provide unity catalog name")
        onboard_cmd_dict["dlt_meta_schema"] = self._wsi._question(
            "Provide dlt meta schema name", default=f'dlt_meta_dataflowspecs_{uuid.uuid4().hex}')
        onboard_cmd_dict["bronze_schema"] = self._wsi._question(
            "Provide dlt meta bronze layer schema name", default=f'dltmeta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = self._wsi._question(
            "Provide dlt meta silver layer schema name", default=f'dltmeta_silver_{uuid.uuid4().hex}')
        onboard_cmd_dict["onboard_layer"] = self._wsi._choice(
            "Provide dlt meta layer", ['bronze', 'silver', 'bronze_silver'])
        if onboard_cmd_dict["onboard_layer"] == "bronze":
            onboard_cmd_dict["bronze_dataflowspec_table"] = self._wsi._question(
                "Provide bronze dataflow spec table name", default='bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflow spec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
        if onboard_cmd_dict["onboard_layer"] == "silver":
            onboard_cmd_dict["silver_dataflowspec_table"] = self._wsi._question(
                "Provide silver dataflow spec table name", default='silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = self._wsi._question(
                    "Provide silver dataflow spec path", default=f'{self._install_folder()}/silver_dataflow_specs')
        if onboard_cmd_dict["onboard_layer"] == "bronze_silver":
            onboard_cmd_dict["bronze_dataflowspec_table"] = self._wsi._question(
                "Provide bronze dataflow spec table name", default='bronze_dataflowspec')
            onboard_cmd_dict["silver_dataflowspec_table"] = self._wsi._question(
                "Provide silver dataflow spec table name", default='silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflow spec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
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
        onboard_cmd_dict["cloud"] = self._wsi._choice(
            "Provide cloud provider name", ['aws', 'azure', 'gcp'])
        onboard_cmd_dict["update_paths"] = self._wsi._choice(
            "Update workspace/dbfs paths, unity catalog name, bronze/silver schema names in onboarding file?",
            ['True', 'False'])
        cmd = OnboardCommand(**onboard_cmd_dict)

        return cmd

    def _load_deploy_config(self) -> DeployCommand:
        deploy_cmd_dict = {}
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
            "Provide dlt meta layer", ['bronze', 'silver'])
        if deploy_cmd_dict["layer"] == "bronze":
            deploy_cmd_dict["onboard_group"] = self._wsi._question(
                "Provide dlt meta onboard group")
            deploy_cmd_dict["dlt_meta_schema"] = self._wsi._question(
                "Provide dlt_meta dataflowspec schema name")
            deploy_cmd_dict["dataflowspec_table"] = self._wsi._question(
                "Provide bronze dataflowspec table name", default='bronze_dataflowspec')
            if not deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflowspec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
        if deploy_cmd_dict["layer"] == "silver":
            deploy_cmd_dict["onboard_group"] = self._wsi._question(
                "Provide dlt meta onboard group")
            deploy_cmd_dict["dlt_meta_schema"] = self._wsi._question(
                "Provide dlt_meta dataflowspec schema name")
            deploy_cmd_dict["dataflowspec_table"] = self._wsi._question(
                "Provide silver dataflowspec table name", default='silver_dataflowspec')
            if not deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["dataflowspec_path"] = self._wsi._question(
                    "Provide silver dataflowspec path", default=f'{self._install_folder()}/silver_dataflow_specs')
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
        with open(f"{cmd.onboarding_file_path}") as f:
            onboard_obj = json.load(f)

        for data_flow in onboard_obj:
            for key, value in data_flow.items():
                if key == "source_details":
                    for source_key, source_value in value.items():
                        if 'dbfs_path' in source_value:
                            data_flow[key][source_key] = source_value.format(dbfs_path=f"{cmd.dbfs_path}/dltmeta_conf/")
                if 'dbfs_path' in value:
                    data_flow[key] = value.format(dbfs_path=f"{cmd.dbfs_path}/dltmeta_conf/")
                elif 'uc_catalog_name' in value and 'bronze_schema' in value:
                    if cmd.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=cmd.uc_catalog_name,
                            bronze_schema=cmd.bronze_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=cmd.bronze_schema,
                            bronze_schema=""
                        ).replace(".", "")

                elif 'uc_catalog_name' in value and 'silver_schema' in value:
                    if cmd.uc_catalog_name:
                        data_flow[key] = value.format(
                            uc_catalog_name=cmd.uc_catalog_name,
                            silver_schema=cmd.silver_schema
                        )
                    else:
                        data_flow[key] = value.format(
                            uc_catalog_name=cmd.silver_schema,
                            silver_schema=""
                        ).replace(".", "")
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        updated_ob_file_path = cmd.onboarding_file_path.replace(onboarding_filename, "onboarding.json")
        with open(updated_ob_file_path, "w") as onboarding_file:
            json.dump(onboard_obj, onboarding_file)
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
