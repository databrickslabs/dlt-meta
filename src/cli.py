"""Main entry point of the Python Wheel."""

import logging
import json
import glob
import os
from io import BytesIO
import subprocess
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

from databricks.sdk.service.catalog import VolumeType, SchemasAPI, VolumeInfo
logger = logging.getLogger("dlt-meta")
logger.setLevel(logging.INFO)


DLT_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install {remote_wheel}
# dbutils.library.restartPython()
# COMMAND ----------
layer = spark.conf.get("layer", None)
from src.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""


cloud_node_type_id_dict = {"aws": "i3.xlarge", "azure": "Standard_D3_v2", "gcp": "n1-highmem-4"}


@dataclass
class BaseCommand:
    dbfs_path: str
    uc_enabled: bool
    uc_catalog_name: str
    dlt_meta_schema: str
    cloud: str
    dbr_version: str


@dataclass
class OnboardCommand(BaseCommand):
    onboarding_file_path: str
    onboarding_files_dir_path: str
    onboard_layer: str
    bronze_dataflowspec_table: str
    bronze_dataflowspec_path: str
    silver_dataflowspec_table: str
    silver_dataflowspec_path: str
    overwrite: bool
    env: str
    import_author: str
    version: str


@dataclass
class DeployCommand(BaseCommand):
    serverless: bool
    num_workers: int
    layer: str
    onboard_group: str
    dataflowspec_table: str
    dataflowspec_path: str 
    pipeline_name: str 
    dlt_target_schema: str


def _my_username(ws: WorkspaceClient):
    if not hasattr(ws, "_me"):
        _me = ws.current_user.me()
    return _me.user_name


def onboard(cmd: OnboardCommand):
    ws = WorkspaceClient()
    logger.info("onboarding_files_dir: ", cmd.onboarding_files_dir_path)
    logger.info(f"uploading to {cmd.dbfs_path}")
    ws.dbfs.create(path=cmd.dbfs_path + "/dltmeta_conf/", overwrite=True)
    ws.dbfs.copy(cmd.onboarding_files_dir_path,
                 cmd.dbfs_path + "/dltmeta_conf/",
                 overwrite=True, recursive=True)
    logger.info(f"uploading to  {cmd.dbfs_path}/dltmeta_conf complete!!!")
    if cmd.uc_catalog_name:
        SchemasAPI(ws.api_client).create(catalog_name=cmd.uc_catalog_name,
                                         name=cmd.dlt_meta_schema,
                                         comment="dlt_meta framework schema")
        volume_info = ws.volumes.create(catalog_name=cmd.uc_catalog_name,
                                        schema_name=cmd.dlt_meta_schema,
                                        name=f"{cmd.uc_catalog_name}_volumne_dlt_meta",
                                        volume_type=VolumeType.MANAGED)
        SchemasAPI(ws.api_client).create(catalog_name=cmd.uc_catalog_name,
                                         name=cmd.bronze_schema,
                                         comment="bronze_schema")
        SchemasAPI(ws.api_client).create(catalog_name=cmd.uc_catalog_name,
                                         name=cmd.silver_schema,
                                         comment="silver_schema")
    whl_file_path = build_and_upload_package(cmd, ws, volume_info)
    created_job = create_onnboarding_job(cmd, ws, volume_info, whl_file_path)
    logger.info(f"Waiting for job to complete. job_id={created_job.job_id}")
    run_by_id = ws.jobs.run_now(job_id=created_job.job_id).result()
    logger.info(f"DLT-META Onboarding Job finished with run_id={run_by_id}. Please check onboarding table for details")


def create_onnboarding_job(cmd: OnboardCommand, ws: WorkspaceClient, volume_info: VolumeInfo, whl_file_path):
    cluster_spec = compute.ClusterSpec(
        cluster_name="dlt_meta_onboarding_cluster",
        spark_version=cmd.dbr_version,
        num_workers=1,
        driver_node_type_id=cloud_node_type_id_dict[cmd.cloud],
        node_type_id=cloud_node_type_id_dict[cmd.cloud],
        data_security_mode=compute.DataSecurityMode.SINGLE_USER
        if cmd.uc_enabled else compute.DataSecurityMode.LEGACY_SINGLE_USER,
        autotermination_minutes=30,
        spark_conf={},
        spark_env_vars={
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        }
    )
    onboarding_filename = os.path.basename(cmd.onboarding_file_path)
    return ws.jobs.create(
        name="dlt_meta_onboarding_job",
        tasks=[
            jobs.Task(
                task_key="dlt_meta_omnbarding_task",
                description="test",
                new_cluster=cluster_spec,
                timeout_seconds=0,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name="dlt_meta",
                    entry_point="run",
                    named_parameters={
                                "onboard_layer": "bronze_silver",
                                "database":
                                    cmd.uc_catalog_name.dlt_meta_schema if cmd.uc_catalog_name else cmd.dlt_meta_schema,
                                "onboarding_file_path":
                                f"{cmd.dbfs_path}/dltmeta_conf{onboarding_filename}",
                                "bronze_dataflowspec_table": cmd.bronze_dataflowspec_table,
                                "bronze_dataflowspec_path": cmd.bronze_dataflowspec_path,
                                "silver_dataflowspec_table": cmd.silver_dataflowspec_table,
                                "silver_dataflowspec_path": cmd.silver_dataflowspec_path,
                                "import_author": cmd.import_author,
                                "version": cmd.version,
                                "overwrite": cmd.overwrite,
                                "env": cmd.env,
                                "uc_enabled": "True" if cmd.uc_enabled else "False"
                    },
                ),
                libraries=[jobs.compute.Library(whl=whl_file_path)]
            ),
        ]
    )


def _install_folder(ws: WorkspaceClient):
    return f"/Users/{_my_username(ws)}/dlt-meta"


def create_dlt_meta_pipeline(cmd: DeployCommand, ws: WorkspaceClient, whl_file_path: str):
    runner_notebook_py = DLT_META_RUNNER_NOTEBOOK.format(remote_wheel=whl_file_path).encode("utf8")
    runner_notebook_path = f"{_install_folder(ws)}/init_dlt_meta_pipeline.py"
    ws.workspace.upload(runner_notebook_path, runner_notebook_py, overwrite=True)
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
        created = ws.pipelines.create(catalog=ws.uc_catalog_name,
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
    else:
        file_dbfs_path = f"/{whl_file_path}".replace(":", "")
        configuration["dlt_meta_whl"] = file_dbfs_path
        configuration[f"{cmd.layer}.dataflowspecTable"] = (
            f"{cmd.dlt_meta_schema}.{cmd.dataflowspec_table}"
        )
        created = ws.pipelines.create(
            name=cmd.pipeline_name,
            # serverless=True,
            channel="PREVIEW",
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(
                        path=f"{cmd.runners_nb_path}/runners/init_dlt_meta_pipeline"
                        # Todo: change this path to the actual path
                    )
                )
            ],
            target=cmd.dlt_target_schema,
            clusters=[pipelines.PipelineCluster(label="default", num_workers=4)]

        )
    if created is None:
        raise Exception("Pipeline creation failed")
    return created.pipeline_id


def build_and_upload_package(cmd: BaseCommand, ws: WorkspaceClient, volume_info: VolumeInfo):
    child = subprocess.Popen(
        ["pip3", "wheel", "-w", "dist", ".", "--no-deps"]
    )
    exit_code = child.wait()
    if exit_code != 0:
        raise Exception("Non-zero exitcode: %s" % (exit_code))
    else:
        whl_path = glob.glob("dist/*.whl")[0]
        whl_fp = open(whl_path, "rb")
        whl_name = os.path.basename(whl_path)
        if cmd.uc_catalog_name:
            uc_target_whl_path = (
                f"/Volumes/{volume_info.catalog_name}/"
                f"{volume_info.schema_name}/{volume_info.name}/dltmeta_whl/{whl_name}"
            )
            ws.files.upload(
                file_path=uc_target_whl_path,
                contents=BytesIO(whl_fp.read()),
                overwrite=True
            )
            return uc_target_whl_path
        else:
            dbfs_whl_path = f"{cmd.dbfs_path}/dltmeta_whl/{whl_name}"
            ws.dbfs.upload(dbfs_whl_path, BytesIO(whl_fp.read()), overwrite=True)
            return dbfs_whl_path


def deploy(cmd: DeployCommand):
    ws = WorkspaceClient()
    whll_file_path = build_and_upload_package(cmd, ws)
    pipeline_id = create_dlt_meta_pipeline(cmd, ws, whll_file_path)
    update_response = ws.pipelines.start_update(pipeline_id=pipeline_id)
    logger.info(f"dlt-meta pipeline={pipeline_id} created and launched with update_id={update_response.update_id}")
    logger.info("Please check the pipeline status in databricks workspace workflows-> Delta Live Tables tab")


MAPPING = {
    "onboard": onboard,
    "deloy": deploy,
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

    kwargs = {k.replace("-", "_"): v for k, v in flags.items()}
    if command == "onboard":
        cmd = OnboardCommand(**kwargs)
    elif command == "deploy":
        cmd = DeployCommand(**kwargs)
    else:
        raise ValueError(f"Invalid command: {command}")

    MAPPING[command](cmd)


if __name__ == "__main__":
    main()
