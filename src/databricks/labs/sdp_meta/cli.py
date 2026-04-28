"""Main entry point for the CLI."""

import logging
import json
import os
import sys
import uuid
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, pipelines, compute
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.catalog import SchemasAPI, VolumeType
from databricks.labs.sdp_meta import __about__
from databricks.labs.sdp_meta.identifiers import (
    prompt_uc_identifier,
    validate_uc_identifier,
)
from databricks.labs.sdp_meta.install import WorkspaceInstaller

logger = logging.getLogger('databricks.labs.sdp_meta')


# Runner notebook template for DLT pipeline
SDP_META_RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install databricks-labs-sdp-meta=={version}
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
layer = spark.conf.get("layer", None)
from databricks.labs.sdp_meta.dataflow_pipeline import DataflowPipeline
DataflowPipeline.invoke_dlt_pipeline(spark, layer)
"""

# Backwards compatibility alias (deprecated)
DLT_META_RUNNER_NOTEBOOK = SDP_META_RUNNER_NOTEBOOK

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
    sdp_meta_schema: str
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
        if not self.sdp_meta_schema:
            raise ValueError("sdp_meta_schema is required")
        if not self.import_author:
            raise ValueError("import_author is required")
        if not self.version:
            raise ValueError("version is required")
        if not self.env:
            raise ValueError("env is required")
        # Validate UC identifiers up front (issue #261). Even when UC is
        # disabled the *_schema and *_dataflowspec_table values are spliced
        # unquoted into the onboarding template (see
        # ``SDPMeta.update_ws_onboarding_paths``: the ``{bronze_schema}`` /
        # ``{silver_schema}`` placeholders land inside identifier positions
        # like ``bronze_database_<env>`` and from there into the deployed
        # pipeline's SQL), so they have to be valid regardless of
        # ``uc_enabled``. Only ``uc_catalog_name`` is gated because the
        # non-UC code path never references it.
        validate_uc_identifier(self.sdp_meta_schema, kind="sdp_meta_schema")
        if self.bronze_dataflowspec_table:
            validate_uc_identifier(
                self.bronze_dataflowspec_table, kind="bronze_dataflowspec_table"
            )
        if self.silver_dataflowspec_table:
            validate_uc_identifier(
                self.silver_dataflowspec_table, kind="silver_dataflowspec_table"
            )
        if self.bronze_schema:
            validate_uc_identifier(self.bronze_schema, kind="bronze_schema")
        if self.silver_schema:
            validate_uc_identifier(self.silver_schema, kind="silver_schema")
        if self.uc_enabled:
            validate_uc_identifier(self.uc_catalog_name, kind="uc_catalog_name")


@dataclass
class DeployCommand:
    """Class representing the deploy command."""
    layer: str
    pipeline_name: str
    dlt_target_schema: str
    onboard_bronze_group: str = None
    onboard_silver_group: str = None
    sdp_meta_bronze_schema: str = None
    sdp_meta_silver_schema: str = None
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
        # Issue #261: catalog / schema / table names are spliced into SQL
        # identifiers downstream (e.g. `spark.read.table(<dataflowspecTable>)`),
        # so reject anything that can't be safely emitted before the pipeline
        # is even created.
        validate_uc_identifier(self.dlt_target_schema, kind="dlt_target_schema")
        if self.uc_enabled:
            validate_uc_identifier(self.uc_catalog_name, kind="uc_catalog_name")
        if self.sdp_meta_bronze_schema:
            validate_uc_identifier(
                self.sdp_meta_bronze_schema, kind="sdp_meta_bronze_schema"
            )
        if self.sdp_meta_silver_schema:
            validate_uc_identifier(
                self.sdp_meta_silver_schema, kind="sdp_meta_silver_schema"
            )
        if self.dataflowspec_bronze_table:
            validate_uc_identifier(
                self.dataflowspec_bronze_table, kind="dataflowspec_bronze_table"
            )
        if self.dataflowspec_silver_table:
            validate_uc_identifier(
                self.dataflowspec_silver_table, kind="dataflowspec_silver_table"
            )


class SDPMeta:
    """Class representing the SDP-META CLI."""

    def __init__(self, ws: WorkspaceClient):
        self._ws = ws
        self._wsi = WorkspaceInstaller(ws)
        self.version = __about__.__version__

    def _ident_question(
        self,
        text: str,
        kind: str,
        *,
        default: str = None,
        max_attempts: int = 10,
    ) -> str:
        """Prompt for a UC identifier with validation + re-prompt on bad input.

        Thin wrapper over
        :func:`databricks.labs.sdp_meta.identifiers.prompt_uc_identifier`
        that supplies this CLI's :class:`WorkspaceInstaller` (``_wsi``)
        as the prompter. Kept so existing call sites remain readable
        (``self._ident_question(text, kind=...)``) while sharing the
        retry / TTY-aware error-print logic with ``bundle.py``
        (issue #261).
        """
        return prompt_uc_identifier(
            self._wsi,
            text,
            kind=kind,
            default=default,
            max_attempts=max_attempts,
        )

    @staticmethod
    def _get_schema_from_json(oc_json: dict) -> str:
        """Read the schema key from onboarding_job_details.json with backward compatibility.

        Supports both the new key ``sdp_meta_schema`` and the legacy key
        ``dlt_meta_schema`` so that JSON files produced by either version
        of the CLI are accepted.

        Args:
            oc_json: Parsed onboarding job details JSON dict.

        Returns:
            The schema name string.

        Raises:
            KeyError: If neither key is found in the JSON.
        """
        if "sdp_meta_schema" in oc_json:
            return oc_json["sdp_meta_schema"]
        if "dlt_meta_schema" in oc_json:
            logger.warning(
                "Found legacy key 'dlt_meta_schema' in onboarding_job_details.json. "
                "Please re-run onboarding with SDP-META to update the file."
            )
            return oc_json["dlt_meta_schema"]
        raise KeyError(
            "Neither 'sdp_meta_schema' nor 'dlt_meta_schema' found in "
            "onboarding_job_details.json. Please re-run the onboarding step."
        )

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

    def create_uc_volume(self, uc_catalog_name, sdp_meta_schema):
        try:
            self._ws.volumes.create(
                catalog_name=uc_catalog_name,
                schema_name=sdp_meta_schema,
                name=sdp_meta_schema,
                volume_type=VolumeType.MANAGED,
            )
        except Exception:
            logger.info(f"Volume {sdp_meta_schema} already exists")
        return f"/Volumes/{uc_catalog_name}/{sdp_meta_schema}/{sdp_meta_schema}/"

    def onboard(self, cmd: OnboardCommand):
        """launch the onboarding job."""
        onboarding_filename = os.path.basename(cmd.onboarding_file_path)
        ob_file = open(cmd.onboarding_file_path, "rb")

        if cmd.uc_enabled:
            self.create_uc_schema(cmd.uc_catalog_name, cmd.sdp_meta_schema)
            cmd.uc_volume_path = self.create_uc_volume(cmd.uc_catalog_name, cmd.sdp_meta_schema)
            self.update_ws_onboarding_paths(cmd)
            self.copy_to_uc_volume(cmd.onboarding_files_dir_path, cmd.uc_volume_path + "/sdp_meta_conf/")
            logger.info(f"uploading to  {cmd.uc_volume_path}/sdp_meta_conf complete!!!")
        else:
            self._ws.dbfs.mkdirs(f"{cmd.dbfs_path}/sdp_meta_conf/")
            self._ws.dbfs.upload(f"{cmd.dbfs_path}/sdp_meta_conf/{onboarding_filename}", ob_file, overwrite=True)
            self.update_ws_onboarding_paths(cmd)
            onboarding_filename = os.path.basename(cmd.onboarding_file_path)
            self.copy_to_dbfs(cmd.onboarding_files_dir_path, cmd.dbfs_path + "/sdp_meta_conf/")
            logger.info(f"uploading to  {cmd.dbfs_path}/sdp_meta_conf complete!!!")
        created_job = self.create_onnboarding_job(cmd)
        logger.info(f"Waiting for job to complete. job_id={created_job.job_id}")
        run = self._ws.jobs.run_now(job_id=created_job.job_id)
        msg = (
            "SDP-META Onboarding Job(job_id={}) "
            "launched with run_id={}, Please check the job status in databricks workspace jobs tab"
        ).format(created_job.job_id, run.run_id)
        logger.info(msg)
        job_url = f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}"
        print(
            f"Job created successfully. job_id={created_job.job_id}, url={job_url}"
        )
        webbrowser.open(f"{self._ws.config.host}/jobs/{created_job.job_id}?o={self._ws.get_workspace_id()}")

    def create_uc_schema(self, uc_catalog_name, sdp_meta_schema):
        try:
            SchemasAPI(self._ws.api_client).get(full_name=f"{uc_catalog_name}.{sdp_meta_schema}")
        except Exception:
            msg = (
                "Schema {catalog}.{schema} not found. "
                "Creating schema={schema}"
            ).format(catalog=uc_catalog_name, schema=sdp_meta_schema)
            logger.info(msg)
            SchemasAPI(self._ws.api_client).create(
                catalog_name=uc_catalog_name,
                name=sdp_meta_schema,
                comment="sdp_meta framework schema"
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
        sdp_meta_environments = [
            jobs.JobEnvironment(
                environment_key="sdp_meta_cli_env",
                spec=compute.Environment(client="1",
                                         dependencies=[f"sdp-meta=={self.version}"]
                                         )
            )
        ]
        return self._ws.jobs.create(
            name="sdp_meta_onboarding_job",
            environments=None if not cmd.serverless else sdp_meta_environments,
            tasks=[
                jobs.Task(
                    task_key="sdp_meta_onbarding_task",
                    description="test",
                    new_cluster=cluster_spec if not cmd.serverless else None,
                    environment_key="sdp_meta_cli_env" if cmd.serverless else None,
                    timeout_seconds=0,
                    python_wheel_task=jobs.PythonWheelTask(
                        package_name="databricks_labs_sdp_meta",
                        entry_point="run",
                        named_parameters=named_parameters,
                    ),
                    libraries=[
                        jobs.compute.Library(
                            pypi=compute.PythonPyPiLibrary(package=f"sdp-meta=={self.version}")
                        )
                    ] if not cmd.serverless else None,
                ),
            ]
        )

    def _get_onboarding_named_parameters(self, cmd: OnboardCommand):
        named_parameters = {
            "onboard_layer": cmd.onboard_layer,
            "database":
                f"{cmd.uc_catalog_name}.{cmd.sdp_meta_schema}"
                if cmd.uc_enabled else cmd.sdp_meta_schema,
            "import_author": cmd.import_author,
            "version": cmd.version,
            "overwrite": "True" if cmd.overwrite else "False",
            "env": cmd.env,
            "uc_enabled": "True" if cmd.uc_enabled else "False"
        }
        if cmd.uc_enabled:
            named_parameters["onboarding_file_path"] = f"{cmd.uc_volume_path}/sdp_meta_conf/{cmd.onboarding_file_path}"
        else:
            named_parameters["onboarding_file_path"] = f"{cmd.dbfs_path}/sdp_meta_conf/{cmd.onboarding_file_path}"
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
        return f"/Users/{self._my_username()}/sdp-meta"

    def _create_sdp_meta_pipeline(self, cmd: DeployCommand):
        """Create the SDP-META pipeline."""
        runner_notebook_py = SDP_META_RUNNER_NOTEBOOK.format(version=self.version).encode("utf8")
        runner_notebook_path = f"{self._install_folder()}/init_sdp_meta_pipeline.py"
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
                        f"{cmd.uc_catalog_name}.{cmd.sdp_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
                else:
                    configuration["bronze.dataflowspecTable"] = (
                        f"{cmd.sdp_meta_bronze_schema}.{cmd.dataflowspec_bronze_table}"
                    )
            if cmd.layer in ["silver", "bronze_silver"]:
                configuration["silver.group"] = cmd.onboard_silver_group
                if cmd.uc_catalog_name:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.uc_catalog_name}.{cmd.sdp_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
                    )
                else:
                    configuration["silver.dataflowspecTable"] = (
                        f"{cmd.sdp_meta_silver_schema}.{cmd.dataflowspec_silver_table}"
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
                                                schema=cmd.dlt_target_schema,
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
        pipeline_id = self._create_sdp_meta_pipeline(cmd)
        update_response = self._ws.pipelines.start_update(pipeline_id=pipeline_id)
        msg = (
            f"sdp-meta pipeline={pipeline_id} created and launched with "
            f"update_id={update_response.update_id}, Please check the pipeline status in "
            "databricks workspace under workflows -> Lakeflow Spark Declarative Pipelines tab"
        )
        logger.info(msg)
        print(
            f"sdp-meta pipeline={pipeline_id} created and launched with update_id={update_response.update_id}, "
            f"url={self._ws.config.host}/#joblist/pipelines/{pipeline_id}?o={self._ws.get_workspace_id()}/"
        )
        webbrowser.open(f"{self._ws.config.host}/#joblist/pipelines/{pipeline_id}?o={self._ws.get_workspace_id()}/")

    def _load_onboard_config(self) -> OnboardCommand:
        onboard_cmd_dict = {}
        onboard_cmd_dict["uc_enabled"] = self._wsi._choice(
            "Run onboarding with unity catalog enabled?", ['True', 'False'])
        onboard_cmd_dict["uc_enabled"] = True if onboard_cmd_dict["uc_enabled"] == "True" else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["dbfs_path"] = None
            onboard_cmd_dict["uc_catalog_name"] = self._ident_question(
                "Provide unity catalog name", kind="uc_catalog_name")
        else:
            onboard_cmd_dict["dbfs_path"] = self._wsi._question(
                "Provide dbfs path", default=f"dbfs:/sdp-meta_cli_demo_{uuid.uuid4().hex}")
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
            "Provide onboarding file path", default='demo/conf/json/onboarding.template')
        cwd = os.getcwd()
        onboarding_files_dir_path = self._wsi._question(
            "Provide onboarding files local directory", default=f'{cwd}/demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = f"file:/{onboarding_files_dir_path}"
        onboard_cmd_dict["sdp_meta_schema"] = self._ident_question(
            "Provide sdp meta schema name",
            kind="sdp_meta_schema",
            default=f'sdp_meta_dataflowspecs_{uuid.uuid4().hex}')
        onboard_cmd_dict["bronze_schema"] = self._ident_question(
            "Provide sdp meta bronze layer schema name",
            kind="bronze_schema",
            default=f'sdp_meta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = self._ident_question(
            "Provide sdp meta silver layer schema name",
            kind="silver_schema",
            default=f'sdp_meta_silver_{uuid.uuid4().hex}')
        onboard_cmd_dict["onboard_layer"] = self._wsi._choice(
            "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
        if onboard_cmd_dict["onboard_layer"] in ["bronze", "bronze_silver"]:
            onboard_cmd_dict["bronze_dataflowspec_table"] = self._ident_question(
                "Provide bronze dataflow spec table name",
                kind="bronze_dataflowspec_table",
                default='bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = self._wsi._question(
                    "Provide bronze dataflow spec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
        if onboard_cmd_dict["onboard_layer"] in ["silver", "bronze_silver"]:
            onboard_cmd_dict["silver_dataflowspec_table"] = self._ident_question(
                "Provide silver dataflow spec table name",
                kind="silver_dataflowspec_table",
                default='silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = self._wsi._question(
                    "Provide silver dataflow spec path", default=f'{self._install_folder()}/silver_dataflow_specs')
        onboard_cmd_dict["overwrite"] = self._wsi._choice(
            "Overwrite dataflow spec?", ['True', 'False'])
        onboard_cmd_dict["overwrite"] = True if onboard_cmd_dict["overwrite"] == 'True' else False
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
                "Deploy SDP-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._ident_question(
                    "Provide unity catalog name", kind="uc_catalog_name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy SDP-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] == "bronze" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_bronze_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_bronze_table"] = oc_job_details_json["bronze_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = oc_job_details_json["bronze_dataflowspec_path"]
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide sdp meta bronze onboard group")
            if deploy_cmd_dict["layer"] == "silver" or deploy_cmd_dict["layer"] == "bronze_silver":
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_silver_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_silver_table"] = oc_job_details_json["silver_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_silver_path"] = oc_job_details_json["silver_dataflowspec_path"]
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide sdp meta silver onboard group")
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        else:
            deploy_cmd_dict["uc_enabled"] = self._wsi._choice(
                "Deploy SDP-META with unity catalog enabled?", ["True", "False"])
            deploy_cmd_dict["uc_enabled"] = True if deploy_cmd_dict["uc_enabled"] == "True" else False
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = self._ident_question(
                    "Provide unity catalog name", kind="uc_catalog_name")
                deploy_cmd_dict["serverless"] = self._wsi._choice(
                    "Deploy SDP-META with serverless?", ["True", "False"])
                deploy_cmd_dict["serverless"] = True if deploy_cmd_dict["serverless"] == "True" else False
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = self._wsi._choice(
                "Provide sdp meta layer", ['bronze', 'silver', 'bronze_silver'])
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                deploy_cmd_dict["onboard_bronze_group"] = self._wsi._question(
                    "Provide sdp meta onboard bronze group")
                deploy_cmd_dict["sdp_meta_bronze_schema"] = self._ident_question(
                    "Provide sdp_meta bronze dataflowspec schema name",
                    kind="sdp_meta_bronze_schema")
                deploy_cmd_dict["dataflowspec_bronze_table"] = self._ident_question(
                    "Provide bronze dataflowspec table name",
                    kind="dataflowspec_bronze_table",
                    default='bronze_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = self._wsi._question(
                        "Provide bronze dataflowspec path", default=f'{self._install_folder()}/bronze_dataflow_specs')
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                deploy_cmd_dict["onboard_silver_group"] = self._wsi._question(
                    "Provide sdp meta silver onboard group")
                deploy_cmd_dict["sdp_meta_silver_schema"] = self._ident_question(
                    "Provide sdp_meta silver dataflowspec schema name",
                    kind="sdp_meta_silver_schema")
                deploy_cmd_dict["dataflowspec_silver_table"] = self._ident_question(
                    "Provide silver dataflowspec table name",
                    kind="dataflowspec_silver_table",
                    default='silver_dataflowspec')
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_path"] = self._wsi._question(
                        "Provide silver dataflowspec path",
                        default=f'{self._install_folder()}/silver_dataflow_specs')
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = int(self._wsi._question(
                    "Provide number of workers", default=4))
        layer = deploy_cmd_dict["layer"]
        deploy_cmd_dict["pipeline_name"] = self._wsi._question(
            "Provide sdp meta pipeline name", default=f"sdp_meta_{layer}_pipeline_{uuid.uuid4().hex}")
        deploy_cmd_dict["dlt_target_schema"] = self._ident_question(
            "Provide dlt target schema name", kind="dlt_target_schema")
        return DeployCommand(**deploy_cmd_dict)

    def _load_onboard_config_ui(self, form_data) -> OnboardCommand:
        onboard_cmd_dict = {}

        # Get unity catalog settings
        onboard_cmd_dict["uc_enabled"] = True if form_data.get('unity_catalog_enabled') == "1" else False
        if onboard_cmd_dict["uc_enabled"]:
            onboard_cmd_dict["dbfs_path"] = None
            onboard_cmd_dict["uc_catalog_name"] = form_data.get('unity_catalog_name')
        else:
            onboard_cmd_dict["dbfs_path"] = f"dbfs:/sdp-meta_cli_demo_{uuid.uuid4().hex}"

        # Get serverless setting
        onboard_cmd_dict["serverless"] = True if form_data.get('serverless') == "1" else False
        if onboard_cmd_dict["serverless"]:
            onboard_cmd_dict["cloud"] = None
            onboard_cmd_dict["dbr_version"] = None
        else:
            # These fields are not in the form, so using defaults
            onboard_cmd_dict["cloud"] = "aws"  # Default value
            onboard_cmd_dict["dbr_version"] = self._ws.clusters.select_spark_version(latest=True)

        # Get file paths
        onboard_cmd_dict["onboarding_file_path"] = form_data.get(
            'onboarding_file_path', 'demo/conf/json/onboarding.template'
        )
        onboarding_files_dir_path = form_data.get('local_directory', f'{os.getcwd()}/demo/')
        onboard_cmd_dict["onboarding_files_dir_path"] = f"file:/{onboarding_files_dir_path}"

        # Get schema names
        onboard_cmd_dict["sdp_meta_schema"] = form_data.get(
            'sdp_meta_schema', f'sdp_meta_dataflowspecs_{uuid.uuid4().hex}'
        )
        onboard_cmd_dict["bronze_schema"] = form_data.get('bronze_schema', f'sdp_meta_bronze_{uuid.uuid4().hex}')
        onboard_cmd_dict["silver_schema"] = form_data.get('silver_schema', f'sdp_meta_silver_{uuid.uuid4().hex}')

        # Map sdp_meta_layer value from form to expected values
        layer_map = {
            "0": "bronze",
            "1": "bronze_silver",
            "2": "silver"
        }
        onboard_cmd_dict["onboard_layer"] = layer_map.get(form_data.get('sdp_meta_layer'), 'bronze_silver')

        # Handle layer-specific settings
        if onboard_cmd_dict["onboard_layer"] == "bronze" or onboard_cmd_dict["onboard_layer"] == "bronze_silver":
            onboard_cmd_dict["bronze_dataflowspec_table"] = form_data.get('bronze_table', 'bronze_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["bronze_dataflowspec_path"] = f'{self._install_folder()}/bronze_dataflow_specs'

        if onboard_cmd_dict["onboard_layer"] == "silver" or onboard_cmd_dict["onboard_layer"] == "bronze_silver":
            onboard_cmd_dict["silver_dataflowspec_table"] = form_data.get('silver_table', 'silver_dataflowspec')
            if not onboard_cmd_dict["uc_enabled"]:
                onboard_cmd_dict["silver_dataflowspec_path"] = f'{self._install_folder()}/silver_dataflow_specs'

        # Get other settings
        onboard_cmd_dict["overwrite"] = True if form_data.get('overwrite') == "1" else False
        onboard_cmd_dict["version"] = form_data.get('version', 'v1')
        onboard_cmd_dict["env"] = form_data.get('environment', 'prod')
        onboard_cmd_dict["import_author"] = form_data.get('author', self._wsi._short_name)
        onboard_cmd_dict["update_paths"] = True if form_data.get('update_paths') == "1" else False

        # Save to file
        with open("onboarding_job_details.json", "w") as oc_file:
            json.dump(onboard_cmd_dict, oc_file, indent=4)

        cmd = OnboardCommand(**onboard_cmd_dict)
        return cmd

    def _load_deploy_config_ui(self, input_params) -> DeployCommand:
        oc_job_details_json = None
        if os.path.isfile("onboarding_job_details.json"):
            with open("onboarding_job_details.json") as f:
                oc_job_details_json = f.read()

        load_from_ojd_json = input_params.get("load_from_ojd_json", False)
        deploy_cmd_dict = {}

        if load_from_ojd_json and oc_job_details_json:
            oc_job_details_json = json.loads(oc_job_details_json)
            deploy_cmd_dict["uc_enabled"] = input_params.get("uc_enabled", False)
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = input_params.get("uc_catalog_name")
                deploy_cmd_dict["serverless"] = input_params.get("serverless", False)
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = input_params.get("layer")
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_bronze_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_bronze_table"] = oc_job_details_json["bronze_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = oc_job_details_json["bronze_dataflowspec_path"]
                deploy_cmd_dict["onboard_bronze_group"] = input_params.get("onboard_bronze_group")
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                if deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["sdp_meta_silver_schema"] = self._get_schema_from_json(oc_job_details_json)
                    deploy_cmd_dict["dataflowspec_silver_table"] = oc_job_details_json["silver_dataflowspec_table"]
                else:
                    deploy_cmd_dict["dataflowspec_silver_path"] = oc_job_details_json["silver_dataflowspec_path"]
                deploy_cmd_dict["onboard_silver_group"] = input_params.get("onboard_silver_group")
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = input_params.get("num_workers", 4)
        else:
            deploy_cmd_dict["uc_enabled"] = input_params.get("uc_enabled", False)
            if deploy_cmd_dict["uc_enabled"]:
                deploy_cmd_dict["uc_catalog_name"] = input_params.get("uc_catalog_name")
                deploy_cmd_dict["serverless"] = input_params.get("serverless", False)
            else:
                deploy_cmd_dict["serverless"] = False
            deploy_cmd_dict["layer"] = input_params.get("layer")
            if deploy_cmd_dict["layer"] in ["bronze", "bronze_silver"]:
                deploy_cmd_dict["onboard_bronze_group"] = input_params.get("onboard_bronze_group")
                deploy_cmd_dict["sdp_meta_bronze_schema"] = input_params.get("sdp_meta_bronze_schema")
                deploy_cmd_dict["dataflowspec_bronze_table"] = input_params.get("dataflowspec_bronze_table",
                                                                                "bronze_dataflowspec")
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_bronze_path"] = input_params.get(
                        "dataflowspec_bronze_path",
                        f'{self._install_folder()}/bronze_dataflow_specs'
                    )
            if deploy_cmd_dict["layer"] in ["silver", "bronze_silver"]:
                deploy_cmd_dict["onboard_silver_group"] = input_params.get("onboard_silver_group")
                deploy_cmd_dict["sdp_meta_silver_schema"] = input_params.get("sdp_meta_silver_schema")
                deploy_cmd_dict["dataflowspec_silver_table"] = input_params.get("dataflowspec_silver_table",
                                                                                "silver_dataflowspec")
                if not deploy_cmd_dict["uc_enabled"]:
                    deploy_cmd_dict["dataflowspec_silver_path"] = input_params.get(
                        "dataflowspec_silver_path",
                        f'{self._install_folder()}/silver_dataflow_specs'
                    )
            if not deploy_cmd_dict["serverless"]:
                deploy_cmd_dict["num_workers"] = input_params.get("num_workers", 4)

        layer = deploy_cmd_dict["layer"]
        deploy_cmd_dict["pipeline_name"] = input_params.get("pipeline_name",
                                                            f"sdp_meta_{layer}_pipeline_{uuid.uuid4().hex}")
        deploy_cmd_dict["dlt_target_schema"] = input_params.get("dlt_target_schema")

        return DeployCommand(**deploy_cmd_dict)

    def update_ws_onboarding_paths(self, cmd: OnboardCommand):
        """Create onboarding file for cloudfiles as source."""
        string_subs = {
            "{uc_volume_path}": f"{cmd.uc_volume_path}/sdp_meta_conf/",
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


def onboard(sdp_meta: SDPMeta):
    logger.info("Please answer a couple of questions to for launching SDP META onboarding job")
    cmd = sdp_meta._load_onboard_config()
    sdp_meta.onboard(cmd)


def onboard_ui(sdp_meta: SDPMeta, form_data):
    logger.info("Please answer a couple of questions to for launching SDP META onboarding job")
    cmd = sdp_meta._load_onboard_config_ui(form_data)
    sdp_meta.onboard(cmd)


def deploy(sdp_meta: SDPMeta):
    logger.info("Please answer a couple of questions to for launching SDP META deployment job")
    cmd = sdp_meta._load_deploy_config()
    sdp_meta.deploy(cmd)


def deploy_ui(sdp_meta: SDPMeta, form_data):
    logger.info("Please answer a couple of questions to for launching SDP META deployment job")
    cmd = sdp_meta._load_deploy_config_ui(form_data)
    sdp_meta.deploy(cmd)


# ---------------------------------------------------------------------------
# DAB (Declarative Automation Bundle) command wrappers
#
# Mirror the onboard/deploy pattern: each wrapper takes the shared SDPMeta
# instance, uses `sdp_meta._wsi` (a WorkspaceInstaller, set up in the SDPMeta
# constructor) as the interactive prompter, and delegates to the matching
# pure-function handler in `bundle.py`. The handlers themselves don't need
# a WorkspaceClient -- they shell out to the `databricks` CLI -- but routing
# them through SDPMeta keeps the dispatcher's signature uniform with the
# legacy commands.
#
# These four entries MUST stay in lock-step with `labs.yml commands:` and
# the `MAPPING` dict below. The `tests/test_cli.py::CliCommandWiringTests`
# regression test enforces both.
# ---------------------------------------------------------------------------


def bundle_init(sdp_meta: SDPMeta, flags: dict = None):
    """Scaffold a new sdp-meta DAB.

    With ``--quickstart`` (declared in labs.yml), all 13 template prompts are
    pre-answered with developer-friendly defaults via a generated
    ``--config-file`` (see ``QUICKSTART_BUNDLE_INIT_DEFAULTS`` in bundle.py).
    The user still has to fix ``sdp_meta_dependency`` afterwards, but the
    happy path is ``bundle-init --quickstart`` -> edit one file ->
    ``bundle-validate``. ``--output-dir`` is honored by both modes.
    Without ``--quickstart``, falls back to the interactive prompts.
    """
    from databricks.labs.sdp_meta.bundle import (
        BundleInitCommand,
        _load_bundle_init_config,
        bundle_init as _run,
        write_quickstart_config_file,
    )

    flags = flags or {}
    quickstart = bool(flags.get("quickstart"))
    output_dir = flags.get("output-dir") or flags.get("output_dir") or "."

    if quickstart:
        logger.info(
            "Scaffolding a new sdp-meta DAB in --quickstart mode "
            "(no prompts; developer defaults)."
        )
        # Stash the generated config alongside the rendered bundle so users
        # have a record of which defaults they got. tempfile would also work
        # but leaving it next to the bundle aids debugging.
        cfg_dir = Path(output_dir).resolve()
        cfg_dir.mkdir(parents=True, exist_ok=True)
        cfg_path = write_quickstart_config_file(cfg_dir)
        cmd = BundleInitCommand(output_dir=output_dir, config_file=str(cfg_path))
    else:
        logger.info("Scaffolding a new sdp-meta DAB from the packaged template.")
        cmd = _load_bundle_init_config(sdp_meta._wsi)
        # CLI --output-dir wins over an interactive answer when both are given,
        # since the user explicitly typed it on the command line.
        if flags.get("output-dir") or flags.get("output_dir"):
            cmd.output_dir = output_dir
    _run(cmd)


def bundle_prepare_wheel(sdp_meta: SDPMeta, flags: dict = None):
    # `flags` is accepted for dispatcher uniformity (see main()) but unused.
    del flags
    logger.info("Building the sdp-meta wheel and uploading it to a UC volume.")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_prepare_wheel_config,
        bundle_prepare_wheel as _run,
    )
    _run(_load_bundle_prepare_wheel_config(sdp_meta._wsi))


def bundle_validate(sdp_meta: SDPMeta, flags: dict = None):
    del flags
    logger.info("Validating the sdp-meta DAB (databricks bundle validate + sanity checks).")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_validate_config,
        bundle_validate as _run,
    )
    rc = _run(_load_bundle_validate_config(sdp_meta._wsi))
    if rc != 0:
        sys.exit(rc)


def bundle_add_flow(sdp_meta: SDPMeta, flags: dict = None):
    del flags
    logger.info("Appending flow entries to the bundle's onboarding file.")
    from databricks.labs.sdp_meta.bundle import (
        _load_bundle_add_flow_config,
        bundle_add_flow as _run,
    )
    rc = _run(_load_bundle_add_flow_config(sdp_meta._wsi))
    if rc != 0:
        sys.exit(rc)


MAPPING = {
    "onboard": onboard,
    "deploy": deploy,
    "onboard_ui": onboard_ui,
    "deploy_ui": deploy_ui,
    "bundle-init": bundle_init,
    "bundle-prepare-wheel": bundle_prepare_wheel,
    "bundle-validate": bundle_validate,
    "bundle-add-flow": bundle_add_flow,
}


# Backwards compatibility alias for class name (deprecated)
DLTMeta = SDPMeta


def main(raw):
    payload = json.loads(raw)
    command = payload["command"]

    if command not in MAPPING:
        msg = f"cannot find command: {command}. Available: {list(MAPPING.keys())}"
        raise KeyError(msg)
    flags = payload["flags"]
    log_level = flags.pop("log_level")
    if log_level != "disabled":
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())
    version = __about__.__version__
    # Support both old and new product names
    ws = WorkspaceClient(product='sdp-meta', product_version=version)
    sdp_meta = SDPMeta(ws)
    if command in ["onboard_ui", "deploy_ui"]:
        MAPPING[command](sdp_meta, payload)
    elif command.startswith("bundle-"):
        # Bundle wrappers receive `flags` so they can opt into non-interactive
        # behavior (e.g. `bundle-init --quickstart`). Wrappers that ignore
        # flags accept them as a `flags=None` keyword arg, which keeps the
        # wrapper callable in tests without constructing a fake payload.
        MAPPING[command](sdp_meta, flags=flags)
    else:
        MAPPING[command](sdp_meta)


if __name__ == "__main__":
    main(*sys.argv[1:])
