import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import webbrowser
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import OperationFailed
from databricks.sdk.mixins.compute import SemVer
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.sql import EndpointInfoWarehouseType, SpotInstancePolicy
from databricks.sdk.service.workspace import ImportFormat

from src.config import WorkspaceConfig

from __about__ import __version__

TAG_STEP = "step"
TAG_APP = "App"
NUM_USER_ATTEMPTS = 10  # number of attempts user gets at answering a question
EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
    "parent_run_id": "{{parent_run_id}}",
}

DEBUG_NOTEBOOK = """
# Databricks notebook source
# MAGIC %md
# MAGIC # Debug companion for UCX installation (see [README]({readme_link}))
# MAGIC
# MAGIC Production runs are supposed to be triggered through the following jobs: {job_links}
# MAGIC
# MAGIC **This notebook is overwritten with each UCX update/(re)install.**

# COMMAND ----------

# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

import logging
from pathlib import Path
from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework import logger
from databricks.sdk import WorkspaceClient

logger._install()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = WorkspaceConfig.from_file(Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

RUNNER_NOTEBOOK = """
# Databricks notebook source
# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.ucx.runtime import main

main(f'--config=/Workspace{config_file}',
     f'--task=' + dbutils.widgets.get('task'),
     f'--job_id=' + dbutils.widgets.get('job_id'),
     f'--run_id=' + dbutils.widgets.get('run_id'),
     f'--parent_run_id=' + dbutils.widgets.get('parent_run_id'))
"""

logger = logging.getLogger('databricks.labs.dltmeta')

@dataclass
class Task:
    task_id: int
    workflow: str
    name: str
    doc: str
    fn: Callable[[WorkspaceConfig], None]
    depends_on: list[str] = None
    job_cluster: str = "main"
    notebook: str = None
    dashboard: str = None


class WorkspaceInstaller:
    def __init__(self, ws: WorkspaceClient, *, prefix: str = "dlt-meta", promtps: bool = True):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._prefix = prefix
        self._prompts = promtps
        self._this_file = Path(__file__)
        self._override_clusters = {}
        self._dashboards = {}

    def run(self):
        logger.info(f"Installing DLT-META v{self._version}")
        self._configure()
        self._run_configured()

    def _run_configured(self):
        # TODO: put back when ready self._create_jobs()
        readme = f'{self.notebook_link(f"{self._install_folder}/README.py")}'
        msg = f"Installation completed successfully! Please refer to the {readme} notebook for next steps."
        logger.info(msg)

    @property
    def _warehouse_id(self) -> str:
        if self._current_config.warehouse_id is not None:
            return self._current_config.warehouse_id
        warehouses = [_ for _ in self._ws.warehouses.list() if _.warehouse_type == EndpointInfoWarehouseType.PRO]
        warehouse_id = self._current_config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing PRO SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        self._current_config.warehouse_id = warehouse_id
        return warehouse_id

    @property
    def _my_username(self):
        if not hasattr(self, "_me"):
            self._me = self._ws.current_user.me()
        return self._me.user_name

    @property
    def _short_name(self):
        if "@" in self._my_username:
            username = self._my_username.split("@")[0]
        else:
            username = self._me.display_name
        return username

    @property
    def _install_folder(self):
        return f"/Users/{self._my_username}/.{self._prefix}"

    @property
    def config_file(self):
        return f"{self._install_folder}/config.json"

    @property
    def _current_config(self):
        if hasattr(self, "_config"):
            return self._config
        with self._ws.workspace.download(self.config_file) as f:
            self._config = WorkspaceConfig.from_bytes(f.read())
        return self._config

    def _name(self, name: str) -> str:
        return f"[{self._prefix.upper()}][{self._short_name}] {name}"

    def _configure(self):
        ws_file_url = self.notebook_link(self.config_file)
        try:
            self._ws.workspace.get_status(self.config_file)
            logger.info(f"DLT META is already configured. See {ws_file_url}")
            return
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err

        logger.info("Please answer a couple of questions to configure DLT META")

        self._config = WorkspaceConfig(
            dbr_version=self._choice_from_dict("Provide databricks runtime spark version", {
                sv.name: sv.key for sv in self._ws.clusters.spark_versions().versions
                    if 'ml' not in sv.key 
                    and 'aarch64' not in sv.key 
                    and 'LTS' in sv.name
            }),
            source=self._choice("Provide source type", ['cloudfiles', 'kafka', 'eventhub']),
            dbfs_path=self._question("Provide databricks workspace dbfs path", default=f'dbfs:{self._install_folder}/data'),
        )
        if self._config.source == 'eventhub':
            self._config.eventhub_name=self._question("Provide eventhub_name e.g iot"),
            self._config.eventhub_producer_accesskey_name=self._question("Provide access key that has write permission on the eventhub e.g iotProducerAccessKey"),
            self._config.eventhub_consumer_accesskey_name=self._question("Provide access key that has read permission on the eventhub  e.g iotConsumerAccessKey"),
            self._config.eventhub_producer_accesskey_secret_name=self._question("Provide name of the secret that stores access key with write permission on the eventhub. Optional if same as `eventhub_producer_accesskey_name` e.g iotProducerAccessKey"),
            self._config.eventhub_consumer_accesskey_secret_name=self._question("Provide name of the secret that stores access key with read permission on the eventhub. Optional if same as `eventhub_consumer_accesskey_name`  e.g iotConsumerAccessKey"),
            self._config.eventhub_secrets_scope_name=self._question("Provide eventhub_secrets_scope_name e.g eventhubs_creds"),
            self._config.eventhub_namespace=self._question("Provide eventhub_namespace e.g topic-standard"),
            self._config.eventhub_port=self._question("Provide eventhub_port", default='9093'),
        elif self._config.source == 'kafka':
            self._config.kafka_topic_name=self._question("Provide kafka topic name e.g iot"),
            self._config.kafka_broker=self._question("Provide kafka broker", default='127.0.0.1:9092'),

        self._write_config()
        msg = "Open config file in the browser and continue installing?"
        if self._prompts and self._question(msg, default="yes") == "yes":
            webbrowser.open(ws_file_url)

    def _write_config(self):
        try:
            self._ws.workspace.get_status(self._install_folder)
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err
            logger.debug(f"Creating install folder: {self._install_folder}")
            self._ws.workspace.mkdirs(self._install_folder)

        config_bytes = json.dumps(self._config.as_dict(), indent=2).encode("utf8")
        logger.info(f"Creating configuration file: {self.config_file}")
        self._ws.workspace.upload(self.config_file, config_bytes, format=ImportFormat.AUTO)

    def _create_jobs(self):
        logger.debug(f"Creating jobs from tasks")
        remote_wheel = self._upload_wheel()
        self._deployed_steps = self.deployed_steps()
        desired_steps = {'main'}
        wheel_runner = None

        if self._override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheel)
        for step_name in desired_steps:
            settings = self._job_settings(step_name, remote_wheel)
            if self._override_clusters:
                settings = self._apply_cluster_overrides(settings, self._override_clusters, wheel_runner)
            if step_name in self._deployed_steps:
                job_id = self._deployed_steps[step_name]
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            else:
                logger.info(f"Creating new job configuration for step={step_name}")
                self._deployed_steps[step_name] = self._ws.jobs.create(**settings).job_id

        for step_name, job_id in self._deployed_steps.items():
            if step_name not in desired_steps:
                logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                self._ws.jobs.delete(job_id)

        self._create_readme()
        self._create_debug(remote_wheel)

    def _create_debug(self, remote_wheel: str):
        readme_link = self.notebook_link(f"{self._install_folder}/README.py")
        job_links = ", ".join(
            f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            for step_name, job_id in self._deployed_steps.items()
        )
        path = f"{self._install_folder}/DEBUG.py"
        logger.debug(f"Created debug notebook: {self.notebook_link(path)}")
        self._ws.workspace.upload(
            path,
            DEBUG_NOTEBOOK.format(
                remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links, config_file=self.config_file
            ).encode("utf8"),
            overwrite=True,
        )

    def notebook_link(self, path: str) -> str:
        return f"{self._ws.config.host}/#workspace{path}"

    def _choice_from_dict(self, text: str, choices: dict[str, Any]) -> Any:
        key = self._choice(text, list(choices.keys()))
        return choices[key]

    def _choice(self, text: str, choices: list[Any], *, max_attempts: int = 10) -> str:
        if not self._prompts:
            return "any"
        choices = sorted(choices, key=str.casefold)
        numbered = "\n".join(f"\033[1m[{i}]\033[0m \033[36m{v}\033[0m" for i, v in enumerate(choices))
        prompt = f"\033[1m{text}\033[0m\n{numbered}\nEnter a number between 0 and {len(choices)-1}: "
        attempt = 0
        while attempt < max_attempts:
            attempt += 1
            res = input(prompt)
            try:
                res = int(res)
            except ValueError:
                print(f"\033[31m[ERROR] Invalid number: {res}\033[0m\n")
                continue
            if res >= len(choices) or res < 0:
                print(f"\033[31m[ERROR] Out of range: {res}\033[0m\n")
                continue
            return choices[res]
        msg = f"cannot get answer within {max_attempts} attempt"
        raise ValueError(msg)

    @staticmethod
    def _question(text: str, *, default: str | None = None) -> str:
        default_help = "" if default is None else f"\033[36m (default: {default})\033[0m"
        prompt = f"\033[1m{text}{default_help}: \033[0m"
        res = None
        while not res:
            res = input(prompt)
            if not res and default is not None:
                return default
        return res

    def _upload_wheel(self) -> str:
        with tempfile.TemporaryDirectory() as tmp_dir:
            local_wheel = self._build_wheel(tmp_dir)
            remote_wheel = f"{self._install_folder}/wheels/{local_wheel.name}"
            remote_dirname = os.path.dirname(remote_wheel)
            with local_wheel.open("rb") as f:
                self._ws.dbfs.mkdirs(remote_dirname)
                logger.info(f"Uploading wheel to dbfs:{remote_wheel}")
                self._ws.dbfs.upload(remote_wheel, f, overwrite=True)
            with local_wheel.open("rb") as f:
                self._ws.workspace.mkdirs(remote_dirname)
                logger.info(f"Uploading wheel to /Workspace{remote_wheel}")
                self._ws.workspace.upload(remote_wheel, f, overwrite=True, format=ImportFormat.AUTO)
        return remote_wheel
    
    @property
    def _app(self):
        return 'dlt-meta'

    def _job_settings(self, step_name: str, dbfs_path: str):
        email_notifications = None
        if not self._override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted([t for t in _TASKS.values() if t.workflow == step_name], key=lambda _: _.name)
        version = self._version if not self._ws.config.is_gcp else self._version.replace("+", "-")
        return {
            "name": self._name(step_name),
            "tags": {TAG_APP: self._app, TAG_STEP: step_name, "version": f"v{version}"},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task, dbfs_path) for task in tasks],
        }

    def _upload_wheel_runner(self, remote_wheel: str):
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        path = f"{self._install_folder}/wheels/wheel-test-runner-{self._version}.py"
        logger.debug(f"Created runner notebook: {self.notebook_link(path)}")
        py = RUNNER_NOTEBOOK.format(remote_wheel=remote_wheel, config_file=self.config_file).encode("utf8")
        self._ws.workspace.upload(path, py, overwrite=True)
        return path

    @staticmethod
    def _apply_cluster_overrides(settings: dict[str, any], overrides: dict[str, str], wheel_runner: str) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
            if job_task.python_wheel_task is not None:
                job_task.python_wheel_task = None
                params = {"task": job_task.task_key} | EXTRA_TASK_PARAMS
                job_task.notebook_task = jobs.NotebookTask(notebook_path=wheel_runner, base_parameters=params)
        return settings

    def _job_task(self, task, dbfs_path: str) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].depends_on],
        )
        if task.dashboard:
            return self._job_dashboard_task(jobs_task, task)
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task, dbfs_path)

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        local_notebook = self._this_file.parent / task.notebook
        remote_notebook = f"{self._install_folder}/{local_notebook.name}"
        with local_notebook.open("rb") as f:
            self._ws.workspace.upload(remote_notebook, f, overwrite=True)
        return replace(
            jobs_task,
            notebook_task=jobs.NotebookTask(
                notebook_path=remote_notebook,
                # ES-872211: currently, we cannot read WSFS files from Scala context
                base_parameters={
                    "inventory_database": self._current_config.inventory_database,
                    "task": task.name,
                    "config": f"/Workspace{self.config_file}",
                }
                | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_wheel_task(self, jobs_task: jobs.Task, task: Task, dbfs_path: str) -> jobs.Task:
        return replace(
            jobs_task,
            libraries=[compute.Library(whl=f"dbfs:{dbfs_path}")],
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_ucx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters={"task": task.name, "config": f"/Workspace{self.config_file}"} | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        spec = self._cluster_node_type(
            compute.ClusterSpec(
                spark_version=self._ws.clusters.select_spark_version(latest=True),
                data_security_mode=compute.DataSecurityMode.NONE,
                spark_conf={"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"},
                custom_tags={"ResourceClass": "SingleNode"},
                num_workers=0,
            )
        )
        if "main" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=spec,
                )
            )
        if "tacl" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="tacl",
                    new_cluster=replace(
                        spec,
                        data_security_mode=compute.DataSecurityMode.LEGACY_TABLE_ACL,
                        spark_conf={"spark.databricks.acl.sqlOnly": "true"},
                        num_workers=1,  # ShowPermissionsCommand needs a worker
                        custom_tags={},
                    ),
                )
            )
        return clusters

    @property
    def _version(self):
        return __version__

    def _build_wheel(self, tmp_dir: str, *, verbose: bool = False):
        """Helper to build the wheel package"""
        streams = {}
        if not verbose:
            streams = {
                "stdout": subprocess.DEVNULL,
                "stderr": subprocess.DEVNULL,
            }
        project_root = self._find_project_root()
        is_non_released_version = "+" in self._version
        if (project_root / ".git" / "config").exists() and is_non_released_version:
            tmp_dir_path = Path(tmp_dir) / "working-copy"
            # copy everything to a temporary directory
            shutil.copytree(project_root, tmp_dir_path)
            # and override the version file
            version_file = tmp_dir_path / "src/__about__.py"
            with version_file.open("w") as f:
                f.write(f'__version__ = "{self._version}"')
            # working copy becomes project root for building a wheel
            project_root = tmp_dir_path
        logger.debug(f"Building wheel for {project_root} in {tmp_dir}")
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root],
            **streams,
            check=True,
        )
        # get wheel name as first file in the temp directory
        return next(Path(tmp_dir).glob("*.whl"))

    def _find_project_root(self) -> Path:
        for leaf in ["pyproject.toml", "setup.py"]:
            root = WorkspaceInstaller._find_dir_with_leaf(self._this_file, leaf)
            if root is not None:
                return root
        msg = "Cannot find project root"
        raise NotADirectoryError(msg)

    @staticmethod
    def _find_dir_with_leaf(folder: Path, leaf: str) -> Path | None:
        root = folder.root
        while str(folder.absolute()) != root:
            if (folder / leaf).exists():
                return folder
            folder = folder.parent
        return None

    def _cluster_node_type(self, spec: compute.ClusterSpec) -> compute.ClusterSpec:
        cfg = self._current_config
        if cfg.instance_pool_id is not None:
            return replace(spec, instance_pool_id=cfg.instance_pool_id)
        spec = replace(spec, node_type_id=self._ws.clusters.select_node_type(local_disk=True))
        if self._ws.config.is_aws:
            return replace(spec, aws_attributes=compute.AwsAttributes(availability=compute.AwsAvailability.ON_DEMAND))
        if self._ws.config.is_azure:
            return replace(
                spec, azure_attributes=compute.AzureAttributes(availability=compute.AzureAvailability.ON_DEMAND_AZURE)
            )
        return replace(spec, gcp_attributes=compute.GcpAttributes(availability=compute.GcpAvailability.ON_DEMAND_GCP))

    def deployed_steps(self):
        deployed_steps = {}
        logger.debug(f"Fetching all jobs to determine already deployed steps for app={self._app}")
        for j in self._ws.jobs.list():
            tags = j.settings.tags
            if tags is None:
                continue
            if tags.get(TAG_APP, None) != self._app:
                continue
            deployed_steps[tags.get(TAG_STEP, "_")] = j.job_id
        return deployed_steps

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for step, job_id in self.deployed_steps().items():
            job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
            latest_status.append({
                'step': step,
                'state': 'UNKNOWN' if not job_runs else str(job_runs[0].state.result_state),
                'started': '' if not job_runs else job_runs[0].start_time
            })
        return latest_status


if __name__ == "__main__":
    # developing installer:
    # 1. make clean dev
    # 2. . .databricks/bin/activate
    # 3. python -m src.install

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel('DEBUG')
    logging.root.addHandler(console_handler)

    ws = WorkspaceClient(product="dlt-meta", product_version=__version__)
    logger.setLevel("INFO")
    installer = WorkspaceInstaller(ws)
    installer.run()
