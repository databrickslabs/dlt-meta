import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.sql import EndpointInfoWarehouseType

from src.config import WorkspaceConfig

from __about__ import __version__

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
        try:
            self._ws.workspace.get_status(self.config_file)
            logger.info("DLT META is already installed.")
            return
        except DatabricksError as err:
            if err.error_code != "RESOURCE_DOES_NOT_EXIST":
                raise err

    @property
    def _app(self):
        return 'dlt-meta'

    @property
    def _version(self):
        return __version__


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
