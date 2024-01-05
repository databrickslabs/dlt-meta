import logging
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import replace
from pathlib import Path
from typing import Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.workspace import ImportFormat
from databricks.sdk.service import compute
from databricks.sdk.service.sql import EndpointInfoWarehouseType
from src.config import WorkspaceConfig
from src.__about__ import __version__


logger = logging.getLogger('databricks.labs.dltmeta')


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
        return f"/Users/{self._my_username}/{self._prefix}"

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
    def _find_dir_with_leaf(folder: Path, leaf: str) -> Path:
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

    @staticmethod
    def _question(text: str, *, default: str = None) -> str:
        default_help = "" if default is None else f"\033[36m (default: {default})\033[0m"
        prompt = f"\033[1m{text}{default_help}: \033[0m"
        res = None
        while not res:
            res = input(prompt)
            if not res and default is not None:
                return default
        return res

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

    def _choice_from_dict(self, text: str, choices: dict[str, Any]) -> Any:
        key = self._choice(text, list(choices.keys()))
        return choices[key]


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
