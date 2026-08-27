import logging

from databricks.sdk import WorkspaceClient

from src.__about__ import __version__
from src.install import WorkspaceInstaller

logger = logging.getLogger("databricks.labs.dlt-meta.install")

if __name__ == "__main__":
    logger.setLevel("INFO")
    ws = WorkspaceClient(product="dlt-meta", product_version=__version__)
    installer = WorkspaceInstaller(ws)
    installer.uninstall()
