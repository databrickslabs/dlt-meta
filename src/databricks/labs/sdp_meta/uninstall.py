import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.sdp_meta.__about__ import __version__
from databricks.labs.sdp_meta.install import WorkspaceInstaller

logger = logging.getLogger("databricks.labs.sdp-meta.install")

if __name__ == "__main__":
    logger.setLevel("INFO")
    ws = WorkspaceClient(product="sdp-meta", product_version=__version__)
    installer = WorkspaceInstaller(ws)
    installer.uninstall()
