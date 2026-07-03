"""Stage a bundle's ``conf/`` tree onto a UC Volume before onboarding runs.

Why this exists
---------------
The DAB template syncs ``conf/`` (onboarding file, DQE, silver transformations,
schema DDLs, ...) to the bundle's workspace files root, and the onboarding job
originally read them from ``${workspace.file_path}/conf/...``. That breaks in
two independent ways:

1. ``${workspace.file_path}`` is a DAB *substitution* variable. DAB only expands
   it inside bundle *config* files (``databricks.yml`` / ``resources/*.yml``) --
   NOT inside synced *data* files like ``conf/onboarding.yml``. So any
   ``${workspace.file_path}/conf/...`` path written inside the onboarding file
   reaches the engine as a literal, unresolved token and Spark fails with
   ``[PATH_NOT_FOUND] ... dbfs:/${workspace.file_path}/conf/...``.

2. On serverless compute, Spark's text/json reader cannot read
   ``/Workspace/.../files/...`` paths at all (they resolve to ``dbfs:/Workspace``
   and fail with ``PATH_NOT_FOUND``). Conf files must live on a UC Volume (or
   cloud storage) to be readable.

This module runs as the FIRST task of the onboarding job. It copies every file
under the workspace-synced ``conf/`` dir to ``/Volumes/<catalog>/<schema>/
<volume>/conf/`` and, for text files, rewrites the literal
``${workspace.file_path}/conf`` token to that volume base as it copies. The
onboarding task (which depends on this one) then reads ``onboarding_file_path``
from the volume, and the persisted dataflowspec rows carry volume-absolute paths
that the downstream pipelines can also read on serverless.

This mirrors what ``demo/launch_dab_template_demo.py`` does host-side, but runs
inside the job so a plain ``databricks bundle deploy`` + ``databricks bundle run
onboarding`` works out of the box.
"""
import argparse
import io
import logging
import posixpath

logger = logging.getLogger("sdp-meta")
logger.setLevel(logging.INFO)

# The literal token the DAB template writes into conf data files. We rewrite the
# ``.../conf`` prefix so anything below it (``/schemas/x.ddl``,
# ``/dqe/.../y.yml``, ``/silver_transformations.yml``) rebases onto the volume.
WORKSPACE_CONF_TOKEN = "${workspace.file_path}/conf"

# Suffixes we treat as text (decode + token-rewrite). Everything else is copied
# byte-for-byte.
_TEXT_SUFFIXES = (".yml", ".yaml", ".json", ".ddl", ".csv", ".txt", ".sql")


def is_text_file(name):
    """Return True if ``name`` should be treated as a rewritable text file."""
    lowered = name.lower()
    return any(lowered.endswith(suffix) for suffix in _TEXT_SUFFIXES)


def rewrite_conf_text(text, volume_conf_base, token=WORKSPACE_CONF_TOKEN):
    """Rebase ``${workspace.file_path}/conf`` references onto the UC Volume.

    Args:
        text: file contents.
        volume_conf_base: e.g. ``/Volumes/main/sdp_meta/bundle_conf/conf``.
        token: the workspace token prefix to replace.
    """
    return text.replace(token, volume_conf_base)


def volume_conf_base(uc_catalog, uc_schema, conf_volume):
    """Build the ``/Volumes/.../conf`` base path for the staged conf tree."""
    return f"/Volumes/{uc_catalog}/{uc_schema}/{conf_volume}/conf"


def ensure_volume(ws, uc_catalog, uc_schema, conf_volume):
    """Create the schema + managed volume if they don't already exist.

    Idempotent: pre-existing schema/volume are left untouched. Kept tolerant of
    SDK version differences in the ``AlreadyExists`` error surface.
    """
    from databricks.sdk.service.catalog import VolumeType

    try:
        ws.schemas.create(name=uc_schema, catalog_name=uc_catalog)
        logger.info(f"Created schema {uc_catalog}.{uc_schema}")
    except Exception as exc:  # noqa: BLE001 - schema may already exist
        logger.info(f"Schema {uc_catalog}.{uc_schema} not created ({exc}); assuming it exists")

    try:
        ws.volumes.create(
            catalog_name=uc_catalog,
            schema_name=uc_schema,
            name=conf_volume,
            volume_type=VolumeType.MANAGED,
        )
        logger.info(f"Created volume {uc_catalog}.{uc_schema}.{conf_volume}")
    except Exception as exc:  # noqa: BLE001 - volume may already exist
        logger.info(f"Volume {uc_catalog}.{uc_schema}.{conf_volume} not created ({exc}); assuming it exists")


def _iter_workspace_files(ws, root):
    """Yield absolute workspace paths of every FILE under ``root`` (recursive)."""
    from databricks.sdk.service.workspace import ObjectType

    for obj in ws.workspace.list(root):
        object_type = getattr(obj, "object_type", None)
        if object_type == ObjectType.DIRECTORY:
            yield from _iter_workspace_files(ws, obj.path)
        else:
            # FILE or NOTEBOOK-shaped entries; conf/ only holds data files.
            yield obj.path


def _download_workspace_file(ws, path):
    """Return the raw bytes of a workspace file via the workspace export API."""
    handle = ws.workspace.download(path)
    try:
        return handle.read()
    finally:
        close = getattr(handle, "close", None)
        if callable(close):
            close()


def stage_conf_tree(ws, source_conf_dir, target_conf_base):
    """Copy every file under ``source_conf_dir`` to ``target_conf_base``.

    Text files get their ``${workspace.file_path}/conf`` token rewritten to
    ``target_conf_base``; binaries are copied verbatim.

    Returns the number of files staged.
    """
    source_conf_dir = source_conf_dir.rstrip("/")
    staged = 0
    for src in _iter_workspace_files(ws, source_conf_dir):
        rel = posixpath.relpath(src, source_conf_dir)
        dst = f"{target_conf_base}/{rel}"
        raw = _download_workspace_file(ws, src)
        if is_text_file(src):
            text = raw.decode("utf-8")
            patched = rewrite_conf_text(text, target_conf_base)
            payload = patched.encode("utf-8")
        else:
            payload = raw
        ws.files.upload(file_path=dst, contents=io.BytesIO(payload), overwrite=True)
        logger.info(f"Staged {src} -> {dst}")
        staged += 1
    return staged


def parse_args(argv=None):
    """Parse the named parameters passed by the onboarding job's stage task."""
    parser = argparse.ArgumentParser(description="Stage bundle conf/ onto a UC Volume.")
    parser.add_argument("--source_conf_dir", required=True,
                        help="Workspace path of the synced conf/ dir (DAB expands "
                             "${workspace.file_path}/conf).")
    parser.add_argument("--uc_catalog", required=True)
    parser.add_argument("--uc_schema", required=True)
    parser.add_argument("--conf_volume", required=True,
                        help="UC Volume name (inside uc_schema) to stage conf/ into.")
    return parser.parse_args(argv)


def main(argv=None):
    """Wheel entry point: ensure the volume, then stage conf/ into it."""
    args = parse_args(argv)
    from databricks.sdk import WorkspaceClient

    ws = WorkspaceClient()
    target = volume_conf_base(args.uc_catalog, args.uc_schema, args.conf_volume)
    logger.info(f"Staging {args.source_conf_dir} -> {target} (Spark-readable on serverless)")
    ensure_volume(ws, args.uc_catalog, args.uc_schema, args.conf_volume)
    staged = stage_conf_tree(ws, args.source_conf_dir, target)
    logger.info(f"Staged {staged} conf file(s) under {target}/")


if __name__ == "__main__":
    main()
