"""Unit tests for the conf -> UC Volume staging entry point."""
import io
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from databricks.sdk.service.workspace import ObjectType

from databricks.labs.sdp_meta import stage_conf


class _FakeWorkspaceAPI:
    """Minimal stand-in for WorkspaceClient.workspace backed by a flat dict."""

    def __init__(self, files):
        # files: {absolute_path: bytes}
        self.files = files

    def list(self, path):
        path = path.rstrip("/")
        children = {}
        for fpath in self.files:
            if not fpath.startswith(path + "/"):
                continue
            rest = fpath[len(path) + 1:]
            head, _, tail = rest.partition("/")
            child_path = f"{path}/{head}"
            children[child_path] = ObjectType.DIRECTORY if tail else ObjectType.FILE
        return [SimpleNamespace(path=p, object_type=t) for p, t in children.items()]

    def download(self, path):
        return io.BytesIO(self.files[path])


class _FakeFilesAPI:
    def __init__(self):
        self.uploaded = {}

    def upload(self, file_path, contents, overwrite=False):
        self.uploaded[file_path] = contents.read()


class _FakeCatalogAPI:
    def __init__(self, raise_on_create=False):
        self.raise_on_create = raise_on_create
        self.created = []

    def create(self, **kwargs):
        if self.raise_on_create:
            raise Exception("already exists")
        self.created.append(kwargs)


class _FakeWorkspaceClient:
    def __init__(self, files, raise_on_create=False):
        self.workspace = _FakeWorkspaceAPI(files)
        self.files = _FakeFilesAPI()
        self.schemas = _FakeCatalogAPI(raise_on_create)
        self.volumes = _FakeCatalogAPI(raise_on_create)


class StageConfPureTests(unittest.TestCase):
    def test_rewrite_conf_text_rebases_token(self):
        text = 'source_schema_path: "${workspace.file_path}/conf/schemas/t.ddl"'
        out = stage_conf.rewrite_conf_text(text, "/Volumes/c/s/v/conf")
        self.assertEqual(
            out, 'source_schema_path: "/Volumes/c/s/v/conf/schemas/t.ddl"'
        )
        self.assertNotIn("${workspace.file_path}", out)

    def test_rewrite_leaves_unrelated_text_untouched(self):
        text = "no token here"
        self.assertEqual(stage_conf.rewrite_conf_text(text, "/Volumes/c/s/v/conf"), text)

    def test_is_text_file(self):
        for name in ("a.yml", "a.YAML", "a.json", "x.ddl", "d.csv", "n.txt", "q.sql"):
            self.assertTrue(stage_conf.is_text_file(name), name)
        for name in ("a.parquet", "b.png", "c.whl"):
            self.assertFalse(stage_conf.is_text_file(name), name)

    def test_volume_conf_base(self):
        self.assertEqual(
            stage_conf.volume_conf_base("main", "sdp_meta", "bundle_conf"),
            "/Volumes/main/sdp_meta/bundle_conf/conf",
        )

    def test_parse_args(self):
        args = stage_conf.parse_args([
            "--source_conf_dir=/Workspace/Users/me/files/conf",
            "--uc_catalog=main",
            "--uc_schema=sdp_meta",
            "--conf_volume=bundle_conf",
        ])
        self.assertEqual(args.source_conf_dir, "/Workspace/Users/me/files/conf")
        self.assertEqual(args.uc_catalog, "main")
        self.assertEqual(args.uc_schema, "sdp_meta")
        self.assertEqual(args.conf_volume, "bundle_conf")


class StageConfTreeTests(unittest.TestCase):
    def setUp(self):
        self.root = "/Workspace/Users/me/files/conf"
        self.target = "/Volumes/main/sdp_meta/bundle_conf/conf"
        self.files = {
            f"{self.root}/onboarding.yml": (
                b'- data_flow_id: "100"\n'
                b'  silver_transformation_json_dev: '
                b'"${workspace.file_path}/conf/silver_transformations.yml"\n'
            ),
            f"{self.root}/silver_transformations.yml": b"- target_table: t\n",
            f"{self.root}/dqe/example_table/bronze_expectations.yml": (
                b"expectations: []\n"
            ),
            # A binary file must be copied byte-for-byte (no decode/rewrite).
            f"{self.root}/data/seed.parquet": b"\x00\x01PAR1${workspace.file_path}/conf",
        }
        self.ws = _FakeWorkspaceClient(self.files)

    def test_stage_conf_tree_uploads_all_files_to_volume(self):
        staged = stage_conf.stage_conf_tree(self.ws, self.root, self.target)
        self.assertEqual(staged, 4)
        expected_paths = {
            f"{self.target}/onboarding.yml",
            f"{self.target}/silver_transformations.yml",
            f"{self.target}/dqe/example_table/bronze_expectations.yml",
            f"{self.target}/data/seed.parquet",
        }
        self.assertEqual(set(self.ws.files.uploaded), expected_paths)

    def test_text_files_get_token_rewritten(self):
        stage_conf.stage_conf_tree(self.ws, self.root, self.target)
        onboarding = self.ws.files.uploaded[f"{self.target}/onboarding.yml"].decode()
        self.assertIn(f"{self.target}/silver_transformations.yml", onboarding)
        self.assertNotIn("${workspace.file_path}", onboarding)

    def test_binary_files_are_not_rewritten(self):
        stage_conf.stage_conf_tree(self.ws, self.root, self.target)
        blob = self.ws.files.uploaded[f"{self.target}/data/seed.parquet"]
        # The literal token inside the binary must survive untouched.
        self.assertEqual(blob, self.files[f"{self.root}/data/seed.parquet"])

    def test_trailing_slash_on_source_is_tolerated(self):
        staged = stage_conf.stage_conf_tree(self.ws, self.root + "/", self.target)
        self.assertEqual(staged, 4)


class EnsureVolumeTests(unittest.TestCase):
    def test_ensure_volume_creates_schema_and_volume(self):
        ws = _FakeWorkspaceClient({})
        stage_conf.ensure_volume(ws, "main", "sdp_meta", "bundle_conf")
        self.assertEqual(len(ws.schemas.created), 1)
        self.assertEqual(len(ws.volumes.created), 1)
        self.assertEqual(ws.volumes.created[0]["name"], "bundle_conf")

    def test_ensure_volume_tolerates_already_exists(self):
        ws = _FakeWorkspaceClient({}, raise_on_create=True)
        # Must not raise even when schema/volume already exist.
        stage_conf.ensure_volume(ws, "main", "sdp_meta", "bundle_conf")


class StageConfMainTests(unittest.TestCase):
    def test_main_stages_configuration_with_workspace_client(self):
        ws = MagicMock()
        argv = [
            "--source_conf_dir=/Workspace/Users/me/files/conf",
            "--uc_catalog=main",
            "--uc_schema=sdp_meta",
            "--conf_volume=bundle_conf",
        ]
        with (
            patch("databricks.sdk.WorkspaceClient", return_value=ws),
            patch("databricks.labs.sdp_meta.stage_conf.ensure_volume") as ensure,
            patch(
                "databricks.labs.sdp_meta.stage_conf.stage_conf_tree",
                return_value=3,
            ) as stage,
        ):
            stage_conf.main(argv)

        ensure.assert_called_once_with(ws, "main", "sdp_meta", "bundle_conf")
        stage.assert_called_once_with(
            ws,
            "/Workspace/Users/me/files/conf",
            "/Volumes/main/sdp_meta/bundle_conf/conf",
        )


if __name__ == "__main__":
    unittest.main()
