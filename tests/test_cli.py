import unittest
from unittest.mock import MagicMock, patch
from src.__about__ import __version__
from src.cli import DLTMeta, OnboardCommand, DeployCommand, DLT_META_RUNNER_NOTEBOOK


class CliTests(unittest.TestCase):
    onboarding_file_path = "tests/resources/onboarding.json"
    onboard_cmd = OnboardCommand(
        dbr_version="15.3",
        dbfs_path="/dbfs",
        onboarding_file_path=onboarding_file_path,
        onboarding_files_dir_path="tests/resources/",
        onboard_layer="bronze",
        env="dev",
        import_author="John Doe",
        version="1.0",
        cloud="aws",
        dlt_meta_schema="dlt_meta",
        bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
        silver_dataflowspec_path="tests/resources/silver_dataflowspec",
        uc_enabled=True,
        uc_catalog_name="uc_catalog",
        overwrite=True,
        bronze_dataflowspec_table="bronze_dataflowspec",
        silver_dataflowspec_table="silver_dataflowspec",
        update_paths=True
    )
    deploy_cmd = DeployCommand(
        layer="bronze",
        onboard_group="A1",
        dlt_meta_schema="dlt_meta",
        pipeline_name="unittest_dlt_pipeline",
        dataflowspec_table="dataflowspec_table",
        dlt_target_schema="dlt_target_schema",
        num_workers=1,
        uc_catalog_name="uc_catalog",
        dataflowspec_path="tests/resources/dataflowspec",
        uc_enabled=True,
        serverless=False,
        dbfs_path="/dbfs",
    )

    def test_copy_to_uc_volume(self):
        mock_ws = MagicMock()
        dltmeta = DLTMeta(mock_ws)
        with patch('os.walk') as mock_walk:
            mock_walk.return_value = [
                ("/path/to/src", [], ["file1.txt", "file2.txt"]),
                ("/path/to/src/subdir", [], ["file3.txt"])
            ]
            with patch('builtins.open') as mock_open:
                mock_open.return_value = MagicMock()
                mock_dbfs_upload = MagicMock()
                mock_ws.dbfs.upload = mock_dbfs_upload
                dltmeta.test_copy_to_uc_volume("file:/path/to/src", "/dbfs/path/to/dst")
                self.assertEqual(mock_dbfs_upload.call_count, 3)

    @patch('src.cli.WorkspaceClient')
    def test_onboard(self, mock_workspace_client):
        mock_dbfs = MagicMock()
        mock_jobs = MagicMock()
        mock_workspace_client.dbfs = mock_dbfs
        mock_workspace_client.jobs = mock_jobs
        mock_workspace_client.dbfs.exists.return_value = False
        mock_workspace_client.dbfs.mkdirs.return_value = None
        mock_workspace_client.dbfs.upload.return_value = None
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        mock_workspace_client.jobs.run_now.return_value = MagicMock(run_id="run_id")

        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        with patch.object(dltmeta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            dltmeta.onboard(self.onboard_cmd)

        mock_workspace_client.dbfs.exists.assert_called_once_with('/dbfs/dltmeta_conf/')
        mock_workspace_client.dbfs.mkdirs.assert_called_once_with("/dbfs/dltmeta_conf/")
        mock_workspace_client.jobs.create.assert_called_once()
        mock_workspace_client.jobs.run_now.assert_called_once_with(job_id="job_id")

    @patch('src.cli.WorkspaceClient')
    def test_create_onnboarding_job(self, mock_workspace_client):

        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        with patch.object(dltmeta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            job = dltmeta.create_onnboarding_job(self.onboard_cmd)

        mock_workspace_client.jobs.create.assert_called_once()
        self.assertEqual(job.job_id, "job_id")

    @patch('src.cli.WorkspaceClient')
    def test_install_folder(self, mock_workspace_client):
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta._install_folder = MagicMock(return_value="/Users/name/dlt-meta")
        folder = dltmeta._install_folder()
        self.assertEqual(folder, "/Users/name/dlt-meta")

    @patch('src.cli.WorkspaceClient')
    def test_create_dlt_meta_pipeline(self, mock_workspace_client):
        mock_workspace_client.pipelines.create.return_value = MagicMock(pipeline_id="pipeline_id")
        mock_workspace_client.workspace.mkdirs.return_value = None
        mock_workspace_client.workspace.upload.return_value = None
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta._wsi._upload_wheel.return_value = None
        dltmeta._my_username = MagicMock(return_value="name")
        dltmeta._create_dlt_meta_pipeline(self.deploy_cmd)
        runner_notebook_py = DLT_META_RUNNER_NOTEBOOK.format(version=__version__).encode("utf8")
        runner_notebook_path = f"{dltmeta._install_folder()}/init_dlt_meta_pipeline.py"
        mock_workspace_client.workspace.mkdirs.assert_called_once_with("/Users/name/dlt-meta")
        mock_workspace_client.workspace.upload.assert_called_once_with(runner_notebook_path,
                                                                       runner_notebook_py,
                                                                       overwrite=True)
        mock_workspace_client.pipelines.create.assert_called_once()

        mock_workspace_client.pipelines.create.assert_called_once()

    def test_get_onboarding_named_parameters(self):
        cmd = OnboardCommand(
            dbr_version="7.3",
            dbfs_path="/dbfs",
            onboarding_file_path="tests/resources/onboarding.json",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            dlt_meta_schema="dlt_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True
        )
        dltmeta = DLTMeta(None)
        named_parameters = dltmeta._get_onboarding_named_parameters(cmd, "onboarding.json")
        expected_named_parameters = {
            "onboard_layer": "bronze",
            "database": "uc_catalog.dlt_meta" if cmd.uc_enabled else "dlt_meta",
            "onboarding_file_path": "/dbfs/dltmeta_conf/onboarding.json",
            "import_author": "John Doe",
            "version": "1.0",
            "overwrite": "True",
            "env": "dev",
            "uc_enabled": "True",
            "bronze_dataflowspec_table": "bronze_dataflowspec"
        }
        self.assertEqual(named_parameters, expected_named_parameters)
