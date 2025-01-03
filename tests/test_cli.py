import unittest
import os
from unittest.mock import MagicMock, patch, mock_open
import json
from databricks.sdk.service.catalog import VolumeType
from src.__about__ import __version__
from src.cli import DLT_META_RUNNER_NOTEBOOK, DeployCommand, DLTMeta, OnboardCommand, main


class CliTests(unittest.TestCase):
    onboarding_file_path = "tests/resources/onboarding.json"
    onboard_cmd_with_uc = OnboardCommand(
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
        uc_volume_path="uc_catalog/dlt_meta/files",
        overwrite=True,
        bronze_dataflowspec_table="bronze_dataflowspec",
        silver_dataflowspec_table="silver_dataflowspec",
        update_paths=True,
    )

    onboard_cmd_without_uc = OnboardCommand(
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
        uc_enabled=False,
        dbfs_path="/dbfs",
        overwrite=True,
        bronze_dataflowspec_table="bronze_dataflowspec",
        silver_dataflowspec_table="silver_dataflowspec",
        update_paths=True,
    )

    deploy_cmd = DeployCommand(
        layer="bronze_silver",
        onboard_bronze_group="A1",
        onboard_silver_group="A1",
        dlt_meta_bronze_schema="dlt_bronze_schema",
        dlt_meta_silver_schema="dlt_silver_schema",
        dataflowspec_bronze_table="bronze_dataflowspec_table",
        dataflowspec_silver_table="silver_dataflowspec_table",
        num_workers=1,
        uc_catalog_name="uc_catalog",
        pipeline_name="unittest_dlt_pipeline",
        dlt_target_schema="dlt_target_schema",
        uc_enabled=True,
        serverless=False,
        dbfs_path="/dbfs",
    )

    def test_copy_to_dbfs(self):
        mock_ws = MagicMock()
        dltmeta = DLTMeta(mock_ws)
        with patch("os.walk") as mock_walk:
            mock_walk.return_value = [
                ("/path/to/src", [], ["file1.txt", "file2.txt"]),
                ("/path/to/src/subdir", [], ["file3.txt"]),
            ]
            with patch("builtins.open") as mock_open:
                mock_open.return_value = MagicMock()
                mock_dbfs_upload = MagicMock()
                mock_ws.dbfs.upload = mock_dbfs_upload
                dltmeta.copy_to_dbfs("file:/path/to/src", "/dbfs/path/to/dst")
                self.assertEqual(mock_dbfs_upload.call_count, 3)

    @patch("src.cli.WorkspaceClient")
    @patch("builtins.open", new_callable=MagicMock)
    def test_onboard_with_uc(self, mock_open, mock_workspace_client):
        mock_jobs = MagicMock()
        mock_open.return_value = MagicMock()
        mock_workspace_client.jobs = mock_jobs
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        mock_workspace_client.jobs.run_now.return_value = MagicMock(run_id="run_id")
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta.update_ws_onboarding_paths = MagicMock()
        dltmeta.create_uc_schema = MagicMock()
        dltmeta.create_uc_volume = MagicMock()
        dltmeta.copy_to_uc_volume = MagicMock()
        with patch.object(dltmeta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            dltmeta.onboard(self.onboard_cmd_with_uc)
        dltmeta.create_uc_volume.assert_called_once_with(
            self.onboard_cmd_with_uc.uc_catalog_name,
            self.onboard_cmd_with_uc.dlt_meta_schema
        )
        dltmeta.create_uc_schema.assert_called_once_with(
            self.onboard_cmd_with_uc.uc_catalog_name,
            self.onboard_cmd_with_uc.dlt_meta_schema
        )
        mock_workspace_client.jobs.create.assert_called_once()
        mock_workspace_client.jobs.run_now.assert_called_once_with(job_id="job_id")

    @patch("src.cli.WorkspaceClient")
    @patch("builtins.open", new_callable=MagicMock)
    def test_onboard_without_uc(self, mock_open, mock_workspace_client):
        mock_dbfs = MagicMock()
        mock_jobs = MagicMock()
        mock_open.return_value = MagicMock()
        mock_workspace_client.dbfs = mock_dbfs
        mock_workspace_client.jobs = mock_jobs
        mock_workspace_client.dbfs.mkdirs.return_value = None
        mock_workspace_client.dbfs.upload.return_value = None
        mock_copy_to_dbfs = MagicMock()
        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        mock_workspace_client.jobs.run_now.return_value = MagicMock(run_id="run_id")

        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta.copy_to_dbfs = mock_copy_to_dbfs.return_value
        dltmeta.update_ws_onboarding_paths = MagicMock()
        with patch.object(dltmeta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            dltmeta.onboard(self.onboard_cmd_without_uc)
        mock_workspace_client.dbfs.mkdirs.assert_called_once_with("/dbfs/dltmeta_conf/")
        mock_workspace_client.dbfs.upload.assert_called_with(
            "/dbfs/dltmeta_conf/onboarding.json",
            mock_open.return_value,
            overwrite=True
        )
        mock_workspace_client.jobs.create.assert_called_once()
        mock_workspace_client.jobs.run_now.assert_called_once_with(job_id="job_id")

    @patch("src.cli.WorkspaceClient")
    def test_create_onnboarding_job(self, mock_workspace_client):

        mock_workspace_client.jobs.create.return_value = MagicMock(job_id="job_id")
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        with patch.object(dltmeta._wsi, "_upload_wheel", return_value="/path/to/wheel"):
            job = dltmeta.create_onnboarding_job(self.onboard_cmd_with_uc)

        mock_workspace_client.jobs.create.assert_called_once()
        self.assertEqual(job.job_id, "job_id")

    @patch("src.cli.WorkspaceClient")
    def test_install_folder(self, mock_workspace_client):
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta._install_folder = MagicMock(return_value="/Users/name/dlt-meta")
        folder = dltmeta._install_folder()
        self.assertEqual(folder, "/Users/name/dlt-meta")

    @patch("src.cli.WorkspaceClient")
    def test_create_dlt_meta_pipeline(self, mock_workspace_client):
        mock_workspace_client.pipelines.create.return_value = MagicMock(
            pipeline_id="pipeline_id"
        )
        mock_workspace_client.workspace.mkdirs.return_value = None
        mock_workspace_client.workspace.upload.return_value = None
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta._wsi._upload_wheel.return_value = None
        dltmeta._my_username = MagicMock(return_value="name")
        dltmeta._create_dlt_meta_pipeline(self.deploy_cmd)
        runner_notebook_py = DLT_META_RUNNER_NOTEBOOK.format(
            version=__version__
        ).encode("utf8")
        runner_notebook_path = f"{dltmeta._install_folder()}/init_dlt_meta_pipeline.py"
        mock_workspace_client.workspace.mkdirs.assert_called_once_with(
            "/Users/name/dlt-meta"
        )
        mock_workspace_client.workspace.upload.assert_called_once_with(
            runner_notebook_path, runner_notebook_py, overwrite=True
        )
        mock_workspace_client.pipelines.create.assert_called_once()

        mock_workspace_client.pipelines.create.assert_called_once()

    def test_get_onboarding_named_parameters(self):
        cmd = OnboardCommand(
            onboarding_file_path="tests/resources/onboarding.json",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze_silver",
            env="dev",
            import_author="Ravi Gawai",
            version="1.0",
            dlt_meta_schema="dlt_meta",
            bronze_dataflowspec_path="tests/resources/bronze_dataflowspec",
            silver_dataflowspec_path="tests/resources/silver_dataflowspec",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="uc_catalog/dlt_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True,
        )
        dltmeta = DLTMeta(None)
        named_parameters = dltmeta._get_onboarding_named_parameters(
            cmd
        )
        expected_named_parameters = {
            "onboard_layer": "bronze_silver",
            "database": "uc_catalog.dlt_meta" if cmd.uc_enabled else "dlt_meta",
            "onboarding_file_path": "uc_catalog/dlt_meta/files/dltmeta_conf/tests/resources/onboarding.json",
            "import_author": "Ravi Gawai",
            "version": "1.0",
            "overwrite": "True",
            "env": "dev",
            "uc_enabled": "True",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "silver_dataflowspec_table": "silver_dataflowspec",
        }
        self.assertEqual(named_parameters, expected_named_parameters)

    @patch("src.cli.WorkspaceClient")
    def test_create_uc_volume(self, mock_workspace_client):
        mock_volumes_create = MagicMock()
        mock_workspace_client.volumes.create = mock_volumes_create
        mock_volumes_create.return_value = MagicMock(
            catalog_name="uc_catalog",
            schema_name="dlt_meta",
            name="dlt_meta"
        )
        dltmeta = DLTMeta(mock_workspace_client)
        volume_path = dltmeta.create_uc_volume("uc_catalog", "dlt_meta")
        self.assertEqual(
            volume_path,
            f"/Volumes/{mock_volumes_create.return_value.catalog_name}/"
            f"{mock_volumes_create.return_value.schema_name}/"
            f"{mock_volumes_create.return_value.name}/"
        )
        mock_volumes_create.assert_called_once_with(
            catalog_name="uc_catalog",
            schema_name="dlt_meta",
            name="dlt_meta",
            volume_type=VolumeType.MANAGED
        )

    @patch("src.cli.SchemasAPI")
    @patch("src.cli.WorkspaceClient")
    def test_create_uc_schema(self, mock_workspace_client, mock_schemas_api):
        mock_schemas_api_instance = mock_schemas_api.return_value
        mock_schemas_api_instance.get.side_effect = Exception("Schema not found")
        mock_schemas_api_instance.create.return_value = None

        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta.create_uc_schema("uc_catalog", "dlt_meta")

        mock_schemas_api_instance.get.assert_called_once_with(full_name="uc_catalog.dlt_meta")
        mock_schemas_api_instance.create.assert_called_once_with(
            catalog_name="uc_catalog",
            name="dlt_meta",
            comment="dlt_meta framework schema"
        )

    @patch("src.cli.SchemasAPI")
    @patch("src.cli.WorkspaceClient")
    def test_create_uc_schema_already_exists(self, mock_workspace_client, mock_schemas_api):
        mock_schemas_api_instance = mock_schemas_api.return_value
        mock_schemas_api_instance.get.return_value = None

        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta.create_uc_schema("uc_catalog", "dlt_meta")

        mock_schemas_api_instance.get.assert_called_once_with(full_name="uc_catalog.dlt_meta")
        mock_schemas_api_instance.create.assert_not_called()

    @patch("src.cli.WorkspaceClient")
    def test_deploy(self, mock_workspace_client):
        mock_pipelines_create = MagicMock()
        mock_pipelines_start_update = MagicMock()
        mock_workspace_client.pipelines.create = mock_pipelines_create
        mock_workspace_client.pipelines.start_update = mock_pipelines_start_update
        mock_pipelines_create.return_value = MagicMock(pipeline_id="pipeline_id")
        mock_pipelines_start_update.return_value = MagicMock(update_id="update_id")

        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta._install_folder = MagicMock(return_value="/Users/name/dlt-meta")
        dltmeta._my_username = MagicMock(return_value="name")

        dltmeta._create_dlt_meta_pipeline = MagicMock(return_value="pipeline_id")

        deploy_cmd = DeployCommand(
            layer="bronze",
            onboard_bronze_group="A1",
            dlt_meta_bronze_schema="dlt_meta",
            pipeline_name="unittest_dlt_pipeline",
            dataflowspec_bronze_table="dataflowspec_table",
            dlt_target_schema="dlt_target_schema",
            num_workers=1,
            uc_catalog_name="uc_catalog",
            dataflowspec_bronze_path="tests/resources/dataflowspec",
            uc_enabled=True,
            serverless=False,
            dbfs_path="/dbfs",
        )

        dltmeta.deploy(deploy_cmd)

        dltmeta._create_dlt_meta_pipeline.assert_called_once_with(deploy_cmd)
        mock_pipelines_start_update.assert_called_once_with(pipeline_id="pipeline_id")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_onboard_config(self, mock_workspace_client, mock_workspace_installer):
        mock_ws_installer = mock_workspace_installer.return_value
        mock_ws_installer._choice.side_effect = ['True', 'True', 'bronze_silver', 'False', 'True', 'False']
        mock_ws_installer._question.side_effect = [
            "uc_catalog", "demo/conf/onboarding.template",
            "file:/demo/", "dlt_meta_dataflowspecs", "dltmeta_bronze", "dltmeta_silver",
            "bronze_dataflowspec", "silver_dataflowspec", "v1", "prod", "author", "True"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        cmd = dltmeta._load_onboard_config()

        self.assertTrue(cmd.uc_enabled)
        self.assertEqual(cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(cmd.dbfs_path, None)
        self.assertEqual(cmd.onboarding_file_path, "demo/conf/onboarding.template")
        self.assertEqual(cmd.onboarding_files_dir_path, "file:/file:/demo/")
        self.assertEqual(cmd.dlt_meta_schema, "dlt_meta_dataflowspecs")
        self.assertEqual(cmd.bronze_schema, "dltmeta_bronze")
        self.assertEqual(cmd.silver_schema, "dltmeta_silver")
        self.assertEqual(cmd.onboard_layer, "bronze_silver")
        self.assertEqual(cmd.bronze_dataflowspec_table, "bronze_dataflowspec")
        self.assertEqual(cmd.bronze_dataflowspec_path, None)
        self.assertEqual(cmd.silver_dataflowspec_table, "silver_dataflowspec")
        self.assertEqual(cmd.silver_dataflowspec_path, None)
        self.assertEqual(cmd.version, "v1")
        self.assertEqual(cmd.env, "prod")
        self.assertEqual(cmd.import_author, "author")
        self.assertTrue(cmd.update_paths)

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_onboard_config_without_uc(self, mock_workspace_client, mock_workspace_installer):
        mock_ws_installer = mock_workspace_installer.return_value
        mock_ws_installer._choice.side_effect = ['False', 'False', 'aws',
                                                 'bronze_silver', 'False', 'True', 'False']
        mock_ws_installer._question.side_effect = [
            'dbfs_path', "dbrx", "demo/conf/onboarding.template",
            "file:/demo/", "dlt_meta_dataflowspecs", "dltmeta_bronze",
            "dltmeta_silver", "bronze_dataflowspec_table",
            "bronze_dataflowspec_path", "silver_dataflowspec_table",
            "silver_dataflowspec_path", "v1", "prod", "author", "True"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        cmd = dltmeta._load_onboard_config()

        self.assertFalse(cmd.uc_enabled)
        self.assertFalse(cmd.serverless)
        self.assertEqual(cmd.dbfs_path, "dbfs_path")
        self.assertEqual(cmd.onboarding_file_path, "demo/conf/onboarding.template")
        self.assertEqual(cmd.onboarding_files_dir_path, "file:/file:/demo/")
        self.assertEqual(cmd.dlt_meta_schema, "dlt_meta_dataflowspecs")
        self.assertEqual(cmd.bronze_schema, "dltmeta_bronze")
        self.assertEqual(cmd.silver_schema, "dltmeta_silver")
        self.assertEqual(cmd.onboard_layer, "bronze_silver")
        self.assertEqual(cmd.bronze_dataflowspec_table, "bronze_dataflowspec_table")
        self.assertEqual(cmd.silver_dataflowspec_table, "silver_dataflowspec_table")
        self.assertEqual(cmd.bronze_dataflowspec_path, "bronze_dataflowspec_path")
        self.assertEqual(cmd.silver_dataflowspec_path, "silver_dataflowspec_path")
        self.assertEqual(cmd.version, "v1")
        self.assertEqual(cmd.env, "prod")
        self.assertEqual(cmd.import_author, "author")
        self.assertTrue(cmd.update_paths)

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_with_uc_enabled(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "dlt_meta_schema", "bronze_dataflowspec",
            "group", "dlt_meta_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.dlt_meta_silver_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.num_workers, None)
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_without_uc_enabled(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "False", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "group", "dlt_meta_schema", "bronze_dataflowspec",
            "dataflowspec_path", 4, "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._install_folder = MagicMock(return_value="/Users/name/dlt-meta")
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()

        self.assertFalse(deploy_cmd.uc_enabled)
        self.assertFalse(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_path, "dataflowspec_path")
        self.assertEqual(deploy_cmd.num_workers, 4)
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceClient")
    @patch("src.cli.DLTMeta")
    @patch("src.cli.json.loads")
    def test_main_onboard(self, mock_json_loads, mock_dltmeta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "onboard",
            "flags": {"log_level": "info"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("src.cli.onboard"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='dlt-meta', product_version=__version__)
            mock_dltmeta.assert_called_once_with(mock_ws_instance)

    @patch("src.cli.WorkspaceClient")
    @patch("src.cli.DLTMeta")
    @patch("src.cli.json.loads")
    def test_main_deploy(self, mock_json_loads, mock_dltmeta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "deploy",
            "flags": {"log_level": "info"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("src.cli.deploy"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='dlt-meta', product_version=__version__)
            mock_dltmeta.assert_called_once_with(mock_ws_instance)

    @patch("src.cli.json.loads")
    def test_main_invalid_command(self, mock_json_loads):
        mock_json_loads.return_value = {
            "command": "invalid_command",
            "flags": {"log_level": "info"}
        }
        with self.assertRaises(KeyError):
            main("{}")

    @patch("src.cli.WorkspaceClient")
    @patch("src.cli.DLTMeta")
    @patch("src.cli.json.loads")
    def test_main_log_level_disabled(self, mock_json_loads, mock_dltmeta, mock_workspace_client):
        mock_json_loads.return_value = {
            "command": "onboard",
            "flags": {"log_level": "disabled"}
        }
        mock_ws_instance = mock_workspace_client.return_value

        with patch("src.cli.onboard"):
            main("{}")
            mock_workspace_client.assert_called_once_with(product='dlt-meta', product_version=__version__)
            mock_dltmeta.assert_called_once_with(mock_ws_instance)

    @patch("src.cli.WorkspaceClient")
    def test_update_ws_onboarding_paths_with_uc_enabled(self, mock_workspace_client):
        cmd = OnboardCommand(
            onboarding_file_path="tests/resources/template/onboarding.template",
            onboarding_files_dir_path="tests/resources/",
            onboard_layer="bronze",
            env="dev",
            import_author="John Doe",
            version="1.0",
            cloud="aws",
            dlt_meta_schema="dlt_meta",
            uc_enabled=True,
            uc_catalog_name="uc_catalog",
            uc_volume_path="uc_catalog/dlt_meta/files",
            overwrite=True,
            bronze_dataflowspec_table="bronze_dataflowspec",
            silver_dataflowspec_table="silver_dataflowspec",
            update_paths=True,
        )
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_client.return_value
        dltmeta.update_ws_onboarding_paths(cmd)
        check_file = os.path.exists("tests/resources/template/onboarding.json")
        self.assertEqual(check_file, True)
        os.remove("tests/resources/template/onboarding.json")

    @patch("src.cli.WorkspaceClient")
    def test_my_username(self, mock_workspace_client):
        mock_workspace_client.current_user.me.return_value = MagicMock(user_name="test_user")
        mock_workspace_client.current_user.me.return_value.user_name = "test_user"
        mock_workspace_client._me.return_value = MagicMock(user_name="test_user")
        dltmeta = DLTMeta(mock_workspace_client)
        username = dltmeta._my_username()
        self.assertEqual(username, mock_workspace_client._me.user_name)

    def test_onboard_command_post_init(self):
        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="invalid_layer",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path=None,
                uc_enabled=False,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                serverless=False,
                cloud=None,
                dbr_version=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                bronze_dataflowspec_path=None,
                silver_dataflowspec_path=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                uc_enabled=False,
                silver_dataflowspec_path=None,
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema=None,
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=False,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author=None,
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env=None,
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                bronze_dataflowspec_path="tests/resources/bronze_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="bronze_silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                silver_dataflowspec_path="tests/resources/silver_dataflowspec"
            )
        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                env="dev",
                import_author="John Doe",
                version="1.0",
                dlt_meta_schema="dlt_meta",
                dbfs_path="/dbfs",
                overwrite=True,
                serverless=False,
                cloud="aws",
                dbr_version="7.3",
                uc_enabled=False,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                dlt_meta_schema=None,
                env="dev",
                import_author="John Doe",
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )
        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                dlt_meta_schema="dlt_meta",
                env="dev",
                import_author="John Doe",
                version="1.0",
                overwrite=None,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                dlt_meta_schema="dlt_meta",
                env="dev",
                import_author=None,
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

        with self.assertRaises(ValueError):
            OnboardCommand(
                onboarding_file_path="tests/resources/onboarding.json",
                onboarding_files_dir_path="tests/resources/",
                onboard_layer="silver",
                dlt_meta_schema="dlt_meta",
                env=None,
                import_author="author",
                version="1.0",
                overwrite=True,
                serverless=True,
                uc_enabled=True,
                silver_dataflowspec_table="silver_dataflowspec"
            )

    def test_deploy_command_post_init(self):
        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                uc_enabled=True,
                uc_catalog_name=None,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                serverless=False,
                num_workers=None,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer=None,
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group=None,
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table=None,
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name=None,
                dlt_target_schema="dlt_target_schema",
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema=None,
            )

    def test_deploy_command_post_init_additional(self):
        with self.assertRaises(ValueError):
            DeployCommand(
                layer="",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="",
                dlt_target_schema="dlt_target_schema",
                num_workers=1,
            )

        with self.assertRaises(ValueError):
            DeployCommand(
                layer="bronze",
                onboard_bronze_group="A1",
                dlt_meta_bronze_schema="dlt_meta",
                dataflowspec_bronze_table="dataflowspec_table",
                pipeline_name="unittest_dlt_pipeline",
                dlt_target_schema="",
                num_workers=1,
            )

    @patch("src.cli.DLTMeta._install_folder", return_value="/Users/test/dlt-meta")
    @patch("src.cli.WorkspaceClient")
    def test_create_dlt_meta_pipeline_with_uc_enabled(self, mock_workspace_client, mock_install_folder):
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta.version = "1.2.3"
        cmd = DeployCommand(
            layer="bronze",
            onboard_bronze_group="groupA",
            dlt_meta_bronze_schema="schemaA",
            dataflowspec_bronze_table="tableA",
            pipeline_name="my_pipeline",
            dlt_target_schema="my_dlt_schema",
            uc_enabled=True,
            uc_catalog_name="my_catalog",
            serverless=True,
            num_workers=None,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "12345"
        mock_workspace_client.pipelines.create.return_value = mock_created

        pipeline_id = dltmeta._create_dlt_meta_pipeline(cmd)
        self.assertEqual(pipeline_id, "12345")
        mock_workspace_client.pipelines.create.assert_called_once()

    @patch("src.cli.DLTMeta._install_folder", return_value="/Users/test/dlt-meta")
    @patch("src.cli.WorkspaceClient")
    def test_create_dlt_meta_pipeline_without_uc_enabled(self, mock_workspace_client, mock_install_folder):
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta.version = "0.9.1"
        cmd = DeployCommand(
            layer="silver",
            onboard_silver_group="groupB",
            dlt_meta_silver_schema="schemaB",
            dataflowspec_silver_table="tableB",
            pipeline_name="silver_pipeline",
            dlt_target_schema="silver_target_schema",
            dataflowspec_silver_path="tests/resources/silver_dataflowspec",
            uc_enabled=False,
            uc_catalog_name=None,
            serverless=False,
            num_workers=5,
        )
        mock_created = MagicMock()
        mock_created.pipeline_id = "98765"
        mock_workspace_client.pipelines.create.return_value = mock_created

        pipeline_id = dltmeta._create_dlt_meta_pipeline(cmd)
        self.assertEqual(pipeline_id, "98765")
        mock_workspace_client.pipelines.create.assert_called_once()

    @patch("src.cli.DLTMeta._install_folder", return_value="/Users/test/dlt-meta")
    @patch("src.cli.WorkspaceClient")
    def test_create_dlt_meta_pipeline_invalid_layer_raises_value_error(
        self, mock_workspace_client, mock_install_folder
    ):
        dltmeta = DLTMeta(mock_workspace_client)
        cmd = DeployCommand(
            layer="invalid",
            serverless=True,
            onboard_bronze_group="group",
            dlt_meta_bronze_schema="schema",
            dataflowspec_bronze_table="table",
            pipeline_name="test_pipeline",
            dlt_target_schema="target_schema",
        )
        with self.assertRaises(ValueError):
            dltmeta._create_dlt_meta_pipeline(cmd)

    @patch("src.cli.DLTMeta._install_folder", return_value="/Users/test/dlt-meta")
    @patch("src.cli.WorkspaceClient")
    def test_create_dlt_meta_pipeline_raise_exception_on_no_creation(self, mock_workspace_client, mock_install_folder):
        dltmeta = DLTMeta(mock_workspace_client)
        cmd = DeployCommand(
            layer="bronze",
            serverless=True,
            uc_enabled=True,
            uc_catalog_name="catalog",
            onboard_bronze_group="group",
            dlt_meta_bronze_schema="schema",
            dataflowspec_bronze_table="table",
            pipeline_name="test_pipeline",
            dlt_target_schema="target_schema",
        )
        mock_workspace_client.pipelines.create.return_value = None
        with self.assertRaises(Exception):
            dltmeta._create_dlt_meta_pipeline(cmd)

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_with_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "True", "True", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "pipeline_name", "dlt_target_schema"
        ]
        oc_job_details_json = {
            "dlt_meta_schema": "dlt_meta_schema",
            "bronze_dataflowspec_table": "bronze_dataflowspec_table",
            "bronze_dataflowspec_path": "bronze_dataflowspec_path"
        }
        with patch("builtins.open", mock_open(read_data=json.dumps(oc_job_details_json))):
            dltmeta = DLTMeta(mock_workspace_client)
            dltmeta._wsi = mock_workspace_installer
            deploy_cmd = dltmeta._load_deploy_config()
        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec_table")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_nouc_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "False", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "bronze_group", "silver_group", "4", "pipeline_name",
            "dlt_target_schema"
        ]
        oc_job_details_json = {
            "dlt_meta_schema": "dlt_meta_schema",
            "bronze_dataflowspec_path": "bronze_dataflowspec_path",
            "silver_dataflowspec_path": "silver_dataflowspec_path"
        }
        with patch("builtins.open", mock_open(read_data=json.dumps(oc_job_details_json))):
            dltmeta = DLTMeta(mock_workspace_client)
            dltmeta._wsi = mock_workspace_installer
            deploy_cmd = dltmeta._load_deploy_config()
        self.assertFalse(deploy_cmd.uc_enabled)
        self.assertFalse(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_path, "bronze_dataflowspec_path")
        self.assertEqual(deploy_cmd.dataflowspec_silver_path, "silver_dataflowspec_path")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_without_json(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "dlt_meta_schema", "bronze_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_with_silver_layer(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "group", "dlt_meta_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "silver")
        self.assertEqual(deploy_cmd.onboard_silver_group, "group")
        self.assertEqual(deploy_cmd.dlt_meta_silver_schema, "dlt_meta_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_with_bronze_silver_layer(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["No", "True", "True", "bronze_silver"]
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "bronze_group", "dlt_meta_bronze_schema", "bronze_dataflowspec",
            "silver_group", "dlt_meta_silver_schema", "silver_dataflowspec",
            "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()

        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_bronze_schema")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.dlt_meta_silver_schema, "dlt_meta_silver_schema")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("src.cli.WorkspaceInstaller")
    @patch("src.cli.WorkspaceClient")
    def test_load_deploy_config_from_json_file(self, mock_workspace_client, mock_workspace_installer):
        mock_workspace_installer._choice.side_effect = ["Yes", "True", "True", "bronze_silver"]
        oc_job_details_json = "tests/resources/onboarding_job_details.json"
        import shutil
        shutil.copyfile(oc_job_details_json, "onboarding_job_details.json")
        mock_workspace_installer._question.side_effect = [
            "uc_catalog", "bronze_group", "silver_group",
            "pipeline_name", "dlt_target_schema"
        ]
        dltmeta = DLTMeta(mock_workspace_client)
        dltmeta._wsi = mock_workspace_installer
        deploy_cmd = dltmeta._load_deploy_config()
        self.assertTrue(deploy_cmd.uc_enabled)
        self.assertTrue(deploy_cmd.serverless)
        self.assertEqual(deploy_cmd.uc_catalog_name, "uc_catalog")
        self.assertEqual(deploy_cmd.layer, "bronze_silver")
        self.assertEqual(deploy_cmd.onboard_bronze_group, "bronze_group")
        self.assertEqual(deploy_cmd.onboard_silver_group, "silver_group")
        self.assertEqual(deploy_cmd.dlt_meta_bronze_schema, "dlt_meta_dataflowspecs")
        self.assertEqual(deploy_cmd.dataflowspec_bronze_table, "bronze_dataflowspec")
        self.assertEqual(deploy_cmd.dlt_meta_silver_schema, "dlt_meta_dataflowspecs")
        self.assertEqual(deploy_cmd.dataflowspec_silver_table, "silver_dataflowspec")
        self.assertEqual(deploy_cmd.pipeline_name, "pipeline_name")
        self.assertEqual(deploy_cmd.dlt_target_schema, "dlt_target_schema")

    @patch("os.walk")
    @patch("builtins.open", new_callable=mock_open)
    @patch("src.cli.DLTMeta._my_username", return_value="test_user")
    def test_copy_to_uc_volume(self, mock_my_username, mock_open, mock_os_walk):
        mock_ws = MagicMock()
        dltmeta = DLTMeta(mock_ws)
        mock_os_walk.return_value = [
            ("/path/to/src", [], ["file1.txt", "file2.txt"]),
            ("/path/to/src/subdir", [], ["file3.txt"]),
        ]
        mock_ws.files.upload = MagicMock()
        dltmeta.copy_to_uc_volume("file:/path/to/src", "/uc_volume/path/to/dst")
        expected_calls = [
            ("/uc_volume/path/to/dst/src/file1.txt", mock_open.return_value, True),
            ("/uc_volume/path/to/dst/src/file2.txt", mock_open.return_value, True),
            ("/uc_volume/path/to/dst/src/subdir/file3.txt", mock_open.return_value, True),
        ]
        actual_calls = [
            (call[1]["file_path"], call[1]["contents"], call[1]["overwrite"])
            for call in mock_ws.files.upload.call_args_list
        ]
        self.assertEqual(expected_calls, actual_calls)
        self.assertEqual(mock_ws.files.upload.call_count, 3)
