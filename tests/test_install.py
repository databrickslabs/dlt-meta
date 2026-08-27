"""Test for install module."""
import os
import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute
from databricks.sdk.service.sql import EndpointInfoWarehouseType

from src.install import WorkspaceInstaller
from src.config import WorkspaceConfig


class TestWorkspaceInstaller(unittest.TestCase):
    """Test cases for WorkspaceInstaller."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_ws = MagicMock(spec=WorkspaceClient)
        self.mock_ws.config.is_aws = False
        self.mock_ws.config.is_azure = False

    def test_init_with_databricks_runtime_error(self):
        """Test WorkspaceInstaller initialization in Databricks Runtime."""
        with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "11.3"}):
            with self.assertRaises(SystemExit):
                WorkspaceInstaller(self.mock_ws)

    def test_init_success(self):
        """Test successful WorkspaceInstaller initialization."""
        installer = WorkspaceInstaller(self.mock_ws, prefix="test-prefix", promtps=False)
        self.assertEqual(installer._ws, self.mock_ws)
        self.assertEqual(installer._prefix, "test-prefix")
        self.assertFalse(installer._prompts)
        self.assertEqual(installer._override_clusters, {})
        self.assertEqual(installer._dashboards, {})

    @patch("src.install.logger")
    @patch("src.install.__version__", "1.0.0")
    def test_run(self, mock_logger):
        """Test the run method."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock _configure to avoid real configuration
        with patch.object(installer, '_configure') as mock_configure:
            installer.run()

        mock_logger.info.assert_called_once_with("Installing DLT-META v1.0.0")
        mock_configure.assert_called_once()

    def test_warehouse_id_property_with_configured_warehouse(self):
        """Test _warehouse_id property when warehouse_id is already configured."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_config with warehouse_id
        mock_config = MagicMock()
        mock_config.warehouse_id = "warehouse123"

        # Mock the workspace download
        mock_file_content = b'{"warehouse_id": "warehouse123"}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            warehouse_id = installer._warehouse_id

        self.assertEqual(warehouse_id, "warehouse123")

    def test_warehouse_id_property_without_configured_warehouse(self):
        """Test _warehouse_id property when no warehouse_id is configured."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_config without warehouse_id
        mock_config = MagicMock()
        mock_config.warehouse_id = None

        # Mock warehouses list
        mock_warehouse = MagicMock()
        mock_warehouse.id = "auto_warehouse_123"
        mock_warehouse.warehouse_type = EndpointInfoWarehouseType.PRO
        self.mock_ws.warehouses.list.return_value = [mock_warehouse]

        # Mock the workspace download
        mock_file_content = b'{"warehouse_id": null}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            warehouse_id = installer._warehouse_id

        self.assertEqual(warehouse_id, "auto_warehouse_123")
        self.assertEqual(mock_config.warehouse_id, "auto_warehouse_123")

    def test_warehouse_id_property_no_warehouses_available(self):
        """Test _warehouse_id property when no warehouses are available."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_config without warehouse_id
        mock_config = MagicMock()
        mock_config.warehouse_id = None

        # Mock empty warehouses list
        self.mock_ws.warehouses.list.return_value = []

        # Mock the workspace download
        mock_file_content = b'{"warehouse_id": null}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            with self.assertRaises(ValueError):
                installer._warehouse_id

    def test_my_username_property(self):
        """Test _my_username property."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_user.me()
        mock_user = MagicMock()
        mock_user.user_name = "test@example.com"
        self.mock_ws.current_user.me.return_value = mock_user

        username = installer._my_username
        self.assertEqual(username, "test@example.com")

        # Test caching - should not call again
        username2 = installer._my_username
        self.assertEqual(username2, "test@example.com")
        self.mock_ws.current_user.me.assert_called_once()

    def test_short_name_property_with_email(self):
        """Test _short_name property with email username."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_user.me()
        mock_user = MagicMock()
        mock_user.user_name = "test.user@example.com"
        self.mock_ws.current_user.me.return_value = mock_user

        short_name = installer._short_name
        self.assertEqual(short_name, "test.user")

    def test_short_name_property_without_email(self):
        """Test _short_name property without email username."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_user.me()
        mock_user = MagicMock()
        mock_user.user_name = "testuser"
        mock_user.display_name = "Test User"
        self.mock_ws.current_user.me.return_value = mock_user

        short_name = installer._short_name
        self.assertEqual(short_name, "Test User")

    def test_configure_already_installed(self):
        """Test _configure when DLT META is already installed."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock successful get_status (file exists)
        self.mock_ws.workspace.get_status.return_value = MagicMock()

        with patch("src.install.logger") as mock_logger:
            installer._configure()

        mock_logger.info.assert_called_once_with("DLT META is already installed.")

    def test_configure_not_installed(self):
        """Test _configure when DLT META is not installed."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock DatabricksError for file not found
        error = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
        self.mock_ws.workspace.get_status.side_effect = error

        # Should not raise error for RESOURCE_DOES_NOT_EXIST
        installer._configure()

    def test_configure_other_error(self):
        """Test _configure when other DatabricksError occurs."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock DatabricksError for other error
        error = DatabricksError(error_code="PERMISSION_DENIED")
        self.mock_ws.workspace.get_status.side_effect = error

        # Should re-raise other errors
        with self.assertRaises(DatabricksError):
            installer._configure()

    def test_app_property(self):
        """Test _app property."""
        installer = WorkspaceInstaller(self.mock_ws)
        self.assertEqual(installer._app, "dlt-meta")

    @patch("src.install.__version__", "1.2.3")
    def test_version_property(self):
        """Test _version property."""
        installer = WorkspaceInstaller(self.mock_ws)
        self.assertEqual(installer._version, "1.2.3")

    @patch("subprocess.run")
    @patch("sys.executable", "/usr/bin/python3")
    def test_build_wheel_verbose(self, mock_subprocess):
        """Test _build_wheel method with verbose=True."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock _find_project_root
        mock_project_root = Path("/project/root")

        # Mock wheel file in temp directory
        with patch.object(installer, '_find_project_root', return_value=mock_project_root):
            with patch("pathlib.Path.glob") as mock_glob:
                mock_wheel_path = Path("/tmp/test_wheel-1.0.0-py3-none-any.whl")
                mock_glob.return_value = iter([mock_wheel_path])

                result = installer._build_wheel("/tmp", verbose=True)

        # Verify subprocess call
        expected_cmd = [
            "/usr/bin/python3", "-m", "pip", "wheel", "--no-deps",
            "--wheel-dir", "/tmp", mock_project_root
        ]
        mock_subprocess.assert_called_once_with(expected_cmd, check=True)
        self.assertEqual(result, mock_wheel_path)

    @patch("subprocess.run")
    @patch("sys.executable", "/usr/bin/python3")
    def test_build_wheel_not_verbose(self, mock_subprocess):
        """Test _build_wheel method with verbose=False."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock _find_project_root
        mock_project_root = Path("/project/root")

        # Mock wheel file in temp directory
        with patch.object(installer, '_find_project_root', return_value=mock_project_root):
            with patch("pathlib.Path.glob") as mock_glob:
                mock_wheel_path = Path("/tmp/test_wheel-1.0.0-py3-none-any.whl")
                mock_glob.return_value = iter([mock_wheel_path])

                result = installer._build_wheel("/tmp", verbose=False)

        # Verify subprocess call with redirected output
        expected_cmd = [
            "/usr/bin/python3", "-m", "pip", "wheel", "--no-deps",
            "--wheel-dir", "/tmp", mock_project_root
        ]
        mock_subprocess.assert_called_once_with(
            expected_cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True
        )
        self.assertEqual(result, mock_wheel_path)

    @patch("subprocess.run")
    @patch("shutil.copytree")
    @patch("src.install.__version__", "1.0.0+dev.123")
    def test_build_wheel_dev_version_with_git(self, mock_copytree, mock_subprocess):
        """Test _build_wheel method with development version and git."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock _find_project_root
        mock_project_root = Path("/project/root")

        # Mock git config exists
        with patch.object(installer, '_find_project_root', return_value=mock_project_root):
            with patch("pathlib.Path.exists", return_value=True):  # .git/config exists
                with patch("pathlib.Path.glob") as mock_glob:
                    with patch("pathlib.Path.open", mock_open()) as mock_file:
                        mock_wheel_path = Path("/tmp/test_wheel-1.0.0+dev.123-py3-none-any.whl")
                        mock_glob.return_value = iter([mock_wheel_path])

                        result = installer._build_wheel("/tmp")

        # Verify that copytree was called to create working copy
        mock_copytree.assert_called_once()
        # Verify that version file was written
        mock_file.assert_called()
        self.assertEqual(result, mock_wheel_path)

    def test_find_project_root_with_pyproject_toml(self):
        """Test _find_project_root with pyproject.toml."""
        installer = WorkspaceInstaller(self.mock_ws)

        mock_root = Path("/project/root")
        with patch.object(WorkspaceInstaller, '_find_dir_with_leaf', return_value=mock_root) as mock_find:
            result = installer._find_project_root()

        # Should first try to find pyproject.toml
        mock_find.assert_called_with(installer._this_file, "pyproject.toml")
        self.assertEqual(result, mock_root)

    def test_find_project_root_not_found(self):
        """Test _find_project_root when no project root is found."""
        installer = WorkspaceInstaller(self.mock_ws)

        with patch.object(WorkspaceInstaller, '_find_dir_with_leaf', return_value=None):
            with self.assertRaises(NotADirectoryError):
                installer._find_project_root()

    def test_find_dir_with_leaf_not_found(self):
        """Test _find_dir_with_leaf when leaf is not found."""
        import tempfile

        # Create a temporary directory structure without the target leaf file
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create nested directories
            nested_dir = Path(temp_dir) / "project" / "src" / "subdir"
            nested_dir.mkdir(parents=True, exist_ok=True)

            # Test with a leaf file that doesn't exist anywhere in the hierarchy
            result = WorkspaceInstaller._find_dir_with_leaf(nested_dir, "pyproject.toml")

        self.assertIsNone(result)

    def test_cluster_node_type_with_instance_pool(self):
        """Test _cluster_node_type with instance pool configured."""
        installer = WorkspaceInstaller(self.mock_ws)

        # Mock current_config with instance_pool_id
        mock_config = MagicMock()
        mock_config.instance_pool_id = "pool123"

        spec = compute.ClusterSpec(spark_version="11.3.x-scala2.12")

        # Mock the workspace download
        mock_file_content = b'{"instance_pool_id": "pool123"}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            result = installer._cluster_node_type(spec)

        self.assertEqual(result.instance_pool_id, "pool123")

    def test_cluster_node_type_aws(self):
        """Test _cluster_node_type for AWS."""
        installer = WorkspaceInstaller(self.mock_ws)
        self.mock_ws.config.is_aws = True

        # Mock current_config without instance_pool_id
        mock_config = MagicMock()
        mock_config.instance_pool_id = None

        # Mock select_node_type
        self.mock_ws.clusters.select_node_type.return_value = "i3.xlarge"

        spec = compute.ClusterSpec(spark_version="11.3.x-scala2.12")

        # Mock the workspace download
        mock_file_content = b'{"instance_pool_id": null}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            result = installer._cluster_node_type(spec)

        self.assertEqual(result.node_type_id, "i3.xlarge")
        self.assertEqual(result.aws_attributes.availability, compute.AwsAvailability.ON_DEMAND)

    def test_cluster_node_type_azure(self):
        """Test _cluster_node_type for Azure."""
        installer = WorkspaceInstaller(self.mock_ws)
        self.mock_ws.config.is_azure = True

        # Mock current_config without instance_pool_id
        mock_config = MagicMock()
        mock_config.instance_pool_id = None

        # Mock select_node_type
        self.mock_ws.clusters.select_node_type.return_value = "Standard_DS3_v2"

        spec = compute.ClusterSpec(spark_version="11.3.x-scala2.12")

        # Mock the workspace download
        mock_file_content = b'{"instance_pool_id": null}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            result = installer._cluster_node_type(spec)

        self.assertEqual(result.node_type_id, "Standard_DS3_v2")
        self.assertEqual(result.azure_attributes.availability, compute.AzureAvailability.ON_DEMAND_AZURE)

    def test_cluster_node_type_gcp(self):
        """Test _cluster_node_type for GCP."""
        installer = WorkspaceInstaller(self.mock_ws)
        # Both is_aws and is_azure are False (default), so it's GCP

        # Mock current_config without instance_pool_id
        mock_config = MagicMock()
        mock_config.instance_pool_id = None

        # Mock select_node_type
        self.mock_ws.clusters.select_node_type.return_value = "n1-highmem-4"

        spec = compute.ClusterSpec(spark_version="11.3.x-scala2.12")

        # Mock the workspace download
        mock_file_content = b'{"instance_pool_id": null}'
        mock_file = MagicMock()
        mock_file.read.return_value = mock_file_content
        self.mock_ws.workspace.download.return_value.__enter__.return_value = mock_file

        with patch.object(WorkspaceConfig, 'from_bytes', return_value=mock_config):
            result = installer._cluster_node_type(spec)

        self.assertEqual(result.node_type_id, "n1-highmem-4")
        self.assertEqual(result.gcp_attributes.availability, compute.GcpAvailability.ON_DEMAND_GCP)

    @patch("builtins.input", return_value="test answer")
    def test_question_with_input(self, mock_input):
        """Test _question method with user input."""
        result = WorkspaceInstaller._question("What is your name?")
        self.assertEqual(result, "test answer")

    @patch("builtins.input", return_value="")
    def test_question_with_default(self, mock_input):
        """Test _question method with default value."""
        result = WorkspaceInstaller._question("What is your name?", default="John Doe")
        self.assertEqual(result, "John Doe")

    @patch("builtins.input", side_effect=["", "valid answer"])
    def test_question_empty_then_valid(self, mock_input):
        """Test _question method with empty input then valid input."""
        result = WorkspaceInstaller._question("What is your name?")
        self.assertEqual(result, "valid answer")
        self.assertEqual(mock_input.call_count, 2)

    @patch("builtins.input", return_value="1")
    def test_choice_valid_selection(self, mock_input):
        """Test _choice method with valid selection."""
        installer = WorkspaceInstaller(self.mock_ws, promtps=True)
        choices = ["option1", "option2", "option3"]

        result = installer._choice("Choose an option:", choices)
        self.assertEqual(result, "option2")  # Index 1

    def test_choice_no_prompts(self):
        """Test _choice method when prompts are disabled."""
        installer = WorkspaceInstaller(self.mock_ws, promtps=False)
        choices = ["option1", "option2", "option3"]

        result = installer._choice("Choose an option:", choices)
        self.assertEqual(result, "any")

    @patch("builtins.input", side_effect=["invalid", "99", "-1", "0"])
    @patch("builtins.print")
    def test_choice_invalid_then_valid(self, mock_print, mock_input):
        """Test _choice method with invalid inputs then valid input."""
        installer = WorkspaceInstaller(self.mock_ws, promtps=True)
        choices = ["option1", "option2"]

        result = installer._choice("Choose an option:", choices)
        self.assertEqual(result, "option1")  # Index 0

        # Should print error messages for invalid inputs
        error_calls = [call for call in mock_print.call_args_list if "ERROR" in str(call)]
        self.assertEqual(len(error_calls), 3)  # Three error messages

    @patch("builtins.input", return_value="invalid")
    def test_choice_max_attempts_exceeded(self, mock_input):
        """Test _choice method when max attempts are exceeded."""
        installer = WorkspaceInstaller(self.mock_ws, promtps=True)
        choices = ["option1", "option2"]

        with self.assertRaises(ValueError):
            installer._choice("Choose an option:", choices, max_attempts=3)

    def test_choice_from_dict(self):
        """Test _choice_from_dict method."""
        installer = WorkspaceInstaller(self.mock_ws)
        choices = {"key1": "value1", "key2": "value2"}

        with patch.object(installer, '_choice', return_value="key2") as mock_choice:
            result = installer._choice_from_dict("Choose an option:", choices)

        mock_choice.assert_called_once_with("Choose an option:", ["key1", "key2"])
        self.assertEqual(result, "value2")
