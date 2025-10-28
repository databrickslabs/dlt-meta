# DLT-META CLI Debugging Guide

## Overview
The `cli.py` file is the main entry point for the DLT-META framework CLI. It handles onboarding and deployment of Delta Live Tables pipelines.

## Debug Methods

### 1. VS Code Debugging (Recommended)

I've added debug configurations to `.vscode/launch.json`. You can now:

1. **Set breakpoints** in `src/cli.py` by clicking in the left margin
2. **Use the Debug panel** (Ctrl+Shift+D) and select one of these configurations:
   - "Debug CLI - Onboard" - Debug the onboarding workflow
   - "Debug CLI - Deploy" - Debug the deployment workflow  
   - "Debug CLI - Test Mode" - Debug without arguments

3. **Key debugging points to set breakpoints:**
   - Line 803: `main()` function entry point
   - Line 783: Command routing in MAPPING
   - Line 211: `onboard()` method
   - Line 429: `deploy()` method
   - Line 155: `DLTMeta.__init__()`

### 2. Environment Setup for Debugging

Before debugging, ensure you have:

```bash
# Set Databricks environment variables
$env:DATABRICKS_HOST = "https://your-workspace.cloud.databricks.com"
$env:DATABRICKS_TOKEN = "your-personal-access-token"

# Or create a .databrickscfg file in your home directory
# [DEFAULT]
# host = https://your-workspace.cloud.databricks.com  
# token = your-personal-access-token
```

### 3. Add Debug Logging

Add strategic logging statements:

```python
import logging
logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.DEBUG)

# Add debug statements like:
logger.debug(f"Processing command: {command}")
logger.debug(f"Command arguments: {payload}")
```

### 4. Unit Testing Approach

Run specific tests:

```bash
# Run all CLI tests
python -m pytest tests/test_cli.py -v

# Run specific test
python -m pytest tests/test_cli.py::CliTests::test_onboard_with_uc -v

# Run with coverage
python -m pytest tests/test_cli.py --cov=src.cli --cov-report=html
```

### 5. Interactive Python Debugging

Use Python's built-in debugger:

```python
import pdb

# Add this line where you want to break
pdb.set_trace()

# Or use the newer breakpoint() function (Python 3.7+)
breakpoint()
```

### 6. Mock Databricks SDK for Local Testing

Create a test script to mock the WorkspaceClient:

```python
from unittest.mock import MagicMock, patch
from src.cli import DLTMeta, OnboardCommand

# Mock the Databricks SDK
with patch('src.cli.WorkspaceClient') as mock_ws:
    mock_ws.return_value = MagicMock()
    
    # Create your test command
    cmd = OnboardCommand(
        onboarding_file_path="demo/conf/onboarding.template",
        onboarding_files_dir_path="demo/",
        onboard_layer="bronze",
        env="dev",
        import_author="test",
        version="1.0",
        dlt_meta_schema="test_schema",
        dbfs_path="/test/path",
        uc_enabled=False,
        overwrite=True
    )
    
    # Test your code
    dltmeta = DLTMeta(mock_ws.return_value)
    # Add breakpoints here to debug
```

### 7. Command Line Testing

Test CLI commands directly:

```bash
# Test the main function
python src/cli.py '{"command": "onboard", "flags": {"log_level": "debug"}}'

# Test with environment variables for easier debugging
$env:DLT_META_DEBUG = "true"
python src/cli.py '{"command": "deploy", "flags": {"log_level": "info"}}'
```

## Common Issues to Debug

### 1. Authentication Issues
- Check `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables
- Verify `.databrickscfg` file format
- Debug at line 794: `WorkspaceClient` initialization

### 2. Configuration Validation
- Set breakpoints in `OnboardCommand.__post_init__()` (line 43)
- Set breakpoints in `DeployCommand.__post_init__()` (line 122)

### 3. File Operations
- Debug file uploads at line 182: `copy_to_dbfs()`
- Debug UC volume operations at line 171: `copy_to_uc_volume()`

### 4. Pipeline Creation
- Debug pipeline creation at line 396: `_create_dlt_meta_pipeline()`
- Check job creation at line 329: `create_onnboarding_job()`

## Debug Flow Diagram

```
main() -> 
  parse JSON payload -> 
  route to command function -> 
  create DLTMeta instance ->
  load configuration ->
  execute operation ->
  handle results
```

## Troubleshooting Tips

1. **Enable verbose logging**: Set log level to DEBUG
2. **Check network connectivity**: Ensure you can reach Databricks workspace
3. **Validate file paths**: Make sure onboarding files exist
4. **Mock external dependencies**: Use unittest.mock for isolated testing
5. **Step through validation**: The `__post_init__` methods have extensive validation

## Example Debug Session

1. Set breakpoint at line 803 in `main()`
2. Run "Debug CLI - Onboard" configuration
3. Step through:
   - JSON parsing
   - Command validation  
   - WorkspaceClient creation
   - Configuration loading
   - File operations
   - Job creation

This will help you understand the complete flow and identify issues.