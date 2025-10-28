#!/usr/bin/env python3
"""
Test script to demonstrate CLI JSON payload formats
"""

import json
import subprocess
import sys
from pathlib import Path

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent
CLI_PATH = PROJECT_ROOT / "src" / "cli.py"

def run_cli_command(payload_dict):
    """Run CLI command with JSON payload"""
    json_payload = json.dumps(payload_dict)
    cmd = [sys.executable, str(CLI_PATH), json_payload]
    
    print(f"Running: {' '.join(cmd)}")
    print(f"Payload: {json_payload}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print(f"Exit code: {result.returncode}")
        if result.stdout:
            print(f"STDOUT:\n{result.stdout}")
        if result.stderr:
            print(f"STDERR:\n{result.stderr}")
    except subprocess.TimeoutExpired:
        print("Command timed out")
    except Exception as e:
        print(f"Error running command: {e}")
    
    print("=" * 60)

# Example payloads
def test_onboard_command():
    """Test basic onboard command"""
    payload = {
        "command": "onboard",
        "flags": {
            "log_level": "debug"
        }
    }
    run_cli_command(payload)

def test_deploy_command():
    """Test basic deploy command"""
    payload = {
        "command": "deploy", 
        "flags": {
            "log_level": "info"
        }
    }
    run_cli_command(payload)

def test_invalid_command():
    """Test invalid command to see error handling"""
    payload = {
        "command": "invalid_command",
        "flags": {
            "log_level": "debug"
        }
    }
    run_cli_command(payload)

def test_onboard_ui_command():
    """Test onboard UI command with form data"""
    payload = {
        "command": "onboard_ui",
        "flags": {
            "log_level": "debug"
        },
        # Form data for UI-based onboarding
        "unity_catalog_enabled": "1",
        "unity_catalog_name": "test_catalog",
        "serverless": "1", 
        "onboarding_file_path": "demo/conf/onboarding.template",
        "local_directory": "demo/",
        "dlt_meta_schema": "dlt_meta_test",
        "bronze_schema": "bronze_test",
        "silver_schema": "silver_test",
        "dlt_meta_layer": "1",  # bronze_silver
        "bronze_table": "bronze_dataflowspec",
        "silver_table": "silver_dataflowspec",
        "overwrite": "1",
        "version": "v1",
        "environment": "dev",
        "author": "test_user",
        "update_paths": "1"
    }
    run_cli_command(payload)

if __name__ == "__main__":
    print("DLT-META CLI Test Commands")
    print("=" * 60)
    
    # Test each command type
    print("1. Testing ONBOARD command...")
    test_onboard_command()
    
    print("2. Testing DEPLOY command...")
    test_deploy_command()
    
    print("3. Testing INVALID command...")
    test_invalid_command()
    
    print("4. Testing ONBOARD_UI command...")
    test_onboard_ui_command()
    
    print("All tests completed!")