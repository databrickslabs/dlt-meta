---
title: "DAB Demo"
date: 2024-02-26T14:25:26-04:00
weight: 28
draft: false
---

### DAB Demo

## Overview
This demo showcases how to use Databricks Asset Bundles (DABs) with DLT-Meta:

This demo will perform following steps:
- Create dlt-meta schema's for dataflowspec and bronze/silver layer
- Upload necessary resources to unity catalog volume
- Create DAB files with catalog, schema, file locations populated
- Deploy DAB to databricks workspace
- Run onboarding using DAB commands
- Run Bronze/Silver Pipelines using DAB commands
- Demo examples will showcase fan-out pattern in silver layer
- Demo example will show case custom transformations for bronze/silver layers
- Adding custom columns and metadata to Bronze tables
- Implementing SCD Type 1 to Silver tables
- Applying expectations to filter data in Silver tables

### Steps:
1. Launch Command Prompt

2. Install [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
    - Once you install Databricks CLI, authenticate your current machine to a Databricks Workspace:
    
    ```commandline
    databricks auth login --host WORKSPACE_HOST
    ```

3. Install Python package requirements:
    ```commandline
    # Core requirements
    pip install "PyYAML>=6.0" setuptools databricks-sdk

    # Development requirements
    pip install flake8==6.0 delta-spark==3.0.0 pytest>=7.0.0 coverage>=7.0.0 pyspark==3.5.5
    ```

4. Clone dlt-meta:
    ```commandline
    git clone https://github.com/databrickslabs/dlt-meta.git 
    ```

5. Navigate to project directory:
    ```commandline
    cd dlt-meta
    ```

6. Set python environment variable into terminal:
    ```commandline
    dlt_meta_home=$(pwd)
    export PYTHONPATH=$dlt_meta_home
    ```

7. Generate DAB resources and set up schemas:
    This command will:
    - Generate DAB configuration files
    - Create DLT-Meta schemas
    - Upload necessary files to volumes
    ```commandline
    python demo/generate_dabs_resources.py --source=cloudfiles --uc_catalog_name=<your_catalog_name> --profile=<your_profile>
    ```
    > Note: If you don't specify `--profile`, you'll be prompted for your Databricks workspace URL and access token.

8. Deploy and run the DAB bundle:
    - Navigate to the DAB directory:
    ```commandline
    cd demo/dabs
    ```

    - Validate the bundle configuration:
    ```commandline
    databricks bundle validate --profile=<your_profile>
    ```

    - Deploy the bundle to dev environment:
    ```commandline
    databricks bundle deploy --target dev --profile=<your_profile>
    ```

    - Run the onboarding job:
    ```commandline
    databricks bundle run onboard_people -t dev --profile=<your_profile>
    ```

    - Execute the pipelines:
    ```commandline
    databricks bundle run execute_pipelines_people -t dev --profile=<your_profile>
    ```

![dab_onboarding_job.png](/images/dab_onboarding_job.png)
![dab_dlt_pipelines.png](/images/dab_dlt_pipelines.png)
