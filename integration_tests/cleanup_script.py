""" A script to run integration tests for DLT-Meta."""

import argparse
from databricks.sdk import WorkspaceClient


def get_workspace_api_client(profile=None) -> WorkspaceClient:
    """Get api client with config."""
    if profile:
        workspace_client = WorkspaceClient(profile=profile)
    else:
        workspace_client = WorkspaceClient(host=input('Databricks Workspace URL: '), token=input('Token: '))
    return workspace_client


def process_arguments():
    """Process command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile",
                        help="provide databricks cli profile name, if not provide databricks_host and token")
    args = parser.parse_args()
    return args


def main():
    """Entry method to run integration tests."""
    args = process_arguments()
    workspace_client = get_workspace_api_client(args.profile)
    print("workspace_client created")
    # list = workspace_client.pipelines.list_pipelines(filter="name like 'dlt-meta-silver-integration-test-%'")
    # print("List of pipelines:")
    # for pipeline in list:
    #     print(f"id = {pipeline.pipeline_id} , name = {pipeline.name}")
    #     workspace_client.pipelines.delete(pipeline.pipeline_id)
    uc_catalog_name = "ravi_dlt_meta_uc"
    schema_list = workspace_client.schemas.list(catalog_name=uc_catalog_name)
    for schema in schema_list:
        if schema.name.startswith("dlt_meta_silver_it_"):
            print(f" schema: {schema.name}")
            vol_list = workspace_client.volumes.list(catalog_name=uc_catalog_name, schema_name=schema.name)
            for vol in vol_list:
                print(f"Deleting volume:{vol.full_name}")
                workspace_client.volumes.delete(vol.full_name)
            tables_list = workspace_client.tables.list(catalog_name=uc_catalog_name, schema_name=schema.name)
            for table in tables_list:
                print(f"Deleting table:{table.full_name}")
                workspace_client.tables.delete(table.full_name)
            workspace_client.schemas.delete(schema.full_name)


if __name__ == "__main__":
    main()