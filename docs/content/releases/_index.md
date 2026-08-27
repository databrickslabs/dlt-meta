---
title: "Releases"
date: 2021-08-04T14:50:11-04:00
weight: 80
draft: false
---
# v0.0.10

## ⚠️ Breaking Changes
- **DPM Mode Removal**: DLT-META v0.0.9 pipelines using DPM mode flag must be migrated to the default publishing mode before upgrading. This change is metadata-only and doesn't impact existing datasets, but is irreversible.
- **Multi-Level Namespace Changes**: Custom schema qualification in table names is no longer supported. Tables must be created without database qualifiers.
- **invoke_dlt_pipeline Argument Changes**: Method arguments now require layer-specific prefixes (bronze_ or silver_) to support apply_changes_from_snapshot in both layers. This affects existing pipeline configurations using the previous argument naming.

## Migration Guide
1. **DPM Mode Migration**:
   - Before upgrading to v0.0.10, update pipeline JSON settings as per Databricks documentation [Migrate to the default publishing mode](https://docs.databricks.com/aws/en/dlt/migrate-to-dpm#migrate-to-the-default-publishing-mode)
   - This is a one-way migration - ensure all stakeholders are informed
   - Verify pipeline functionality in test environment first

2. **invoke_dlt_pipeline Updates**:
   - Method signature changed to support layer-specific functions:
     ```python
     invoke_dlt_pipeline(
         spark,
         layer,
         bronze_custom_transform_func=None,    # Previously: custom_transform_func
         silver_custom_transform_func=None,    # New in v0.0.10
         bronze_next_snapshot_and_version=None,  # Previously: next_snapshot_and_version
         silver_next_snapshot_and_version=None   # New in v0.0.10
     )
     ```
   - Layer-specific functions allow different transformations for bronze and silver layers
   - Existing code using single custom_transform_func should move to bronze_custom_transform_func
   - Existing code using next_snapshot_and_version should move to bronze_next_snapshot_and_version
   - Review and update all pipeline configurations using this method

## Enhancements
- Added apply_changes_from_snapshot support in silver layer [PR](https://github.com/databrickslabs/dlt-meta/pull/187)
- Added UI using databricks lakehouse app for onboarding/deploy commands [PR](https://github.com/databrickslabs/dlt-meta/pull/168)
- Added support for non-Delta as sinks(delta, kafka) [PR](https://github.com/databrickslabs/dlt-meta/pull/157)
- Added quarantine support in silver layer for data quality rules [PR](https://github.com/databrickslabs/dlt-meta/pull/191)
- Added support for table comments, column comments, and cluster_by [PR](https://github.com/databrickslabs/dlt-meta/pull/91)
- Added catalog support for sourceDetails and targetDetails [PR](https://github.com/databrickslabs/dlt-meta/issues/173)
- Added DBDemos for dlt-meta [PR](https://github.com/databrickslabs/dlt-meta/issues/183)
- Added YAML support for onboarding [PR](https://github.com/databrickslabs/dlt-meta/issues/184)
- Fixed issue cluster by not working with bronze append only table [PR](https://github.com/databrickslabs/dlt-meta/issues/197)
- Fixed issue view name containing period when using DPM [PR](https://github.com/databrickslabs/dlt-meta/issues/169)
- Fixed issue CLI onboarding overwrite option always set to True [PR](https://github.com/databrickslabs/dlt-meta/issues/163)
- Fixed issue Silver Lakeflow Declarative Pipeline not creating based on passed database [PR](https://github.com/databrickslabs/dlt-meta/issues/160)
- Fixed issue PyPI download stats display [PR](https://github.com/databrickslabs/dlt-meta/issues/200)
- Fixed issue Silver Data Quality not working [PR](https://github.com/databrickslabs/dlt-meta/issues/156)
- Fixed issue Removed DPM flag check inside dataflowpipeline [PR](https://github.com/databrickslabs/dlt-meta/issues/177)
- Fixed issue Updated dlt-meta demos into Delta Live Tables Notebook github [PR](https://github.com/databrickslabs/dlt-meta/issues/158)
- Fixed issue Adding multiple col support for auto_cdc api [PR](https://github.com/databrickslabs/dlt-meta/pull/224)
- Fixed issue Added support for custom transformations for Kafka/Delta [PR](https://github.com/databrickslabs/dlt-meta/pull/228)

# v0.0.9
## Enhancements
- Added  apply_changes_from_snapshot api support in bronze layer: [PR](https://github.com/databrickslabs/dlt-meta/pull/124)
- Added dlt append_flow api support for silver layer: [PR](https://github.com/databrickslabs/dlt-meta/pull/63)
- Added support for file metadata columns for autoloader: [PR](https://github.com/databrickslabs/dlt-meta/pull/56)
- Added support for Bring your own custom transformation: [Issue](https://github.com/databrickslabs/dlt-meta/issues/68)
- Added support to Unify PyPI releases with GitHub OIDC: [PR](https://github.com/databrickslabs/dlt-meta/pull/62)
- Added demo for append_flow and file_metadata options: [PR](https://github.com/databrickslabs/dlt-meta/issues/74)
- Added Demo for silver fanout architecture: [PR](https://github.com/databrickslabs/dlt-meta/pull/83)
- Added  hugo-theme-relearn themee: [PR](https://github.com/databrickslabs/dlt-meta/pull/132)
- Added unit tests to showcase silver layer fanout examples: [PR](https://github.com/databrickslabs/dlt-meta/pull/67)
- Added liquid cluster support: [PR](https://github.com/databrickslabs/dlt-meta/pull/136)
- Added support for UC Volume + Serverless support for CLI, Integration tests and Demos: [PR](https://github.com/databrickslabs/dlt-meta/pull/105)
- Added Chaining bronze/silver pipelines into single DLT: [PR](https://github.com/databrickslabs/dlt-meta/pull/130)
#### Updates 
- Fixed issue for No such file or directory: '/demo' :[PR](https://github.com/databrickslabs/dlt-meta/issues/59)
- Fixed issue DLT-META CLI onboard command issue for Azure: databricks.sdk.errors.platform.ResourceAlreadyExists :[PR](https://github.com/databrickslabs/dlt-meta/issues/51)
- Fixed issue Changed dbfs.create to mkdirs for CLI: [PR](https://github.com/databrickslabs/dlt-meta/pull/53)
- Fixed issue DLT-META CLI should use pypi lib instead of whl : [PR](https://github.com/databrickslabs/dlt-meta/pull/79)
- Fixed issue Onboarding with multiple partition columns errors out: [PR](https://github.com/databrickslabs/dlt-meta/pull/134)


# v0.0.8
## Enhancements
- Added dlt append_flow api support: [PR](https://github.com/databrickslabs/dlt-meta/pull/58)
- Added dlt append_flow api support for silver layer: [PR](https://github.com/databrickslabs/dlt-meta/pull/63)
- Added support for file metadata columns for autoloader: [PR](https://github.com/databrickslabs/dlt-meta/pull/56)
- Added support for Bring your own custom transformation: [Issue](https://github.com/databrickslabs/dlt-meta/issues/68)
- Added support to Unify PyPI releases with GitHub OIDC: [PR](https://github.com/databrickslabs/dlt-meta/pull/62)
- Added demo for append_flow and file_metadata options: [PR](https://github.com/databrickslabs/dlt-meta/issues/74)
- Added Demo for silver fanout architecture: [PR](https://github.com/databrickslabs/dlt-meta/pull/83)
- Added documentation in docs site for new features: [PR](https://github.com/databrickslabs/dlt-meta/pull/64)
- Added unit tests to showcase silver layer fanout examples: [PR](https://github.com/databrickslabs/dlt-meta/pull/67)
#### Updates 
- Fixed issue for No such file or directory: '/demo' :[PR](https://github.com/databrickslabs/dlt-meta/issues/59)
- Fixed issue DLT-META CLI onboard command issue for Azure: databricks.sdk.errors.platform.ResourceAlreadyExists :[PR](https://github.com/databrickslabs/dlt-meta/issues/51)
- Fixed issue Changed dbfs.create to mkdirs for CLI: [PR](https://github.com/databrickslabs/dlt-meta/pull/53)
- Fixed issue DLT-META CLI should use pypi lib instead of whl : [PR](https://github.com/databrickslabs/dlt-meta/pull/79)


# v0.0.7
## Enhancements
### 1. Mismatched Keys: Update read_dlt_delta() with key "source_database" instead of "database" [#33](https://github.com/databrickslabs/dlt-meta/pull/33)
### 2. Create dlt-meta cli documentation #45 
- Readme and docs to include above features


# v0.0.6
## Enhancements
### 1. Migrate to create_streaming_table api from create_streaming_live_table [#37](https://github.com/databrickslabs/dlt-meta/pull/39)
#### Updates 
- Readme and docs to include above features
- Added Data Quality support for silver layer
- Updated existing demos to incorporate unity catalog and integration test framework
- integration tests framework which can be used to launch demos

# v0.0.5

## New Features

### 1. Unity Catalog Support ([#28](https://github.com/databrickslabs/dlt-meta/pull/28))

### 2. Databricks Labs CLI Support ([#28](https://github.com/databrickslabs/dlt-meta/pull/28)) 
- Added two commands for DLT-META
- onboard: Captures all onboarding details from command line and launch onboarding job to your databricks workspace
- deploy: Captures all Lakeflow Declarative Pipeline details from command line and launch Lakeflow Declarative Pipeline to your databricks workspace

### Updates 
- Readme and docs to include above features
- Updated existing demos to incorporate unity catalog and integration test framework
- integration tests framework which can be used to launch demos

# v0.0.4
### Bug Fixes
- [ Introduced new source detail option for eventhub:  eventhub.accessKeySecretName](https://github.com/databrickslabs/dlt-meta/issues/13 )


# v0.0.3
### Bug Fixes
- [ Infer datatypes from sequence_by to __START_AT, __END_AT for apply changes API](https://github.com/databrickslabs/dlt-meta/issues/4 )
-  Removed Git release tag from github actions


# v0.0.2
### New Features
- Table properties support for bronze, quarantine and silver tables using create_streaming_live_table api call
- Support for track history column using apply_changes api
- Support for delta as source
- Validation for bronze/silver onboarding
### Bug Fixes
- Input schema parsing issue in onboarding
### Updates
-  Readme and docs to include above features


# v.0.0.1
#### Release v.0.0.1