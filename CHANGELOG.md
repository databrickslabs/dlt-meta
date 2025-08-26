# Changelog
## [v.0.0.9] 
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
- Fixed issue for No such file or directory: '/demo' :[PR](https://github.com/databrickslabs/dlt-meta/issues/59)
- Fixed issue DLT-META CLI onboard command issue for Azure: databricks.sdk.errors.platform.ResourceAlreadyExists :[PR](https://github.com/databrickslabs/dlt-meta/issues/51)
- Fixed issue Changed dbfs.create to mkdirs for CLI: [PR](https://github.com/databrickslabs/dlt-meta/pull/53)
- Fixed issue DLT-META CLI should use pypi lib instead of whl : [PR](https://github.com/databrickslabs/dlt-meta/pull/79)
- Fixed issue Onboarding with multiple partition columns errors out: [PR](https://github.com/databrickslabs/dlt-meta/pull/134)

## [v.0.0.7] 
- Added dlt-meta cli documentation and readme with browser support: [PR](https://github.com/databrickslabs/dlt-meta/pull/45)

## [v.0.0.6] 
- migrate to create streaming table api from create streaming live table: [PR](https://github.com/databrickslabs/dlt-meta/pull/39)

## [v.0.0.5] 
- Enabled Unity Catalog support: [PR](https://github.com/databrickslabs/dlt-meta/pull/28)
- Added databricks labs cli: [PR](https://github.com/databrickslabs/dlt-meta/pull/28)

## [v0.0.4] - 2023-10-09
### Added
- Functionality to introduce an new option for event hub configuration. Namely a source_details option 'eventhub.accessKeySecretName' to properly construct the eh_shared_key_value properly. Without this option, there were errors while connecting to the event hub service (linked to [issue-13 - java.lang.RuntimeException: non-nullable field authBytes was serialized as null #13](https://github.com/databrickslabs/dlt-meta/issues/13))

## [v0.0.3] - 2023-06-07
### Fixed
-  infer datatypes from sequence_by to __START_AT, __END_AT for apply changes API
### Changed
-   setup.py for version
### Removed
-   Git release tag from github actions

## [v0.0.2] - 2023-05-11
### Added
- Table properties support for bronze, quarantine and silver tables using create_streaming_live_table api call
- Support for track history column using apply_changes api
- Support for delta as source
- Validation for bronze/silver onboarding
### Fixed
- Input schema parsing issue in onboarding
### Modified
-  Readme and docs to include above features

## [v0.0.1] - 2023-03-22
### Added

- Initial public release version.