---
id: intro
title: Introduction
sidebar_position: 1
---

# SDP-META

SDP-META is a metadata-driven framework for building automated Bronze and Silver data pipelines on [Databricks Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). Define your pipelines in a JSON or YAML onboarding file; a single generic Declarative Pipeline builds the full processing graph automatically.

## Architecture

![SDP-META Architecture](/img/sdp-meta-architecture.svg)

SDP-META operates in two phases:

1. **Onboard Job** — reads your onboarding YAML or JSON file and writes structured metadata into `bronze_dataflowspec` and `silver_dataflowspec` Delta tables. Re-run this job whenever you change the onboarding file.
2. **Generic Declarative Pipeline** — at runtime, reads the DataflowSpec Delta tables and dynamically constructs the pipeline graph: sources, transformations, expectations, CDC flows, and sinks.

## Feature matrix

| Feature | Support |
|---|---|
| Input sources | Autoloader (cloudFiles), Delta, Eventhub, Kafka, Snapshot |
| Layers | Bronze, Silver |
| Custom transformations | Bronze and Silver layers |
| Data Quality Expectations | Bronze, Silver |
| Quarantine table | Bronze layer |
| CDC (create_auto_cdc_flow) | Bronze, Silver |
| CDC from Snapshot | Bronze layer |
| append_flow | Bronze layer |
| Liquid clustering | Bronze, Bronze Quarantine, Silver |
| create_sink (Delta, Kafka) | Bronze, Silver |
| Declarative Automation Bundles | Yes |
| SDP-META CLI | Yes |
| Lakehouse App (UI) | Yes |
| MCP Server | Yes |

For setup options, see [Getting Started](./getting-started/index.md).
