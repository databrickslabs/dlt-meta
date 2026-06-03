---
title: "Getting Started"
date: 2021-08-04T14:50:11-04:00
weight: 5
draft: false
---

The following tutorial will guide you through the process for setting up SDP-META on your Databricks Lakehouse environment.

> **Running outside Databricks?** SDP-META also targets **OSS Apache Spark 4.1+** (`pyspark.pipelines`) as a co-equal runtime — no Unity Catalog or Databricks workspace required. Runtime selection is automatic. See [OSS Apache Spark Declarative Pipelines](oss_spark/) for the parity matrix and setup.

You will deploy/configure the solution, configure a database/table for bronze and silver layer as per below stages.

![SDP-META Stages](/images/sdp-meta_stages.png)
