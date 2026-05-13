"""Pytest fixtures for the SDP-META test suite.

A single SparkSession is built once at session start and reused across every
test. Per-test isolation lives in the test base class (database drops, temp
paths). Stopping the SparkContext between tests caused a stale singleton from
``SparkSession.builder.getOrCreate()`` and produced flaky Py4J errors.

Databricks auth env vars are sourced from ``~/.databrickscfg`` via
``databricks auth env --profile <NAME>``. The profile name is read (in
priority order) from:
  1. ``DATABRICKS_CONFIG_PROFILE`` env var if already exported in the shell.
  2. ``.databricks-test-profile`` at the repo root (one line, gitignored).
If neither is present, the suite runs without injecting Databricks env vars
(tests that don't need them still pass).
"""

import json
import os
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def _load_databricks_auth_env() -> None:
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    if not profile:
        profile_file = Path(__file__).resolve().parent.parent / ".databricks-test-profile"
        if profile_file.is_file():
            profile = profile_file.read_text().strip()
    if not profile:
        return
    try:
        result = subprocess.run(
            ["databricks", "auth", "env", "--profile", profile],
            capture_output=True, text=True, check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return
    try:
        env_vars = json.loads(result.stdout).get("env", {})
    except json.JSONDecodeError:
        return
    for key, value in env_vars.items():
        os.environ.setdefault(key, value)


_load_databricks_auth_env()

import pytest
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip


@pytest.fixture(scope="session", autouse=True)
def spark():
    builder = (
        SparkSession.builder.appName("SDP-META_UNIT_TESTS")
        .master("local[4]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("delta.log.cacheSize", "3")
        .config("spark.databricks.delta.delta.log.cacheSize", "3")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
