"""Tests for SQL backends and Unity Catalog metadata reads."""

import sys
from types import ModuleType, SimpleNamespace

import pytest
from databricks.sdk.service.sql import StatementState

from databricks.labs.sdp_meta.governance.tagging import backends
from databricks.labs.sdp_meta.governance.tagging.backends import (
    SdkBackend,
    make_backend,
    preflight_columns,
    preflight_tables,
    read_actual,
)
from databricks.labs.sdp_meta.governance.tagging.config import TaggingError
from databricks.labs.sdp_meta.governance.tagging.models import Desired, Key


class FakeBackend:
    def __init__(self):
        self.responses = [
            [("bronze", "customers", "data_domain", "customer")],
            [("bronze", "customers", "email", "sensitivity", "pii")],
        ]

    def sql(self, _statement):
        return self.responses.pop(0)


def test_read_actual_returns_table_and_column_assignments():
    actual = read_actual(
        FakeBackend(),
        {("main", "bronze", "customers")},
    )

    assert actual == {
        Key("main", "bronze", "customers", None, "data_domain"): "customer",
        Key("main", "bronze", "customers", "email", "sensitivity"): "pii",
    }


def test_read_actual_groups_queries_by_catalog_and_normalizes_null_values():
    class CatalogBackend:
        def sql(self, statement):
            if "column_tags" in statement:
                return []
            if "`main`" in statement:
                return [("bronze", "customers", "quality", None)]
            return [("silver", "orders", "domain", "sales")]

    actual = read_actual(
        CatalogBackend(),
        {
            ("secondary", "silver", "orders"),
            ("main", "bronze", "customers"),
        },
    )

    assert actual == {
        Key("main", "bronze", "customers", None, "quality"): "",
        Key("secondary", "silver", "orders", None, "domain"): "sales",
    }


def test_preflight_columns_accepts_existing_columns():
    backend = FakeBackend()
    backend.responses = [[("email",), ("name",)]]
    desired = {
        Key("main", "bronze", "customers", "email", "sensitivity"): Desired("pii"),
        Key("main", "bronze", "customers", None, "domain"): Desired("customer"),
    }

    preflight_columns(backend, desired)


def test_preflight_columns_rejects_missing_table():
    backend = FakeBackend()
    backend.responses = [[]]
    desired = {
        Key("main", "bronze", "customers", "email", "sensitivity"): Desired("pii")
    }

    with pytest.raises(
        TaggingError,
        match=r"table not found \(or no access\): main.bronze.customers",
    ):
        preflight_columns(backend, desired)


def test_preflight_columns_rejects_missing_configured_columns():
    backend = FakeBackend()
    backend.responses = [[("id",)]]
    desired = {
        Key("main", "bronze", "customers", "email", "sensitivity"): Desired("pii"),
        Key("main", "bronze", "customers", "phone", "sensitivity"): Desired("pii"),
    }

    with pytest.raises(
        TaggingError,
        match=r"configured columns do not exist: \['email', 'phone'\]",
    ):
        preflight_columns(backend, desired)


def test_preflight_tables_accepts_existing_tables():
    backend = FakeBackend()
    backend.responses = [[("bronze", "customers"), ("silver", "orders")]]

    preflight_tables(
        backend,
        {
            ("main", "silver", "orders"),
            ("main", "bronze", "customers"),
        },
    )


def test_preflight_tables_rejects_missing_tables():
    backend = FakeBackend()
    backend.responses = [[("bronze", "customers")]]

    with pytest.raises(TaggingError, match="main.silver.orders"):
        preflight_tables(
            backend,
            {
                ("main", "bronze", "customers"),
                ("main", "silver", "orders"),
            },
        )


def test_make_backend_prefers_active_spark_session(monkeypatch):
    active_session = object()

    class FakeSparkSession:
        @staticmethod
        def getActiveSession():
            return active_session

    pyspark = ModuleType("pyspark")
    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = FakeSparkSession
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)

    backend = make_backend("warehouse")

    assert isinstance(backend, backends.SparkBackend)
    assert backend.spark is active_session


def test_make_backend_uses_sdk_when_spark_is_inactive(monkeypatch):
    class FakeSparkSession:
        @staticmethod
        def getActiveSession():
            return None

    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql.SparkSession = FakeSparkSession
    monkeypatch.setitem(sys.modules, "pyspark.sql", pyspark_sql)
    monkeypatch.setattr(backends, "SdkBackend", lambda warehouse_id: ("sdk", warehouse_id))

    assert make_backend("warehouse-123") == ("sdk", "warehouse-123")


def test_make_backend_requires_warehouse_without_spark(monkeypatch):
    monkeypatch.setitem(sys.modules, "pyspark", None)
    monkeypatch.setitem(sys.modules, "pyspark.sql", None)

    with pytest.raises(TaggingError, match="provide --warehouse-id"):
        make_backend(None)


def test_sdk_backend_executes_statement_and_returns_tuples():
    execution = SimpleNamespace(
        execute_statement=lambda **kwargs: SimpleNamespace(
            status=SimpleNamespace(state=StatementState.SUCCEEDED),
            result=SimpleNamespace(data_array=[["a", 1], ["b", 2]]),
        )
    )
    backend = SdkBackend.__new__(SdkBackend)
    backend.workspace = SimpleNamespace(statement_execution=execution)
    backend.warehouse_id = "warehouse-123"

    assert backend.sql("SELECT * FROM values") == [("a", 1), ("b", 2)]


def test_sdk_backend_reports_execution_failure():
    execution = SimpleNamespace(
        execute_statement=lambda **kwargs: SimpleNamespace(
            status=SimpleNamespace(
                state=StatementState.FAILED,
                error=SimpleNamespace(message="permission denied"),
            )
        )
    )
    backend = SdkBackend.__new__(SdkBackend)
    backend.workspace = SimpleNamespace(statement_execution=execution)
    backend.warehouse_id = "warehouse-123"

    with pytest.raises(RuntimeError, match="SQL failed: permission denied"):
        backend.sql("SELECT secret FROM restricted")
