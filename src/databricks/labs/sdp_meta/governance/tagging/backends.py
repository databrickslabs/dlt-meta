"""SQL execution backends and Unity Catalog metadata reads."""

from typing import Dict, List, Optional, Tuple

from databricks.labs.sdp_meta.governance.tagging.config import fail
from databricks.labs.sdp_meta.governance.tagging.models import Desired, Key


class SparkBackend:
    def __init__(self):
        from pyspark.sql import SparkSession

        self.spark = (
            SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        )

    def sql(self, statement: str) -> List[tuple]:
        return [tuple(row) for row in self.spark.sql(statement).collect()]


class SdkBackend:
    def __init__(self, warehouse_id: str):
        from databricks.sdk import WorkspaceClient

        self.workspace = WorkspaceClient()
        self.warehouse_id = warehouse_id

    def sql(self, statement: str) -> List[tuple]:
        from databricks.sdk.service.sql import StatementState

        response = self.workspace.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=self.warehouse_id,
            wait_timeout="50s",
        )
        if response.status.state != StatementState.SUCCEEDED:
            error = (
                response.status.error.message
                if response.status.error
                else response.status.state
            )
            raise RuntimeError(f"SQL failed: {error}\n  {statement[:200]}")
        rows = (
            response.result.data_array
            if response.result and response.result.data_array
            else []
        )
        return [tuple(row) for row in rows]


def make_backend(warehouse_id: Optional[str]):
    try:
        from pyspark.sql import SparkSession

        if SparkSession.getActiveSession():
            return SparkBackend()
    except ImportError:
        pass
    if not warehouse_id:
        fail("no active Spark session; provide --warehouse-id for the SQL backend")
    return SdkBackend(warehouse_id)


def _table_predicates(pairs: set, schema_field: str) -> str:
    return " OR ".join(
        f"({schema_field} = '{schema}' AND table_name = '{table}')"
        for schema, table in sorted(pairs)
    )


def read_actual(backend, tables: set) -> Dict[Key, str]:
    actual: Dict[Key, str] = {}
    by_catalog: Dict[str, set] = {}
    for catalog, schema, table in tables:
        by_catalog.setdefault(catalog, set()).add((schema, table))
    for catalog, pairs in by_catalog.items():
        predicates = _table_predicates(pairs, "schema_name")
        for schema, table, key, value in backend.sql(
            f"SELECT schema_name, table_name, tag_name, tag_value "
            f"FROM `{catalog}`.information_schema.table_tags "
            f"WHERE {predicates}"
        ):
            actual[Key(catalog, schema, table, None, key)] = value or ""
        for schema, table, column, key, value in backend.sql(
            f"SELECT schema_name, table_name, column_name, tag_name, tag_value "
            f"FROM `{catalog}`.information_schema.column_tags "
            f"WHERE {predicates}"
        ):
            actual[Key(catalog, schema, table, column, key)] = value or ""
    return actual


def preflight_columns(backend, desired: Dict[Key, Desired]) -> None:
    required: Dict[Tuple[str, str, str], set] = {}
    for key in desired:
        if key.column:
            required.setdefault((key.catalog, key.schema, key.table), set()).add(
                key.column
            )
    for (catalog, schema, table), columns in sorted(required.items()):
        available = {
            row[0]
            for row in backend.sql(
                f"SELECT column_name FROM `{catalog}`.information_schema.columns "
                f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
            )
        }
        if not available:
            fail(f"table not found (or no access): {catalog}.{schema}.{table}")
        missing = columns - available
        if missing:
            fail(
                f"{catalog}.{schema}.{table}: configured columns do not exist: "
                f"{sorted(missing)}"
            )


def preflight_tables(backend, tables: set) -> None:
    by_catalog: Dict[str, set] = {}
    for catalog, schema, table in tables:
        by_catalog.setdefault(catalog, set()).add((schema, table))
    for catalog, pairs in by_catalog.items():
        predicates = _table_predicates(pairs, "table_schema")
        available = {
            (schema, table)
            for schema, table in backend.sql(
                f"SELECT table_schema, table_name "
                f"FROM `{catalog}`.information_schema.tables "
                f"WHERE {predicates}"
            )
        }
        missing = pairs - available
        if missing:
            names = [f"{catalog}.{schema}.{table}" for schema, table in sorted(missing)]
            fail(
                f"configured target tables do not exist (or are inaccessible): {names}"
            )
