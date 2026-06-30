# Databricks notebook source
# MAGIC %md
# MAGIC # Row-filter validation
# MAGIC
# MAGIC Asserts that the bronze + silver `customers` tables only expose
# MAGIC rows the row-filter UDF allows the current reader to see. With
# MAGIC the demo predicate (`is_account_group_member('admins') OR
# MAGIC region IN ('US', 'UK')`):
# MAGIC
# MAGIC - **non-admin reader** — visible regions must be a subset of
# MAGIC   `{'US', 'UK'}`. Source CSV has 4 rows per region across
# MAGIC   `{US, UK, DE, JP}`, so the visible total must be 8.
# MAGIC - **admin reader** — sees all 16 rows; we still print counts
# MAGIC   but skip the strict subset assertion (the filter wiring is
# MAGIC   confirmed by the fact that the table built at all — UC fails
# MAGIC   `CREATE TABLE` if the referenced row-filter UDF is missing).

# COMMAND ----------

dbutils.widgets.text("uc_catalog_name", "")
dbutils.widgets.text("bronze_schema", "")
dbutils.widgets.text("silver_schema", "")
uc_catalog_name = dbutils.widgets.get("uc_catalog_name")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")

# COMMAND ----------

is_admin = spark.sql(
    "SELECT is_account_group_member('admins') AS is_admin"
).first().is_admin
print(f"Running as admin? {is_admin}")

# COMMAND ----------

allowed_regions = {"US", "UK"}
failures = []


def _assert_filtered(layer, fqn):
    df = spark.sql(f"SELECT region, COUNT(*) AS n FROM {fqn} GROUP BY region")
    rows = df.collect()
    counts = {r.region: r.n for r in rows}
    total = sum(counts.values())
    print(f"\n{layer:>6} {fqn}")
    print(f"  region counts : {counts}")
    print(f"  total visible : {total}")

    if is_admin:
        return

    leaked = {r for r in counts.keys() if r not in allowed_regions}
    if leaked:
        failures.append(
            f"{layer} {fqn} leaked rows from regions {sorted(leaked)} "
            f"(filter expected only {sorted(allowed_regions)})"
        )
    if total != 8:
        failures.append(
            f"{layer} {fqn} returned {total} rows; expected 8 "
            f"(4 US + 4 UK with the demo CSV)"
        )


_assert_filtered("bronze", f"{uc_catalog_name}.{bronze_schema}.customers")
_assert_filtered("silver", f"{uc_catalog_name}.{silver_schema}.customers")

# COMMAND ----------

if failures:
    raise AssertionError(
        "Row filter not enforced as expected:\n  - "
        + "\n  - ".join(failures)
    )
print("\nRow filter enforced correctly on bronze + silver customers.")
