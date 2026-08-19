# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Tagging — Interactive Demo
# MAGIC
# MAGIC This notebook creates four Unity Catalog Delta tables with 100 rows each,
# MAGIC then applies the same desired tag configuration through the SDP-META
# MAGIC Python API or, optionally, the Labs CLI from your local terminal.
# MAGIC
# MAGIC The default `api` mode is self-contained. Choose `cli` to print commands
# MAGIC that download the notebook-generated `tags.yml` and exercise the
# MAGIC customer-facing Labs CLI.
# MAGIC
# MAGIC ### High-level flow
# MAGIC ```
# MAGIC ┌─────────────────────┐    ┌──────────────────────┐    ┌─────────────────────┐
# MAGIC │ Create Demo Tables  │───>│ Generate tags.yml    │───>│ SDP-META Tagging    │
# MAGIC │ customers, orders,  │    │ in a UC Volume       │    │ API / Labs CLI      │
# MAGIC │ products, txns      │    │                      │    │                     │
# MAGIC └─────────────────────┘    └──────────────────────┘    └──────────┬──────────┘
# MAGIC                                                                  │
# MAGIC                         ┌────────────────────────┐                ▼
# MAGIC                         │ Verify UC Tags and     │<────┌─────────────────────┐
# MAGIC                         │ Delta Ownership State  │     │ Reconcile Desired,  │
# MAGIC                         └────────────────────────┘     │ Actual, and State   │
# MAGIC                                                        └─────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configure the demo
# MAGIC
# MAGIC The notebook generates `tags.yml` from these values and stores it in the
# MAGIC configured Unity Catalog Volume.
# MAGIC
# MAGIC SDP-META can be installed from a GitHub branch or from a wheel stored in
# MAGIC a Unity Catalog Volume. Use the wheel option to validate local changes.

# COMMAND ----------

dbutils.widgets.text("git_branch", "main", "Git Branch")
dbutils.widgets.dropdown(
    "install_source",
    "git_branch",
    ["git_branch", "whl_file"],
    "Install Source",
)
dbutils.widgets.text(
    "whl_file_path",
    "",
    "Wheel File Path (when install_source=whl_file)",
)
dbutils.widgets.text("uc_catalog_name", "sdp_meta", "UC Catalog")
dbutils.widgets.text(
    "uc_schema_name",
    "governance_tagging_demo",
    "UC Schema",
)
dbutils.widgets.text(
    "uc_volume_name",
    "governance_tagging_demo",
    "UC Volume for tags.yml",
)
dbutils.widgets.text(
    "warehouse_id",
    "",
    "SQL Warehouse ID used by local CLI",
)
dbutils.widgets.dropdown(
    "execution_mode",
    "api",
    ["api", "cli"],
    "Apply using Python API or local CLI",
)
dbutils.widgets.dropdown(
    "cleanup",
    "false",
    ["false", "true"],
    "Drop demo schema in final cell",
)

# COMMAND ----------

git_branch = dbutils.widgets.get("git_branch").strip()
install_source = dbutils.widgets.get("install_source")
whl_file_path = dbutils.widgets.get("whl_file_path").strip()

if install_source == "whl_file":
    if not whl_file_path:
        raise ValueError(
            "install_source=whl_file requires whl_file_path, for example "
            "/Volumes/<catalog>/<schema>/<volume>/"
            "sdp_meta-<version>-py3-none-any.whl"
        )
    sdp_meta_install_target = whl_file_path
else:
    if not git_branch:
        raise ValueError("git_branch must not be empty")
    sdp_meta_install_target = (
        "git+https://github.com/databrickslabs/"
        f"dlt-meta.git@{git_branch}"
    )

print(f"Install source: {install_source}")
print(f"Install target: {sdp_meta_install_target}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install SDP-META
# MAGIC
# MAGIC - `git_branch`: installs the selected branch from GitHub.
# MAGIC - `whl_file`: installs a pre-built wheel from the supplied Volume path.

# COMMAND ----------

%pip install $sdp_meta_install_target  # noqa: E999
dbutils.library.restartPython()

# COMMAND ----------

import re


IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,254}$")


def regular_identifier(value: str, label: str) -> str:
    value = value.strip()
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(
            f"{label} must be a regular Unity Catalog identifier, got {value!r}"
        )
    return value


catalog = regular_identifier(
    dbutils.widgets.get("uc_catalog_name"),
    "uc_catalog_name",
)
schema = regular_identifier(
    dbutils.widgets.get("uc_schema_name"),
    "uc_schema_name",
)
volume = regular_identifier(
    dbutils.widgets.get("uc_volume_name"),
    "uc_volume_name",
)
warehouse_id = dbutils.widgets.get("warehouse_id").strip()
execution_mode = dbutils.widgets.get("execution_mode").lower()
state_table = f"{catalog}.{schema}.uc_governance_tag_assignments"
tags_volume_path = f"/Volumes/{catalog}/{schema}/{volume}/tags.yml"

print(f"Target schema: {catalog}.{schema}")
print(f"Ownership state: {state_table}")
print(f"Desired tags file: {tags_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create four demo tables
# MAGIC
# MAGIC Each table contains exactly 100 deterministic rows. Re-running this cell
# MAGIC replaces the tables and resets the demo data.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`"
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`customers`
    USING DELTA
    AS
    SELECT
      id + 1 AS customer_id,
      concat('customer', id + 1, '@example.com') AS email,
      concat('Customer ', id + 1) AS full_name
    FROM range(100)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`transactions`
    USING DELTA
    AS
    SELECT
      id + 1 AS transaction_id,
      (id % 100) + 1 AS customer_id,
      cast(((id % 25) + 1) * 10.25 AS DECIMAL(12, 2)) AS amount
    FROM range(100)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`products`
    USING DELTA
    AS
    SELECT
      id + 1 AS product_id,
      concat('Product ', id + 1) AS product_name,
      concat('category_', id % 5) AS category
    FROM range(100)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE `{catalog}`.`{schema}`.`orders`
    USING DELTA
    AS
    SELECT
      id + 1 AS order_id,
      (id % 100) + 1 AS customer_id,
      (id % 100) + 1 AS product_id,
      (id % 5) + 1 AS quantity
    FROM range(100)
    """
)

print("Created customers, transactions, products, and orders.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate the setup

# COMMAND ----------

table_names = ("customers", "transactions", "products", "orders")
for table_name in table_names:
    count = spark.table(f"`{catalog}`.`{schema}`.`{table_name}`").count()
    assert count == 100, f"{table_name}: expected 100 rows, found {count}"
    print(f"✓ {table_name}: {count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Apply the desired tags
# MAGIC
# MAGIC The next cell creates `tags.yml` in the configured Unity Catalog Volume.
# MAGIC
# MAGIC **API mode (default):** calls `apply_tags()` against the Volume file
# MAGIC for dry-run and then calls it again for live apply.
# MAGIC
# MAGIC **CLI mode:** the next cell prints commands to download the generated
# MAGIC Volume file and apply it from your local terminal.

# COMMAND ----------

from pathlib import Path

import yaml


tags_document = {
    "version": "1",
    "source_id": "uc-tagging-interactive-demo",
    "defaults": {
        "catalog": catalog,
        "schema": schema,
    },
    "tables": {
        "customers": {
            "table": {
                "sdp_meta_demo_domain": "customer",
                "sdp_meta_demo_managed_by": "sdp-meta",
            },
            "columns": {
                "customer_id": {
                    "sdp_meta_demo_semantic_type": "customer_id",
                },
                "email": {
                    "sdp_meta_demo_classification": "pii",
                },
            },
        },
        "transactions": {
            "table": {
                "sdp_meta_demo_domain": "finance",
                "sdp_meta_demo_managed_by": "sdp-meta",
            },
            "columns": {
                "transaction_id": {
                    "sdp_meta_demo_semantic_type": "transaction_id",
                },
                "amount": {
                    "sdp_meta_demo_classification": "financial",
                },
            },
        },
        "products": {
            "table": {
                "sdp_meta_demo_domain": "product",
                "sdp_meta_demo_managed_by": "sdp-meta",
            },
            "columns": {
                "product_id": {
                    "sdp_meta_demo_semantic_type": "product_id",
                },
                "category": {
                    "sdp_meta_demo_semantic_type": "product_category",
                },
            },
        },
        "orders": {
            "table": {
                "sdp_meta_demo_domain": "commerce",
                "sdp_meta_demo_managed_by": "sdp-meta",
            },
            "columns": {
                "order_id": {
                    "sdp_meta_demo_semantic_type": "order_id",
                },
                "quantity": {
                    "sdp_meta_demo_semantic_type": "quantity",
                },
            },
        },
    },
}

api_tags_path = Path(tags_volume_path)
rendered_tags_yaml = yaml.safe_dump(tags_document, sort_keys=False)
api_tags_path.write_text(rendered_tags_yaml, encoding="utf-8")

print(f"✓ Created tags.yml at {tags_volume_path}\n")
print(rendered_tags_yaml)

if execution_mode == "api":
    try:
        from databricks.labs.sdp_meta.governance.tagging import apply_tags
    except ImportError as error:
        raise RuntimeError(
            "SDP-META is not installed on this notebook compute. Attach the "
            "project wheel or run `%pip install databricks-labs-sdp-meta`, "
            "restart Python, and rerun the notebook."
        ) from error

    print("Running non-mutating plan through the SDP-META Python API...")
    dry_run_code = apply_tags(
        tags_file=str(api_tags_path),
        state_table=state_table,
        dry_run=True,
    )
    assert dry_run_code == 0, f"API dry-run failed with exit code {dry_run_code}"

    print("\nApplying and verifying tags through the SDP-META Python API...")
    apply_code = apply_tags(
        tags_file=str(api_tags_path),
        state_table=state_table,
    )
    assert apply_code == 0, f"API apply failed with exit code {apply_code}"
    print("✓ API apply completed successfully.")
else:
    if not warehouse_id:
        print(
            "Set the warehouse_id widget to print ready-to-run commands, or "
            "replace <warehouse-id> below."
        )
    warehouse_arg = warehouse_id or "<warehouse-id>"
    base_command = (
        "databricks labs sdp-meta apply-tags "
        "--tags-file ./tags.yml "
        f"--state-table {state_table} "
        f"--warehouse-id {warehouse_arg}"
    )

    print("DOWNLOAD GENERATED CONFIGURATION:\n")
    print(
        "databricks fs cp "
        f"dbfs:{tags_volume_path} ./tags.yml --overwrite"
    )
    print("DRY RUN:\n")
    print(f"{base_command} --dry-run")
    print("\nAPPLY:\n")
    print(base_command)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CLI interface reference
# MAGIC
# MAGIC The notebook uses the Python API by default, but customers can manage the
# MAGIC same desired state from a terminal with the SDP-META Labs CLI.
# MAGIC
# MAGIC Preview without changing Unity Catalog or ownership state:
# MAGIC
# MAGIC ```bash
# MAGIC databricks fs cp \
# MAGIC   dbfs:/Volumes/<catalog>/<schema>/<volume>/tags.yml \
# MAGIC   ./tags.yml \
# MAGIC   --overwrite
# MAGIC
# MAGIC databricks labs sdp-meta apply-tags \
# MAGIC   --tags-file ./tags.yml \
# MAGIC   --state-table <catalog>.<schema>.uc_governance_tag_assignments \
# MAGIC   --warehouse-id <sql-warehouse-id> \
# MAGIC   --dry-run
# MAGIC ```
# MAGIC
# MAGIC Apply the reviewed plan:
# MAGIC
# MAGIC ```bash
# MAGIC databricks labs sdp-meta apply-tags \
# MAGIC   --tags-file ./tags.yml \
# MAGIC   --state-table <catalog>.<schema>.uc_governance_tag_assignments \
# MAGIC   --warehouse-id <sql-warehouse-id>
# MAGIC ```
# MAGIC
# MAGIC Use `--profile <databricks-cli-profile>` when the target workspace is not
# MAGIC selected by the default Databricks CLI profile. The same `source_id` and
# MAGIC state table must be reused across runs so ownership-aware cleanup remains
# MAGIC stable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verify table tags
# MAGIC
# MAGIC In API mode, continue directly. In CLI mode, run this section after the
# MAGIC local apply succeeds.

# COMMAND ----------

table_tags = spark.sql(
    f"""
    SELECT table_name, tag_name, tag_value
    FROM `{catalog}`.information_schema.table_tags
    WHERE schema_name = '{schema}'
      AND table_name IN ('customers', 'transactions', 'products', 'orders')
      AND tag_name LIKE 'sdp_meta_demo_%'
    ORDER BY table_name, tag_name
    """
)
display(table_tags)

table_tag_rows = table_tags.collect()
assert len(table_tag_rows) == 8, (
    f"Expected 8 explicit demo table tags, found {len(table_tag_rows)}. "
    "Run apply-tags before verification."
)
assert {row.table_name for row in table_tag_rows} == set(table_names)
print("✓ Verified two table tags on each demo table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verify column tags

# COMMAND ----------

column_tags = spark.sql(
    f"""
    SELECT table_name, column_name, tag_name, tag_value
    FROM `{catalog}`.information_schema.column_tags
    WHERE schema_name = '{schema}'
      AND table_name IN ('customers', 'transactions', 'products', 'orders')
      AND tag_name LIKE 'sdp_meta_demo_%'
    ORDER BY table_name, column_name, tag_name
    """
)
display(column_tags)

column_tag_rows = column_tags.collect()
assert len(column_tag_rows) == 8, (
    f"Expected 8 explicit demo column tags, found {len(column_tag_rows)}. "
    "Run apply-tags before verification."
)
assert {row.table_name for row in column_tag_rows} == set(table_names)
print("✓ Verified two column tags on each demo table.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify ownership state
# MAGIC
# MAGIC Unity Catalog stores the current tags. The Delta state table records why
# MAGIC each assignment exists, who contributes it, and whether it was verified.

# COMMAND ----------

state = spark.table(state_table)
display(
    state.select(
        "table_name",
        "column_name",
        "tag_key",
        "last_applied_value",
        "ownership",
        "contributors",
        "status",
    ).orderBy("table_name", "column_name", "tag_key")
)

state_rows = state.collect()
assert len(state_rows) == 16, (
    f"Expected 16 ownership rows, found {len(state_rows)}"
)
assert all(row.ownership == "script" for row in state_rows)
assert all(row.status == "applied" for row in state_rows)
print("✓ Verified 16 applied, script-owned assignments.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Idempotency
# MAGIC
# MAGIC Rerun the apply cell in API mode, or run the same local `apply-tags`
# MAGIC command again in CLI mode. The plan should contain `noop` actions and
# MAGIC execute no tag DDL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Optional cleanup
# MAGIC
# MAGIC Set the `cleanup` widget to `true` and run this final cell. It drops the
# MAGIC demo schema, including all four tables and the ownership-state table.

# COMMAND ----------

if dbutils.widgets.get("cleanup").lower() == "true":
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{schema}` CASCADE")
    print(f"Dropped {catalog}.{schema}")
else:
    print("Cleanup skipped. Set cleanup=true and rerun this cell when finished.")
