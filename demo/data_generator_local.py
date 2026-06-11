"""
Standalone script to generate techsummit demo data on the laptop.
Run with: python demo/data_generator_local.py --local_output_dir <dir> --uc_volume_path <path> [options]

Requires: pip install pyspark dbldatagen
"""

import argparse
import json
import os


def parse_args():
    parser = argparse.ArgumentParser(description="Generate techsummit demo data locally")
    parser.add_argument("--local_output_dir", required=True, help="Local directory to write generated files")
    parser.add_argument("--uc_volume_path", required=True, help="UC volume path (used in onboarding JSON paths)")
    parser.add_argument("--table_count", type=int, default=100, help="Number of tables to generate")
    parser.add_argument("--table_column_count", type=int, default=5, help="Columns per table")
    parser.add_argument("--table_data_rows_count", type=int, default=10, help="Rows per table")
    parser.add_argument("--uc_catalog_name", required=True, help="Unity Catalog name")
    parser.add_argument("--dlt_meta_schema", required=True, help="DLT meta schema")
    parser.add_argument("--bronze_schema", required=True, help="Bronze schema")
    parser.add_argument("--silver_schema", required=True, help="Silver schema")
    return parser.parse_args()


def generate_table_data(spark, base_path, column_count, data_rows, table_count):
    """Generate CSV files for each table using dbldatagen clone()."""
    import dbldatagen as dg
    from pyspark.sql.types import FloatType, IntegerType, StringType

    table_base = f"{base_path}/resources/data/input/table"
    base_spec = (
        dg.DataGenerator(spark, name="dlt_meta_demo", rows=data_rows, partitions=4)
        .withIdOutput()
        .withColumn(
            "r",
            FloatType(),
            expr="floor(rand() * 350) * (86400 + 3600)",
            numColumns=column_count,
        )
        .withColumn("code1", IntegerType(), minValue=100, maxValue=(table_count + 200))
        .withColumn("code2", IntegerType(), minValue=1, maxValue=(table_count + 10))
        .withColumn("code3", StringType(), values=["a", "b", "c"])
        .withColumn("code4", StringType(), values=["a", "b", "c"], random=True)
        .withColumn("code5", StringType(), values=["a", "b", "c"], random=True, weights=[9, 1, 1])
    )
    for i in range(1, table_count + 1):
        df = base_spec.clone().build()
        out_path = f"{table_base}_{i}"
        df.coalesce(1).write.mode("overwrite").option("header", "True").csv(out_path)


def generate_onboarding_file(base_path, uc_volume_path, table_count, uc_catalog_name, bronze_schema, silver_schema):
    """Generate onboarding.json with paths pointing to uc_volume_path."""
    dbfs_path = uc_volume_path
    records = []
    for row_id in range(1, table_count + 1):
        table_name = f"table_{row_id}"
        input_path = f"{uc_volume_path}resources/data/input/table_{row_id}"
        records.append({
            "data_flow_id": row_id,
            "data_flow_group": "A1",
            "source_system": "mysql",
            "source_format": "cloudFiles",
            "source_details": {
                "source_database": "demo",
                "source_table": table_name,
                "source_path_prod": input_path,
            },
            "bronze_database_prod": f"{uc_catalog_name}.{bronze_schema}",
            "bronze_table": table_name,
            "bronze_reader_options": {
                "cloudFiles.format": "csv",
                "cloudFiles.rescuedDataColumn": "_rescued_data",
                "header": "true",
            },
            "bronze_data_quality_expectations_json_prod": f"{dbfs_path}conf/dqe/dqe.json",
            "bronze_database_quarantine_prod": f"{uc_catalog_name}.{bronze_schema}",
            "bronze_quarantine_table": f"{table_name}_quarantine",
            "silver_database_prod": f"{uc_catalog_name}.{silver_schema}",
            "silver_table": table_name,
            "silver_transformation_json_prod": f"{dbfs_path}conf/silver_transformations.json",
            "silver_data_quality_expectations_json_prod": f"{dbfs_path}conf/dqe/silver_dqe.json",
        })
    onboarding = records  # The onboard command expects a JSON array
    conf_dir = f"{base_path}/conf"
    os.makedirs(conf_dir, exist_ok=True)
    with open(f"{conf_dir}/onboarding.json", "w") as f:
        json.dump(onboarding, f, indent=2)


def generate_silver_transformation_json(base_path, table_count, column_count):
    """Generate silver_transformations.json with dynamic column references."""
    r_cols = [f"r_{j}" for j in range(column_count)]
    records = []
    for row_id in range(1, table_count + 1):
        select_exp = (
            ["id"]
            + r_cols
            + [
                "concat(code1,' ',code2) as new_code",
                "code3",
                "code4",
                "code5",
                "_rescued_data",
            ]
        )
        records.append({"target_table": f"table_{row_id}", "select_exp": select_exp})
    conf_dir = f"{base_path}/conf"
    os.makedirs(conf_dir, exist_ok=True)
    with open(f"{conf_dir}/silver_transformations.json", "w") as f:
        json.dump(records, f, indent=2)


def generate_dqe_json(base_path):
    """Generate DQE config files."""
    dqe_json = {
        "expect_or_drop": {
            "no_rescued_data": "_rescued_data IS NULL",
            "valid_product_id": "id IS NOT NULL AND id>0",
        },
        "expect_or_quarantine": {
            "quarantine_rule": "_rescued_data IS NOT NULL OR id IS NULL OR id=0",
        },
    }
    silver_dqe_json = {
        "expect_or_drop": {
            "valid_product_id": "id IS NOT NULL AND id>0",
        },
    }
    dqe_dir = f"{base_path}/conf/dqe"
    os.makedirs(dqe_dir, exist_ok=True)
    with open(f"{dqe_dir}/dqe.json", "w") as f:
        json.dump(dqe_json, f, indent=2)
    with open(f"{dqe_dir}/silver_dqe.json", "w") as f:
        json.dump(silver_dqe_json, f, indent=2)


def main():
    args = parse_args()
    base_path = args.local_output_dir
    uc_volume_path = args.uc_volume_path.rstrip("/") + "/"

    os.makedirs(f"{base_path}/resources/data/input", exist_ok=True)

    print("Starting Spark session (local)...")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("DLT-META_TECH_SUMMIT_LOCAL")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )

    print(f"Generating {args.table_count} tables with {args.table_column_count} cols, {args.table_data_rows_count} rows...")
    generate_table_data(
        spark, base_path, args.table_column_count, args.table_data_rows_count, args.table_count
    )

    print("Generating onboarding.json...")
    generate_onboarding_file(
        base_path,
        uc_volume_path,
        args.table_count,
        args.uc_catalog_name,
        args.bronze_schema,
        args.silver_schema,
    )

    print("Generating silver_transformations.json...")
    generate_silver_transformation_json(base_path, args.table_count, args.table_column_count)

    print("Generating DQE configs...")
    generate_dqe_json(base_path)

    spark.stop()
    print(f"Data generation complete. Output: {base_path}")


if __name__ == "__main__":
    main()
