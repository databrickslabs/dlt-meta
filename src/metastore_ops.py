"""Delta metasore ops."""
from delta.tables import DeltaTable
from pyspark.sql.functions import expr


class DeltaPipelinesMetaStoreOps:
    """This class handles all metastore operations for onboarding tables."""

    def __init__(self, spark):
        """Initialize."""
        self.spark = spark

    def create_database(self, database, comments):
        """Create Database."""
        self.try_run_sql(f"CREATE DATABASE IF NOT EXISTS {database} COMMENT '{comments}'")

    def try_run_sql(self, sql):
        """Try running SQL."""
        self.spark.sql(sql)

    def drop_database(self, database):
        """Drop database."""
        self.try_run_sql(f"DROP DATABASE IF EXISTS {database} CASCADE")

    def reset_table_in_metastore(self, database, table, path):
        """Reset table metadata."""
        self.deregister_table_from_metastore(database, table)
        self.register_table_in_metastore(database, table, path)

    def register_table_in_metastore(self, database, table, path):
        """Register table in metastore."""
        final_table_name = f"{database}.{table}"
        self.spark.sql("CREATE TABLE if not exists " + final_table_name + " USING DELTA LOCATION '" + path + "'")

    def deregister_table_from_metastore(self, database, table):
        """Deregister table."""
        self.try_run_sql(f"DROP TABLE IF EXISTS {database}.{table}")

    def get_table_location(self, database, table):
        """Get table location on blobstorage."""
        return self.spark.sql(f"describe extended {database}.{table}").where("col_name='Location'").collect()[0][1]


class DeltaPipelinesInternalTableOps:
    """Delta internal operations."""

    def __init__(self, spark):
        """Initialize."""
        self.spark = spark

    def merge(self, upsert_records, target_table, merge_keys, original_columns):
        """Merge builder using python semantics.

        Code added for selective updates as against update *
        not_to_update_columns has the exclusion list, hard coded for now.
        """
        upsert_records_df = upsert_records
        target_delta_table = DeltaTable.forName(self.spark, target_table)
        # forPath(self.spark, target_table_path)

        new_l = []
        merge_condition = None
        for key in merge_keys:
            new_l.append(f"source.{key} = target.{key}")
        if len(new_l) > 1:
            merge_condition = " AND ".join(new_l)
        else:
            merge_condition = " ".join(new_l)

        if merge_condition is None:
            raise Exception("please provide merge keys")

        not_to_update_columns = ["createDate", "createdBy"]
        update_dict = {}
        for column in original_columns:
            if column not in not_to_update_columns:
                update_dict[column] = "source." + column
        insert_dict = {}
        for column in original_columns:
            insert_dict[column] = "source." + column
        target_delta_table.alias("target").merge(
            upsert_records_df.alias("source"), expr(merge_condition)
        ).whenMatchedUpdate(set=update_dict).whenNotMatchedInsert(values=insert_dict).execute()
