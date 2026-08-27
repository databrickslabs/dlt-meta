"""Test for MetastoreOps class."""
from tests.utils import DLTFrameworkTestCase


class MetastoreOpsTests(DLTFrameworkTestCase):
    """Test for MetastoreOps class."""

    def test_createDatabase(self):
        """Test create database."""
        db_name = "meta_store_test"
        comments = "unit test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments)
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertTrue(db_created)

    def test_tryRunningSql(self):
        """Test tryRunningsql."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.try_run_sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertTrue(db_created)

    def test_dropDatabase(self):
        """Test dropDatabase."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        self.deltaPipelinesMetaStoreOps.drop_database(db_name)
        db_list = self.spark.sql("SHOW DATABASES").collect()
        db_created = False
        for db in db_list:
            if db["namespace"] == db_name:
                db_created = True
        self.assertFalse(db_created)

    def test_resetTableInMetastore(self):
        """Test resetTable in metastore."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)
        self.deltaPipelinesMetaStoreOps.reset_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)

    def test_registerTableInMetastore(self):
        """Test registerTables in metastore."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)

    def test_deregisterTableFromMetastore(self):
        """Test deregistering tables."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertTrue(len(table_name) > 0)
        self.deltaPipelinesMetaStoreOps.deregister_table_from_metastore(db_name, table)
        table_list = self.spark.sql(f"show tables in {db_name}")
        table_name = table_list.filter(table_list.tableName == table).collect()
        self.assertEqual(len(table_name), 0)

    def test_getTableLocation_positive(self):
        """Get Table location test."""
        db_name = "meta_store_test"
        self.deltaPipelinesMetaStoreOps.create_database(db_name, comments="unit test")
        path = self.temp_delta_tables_path + "/temp_delta_table"
        table = "test_temp_delta_table"

        dept = [("Finance", 10), ("Marketing", 20), ("Sales", 30), ("IT", 40)]
        deptColumns = ["dept_name", "dept_id"]
        deptDF = self.spark.createDataFrame(data=dept, schema=deptColumns)
        deptDF.write.format("delta").save(path)

        self.deltaPipelinesMetaStoreOps.register_table_in_metastore(db_name, table, path)
        location = self.deltaPipelinesMetaStoreOps.get_table_location(db_name, table)
        self.assertEqual(location, f"file:{path}")
