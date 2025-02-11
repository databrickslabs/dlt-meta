"""DataflowPipeline provide generic DLT code using dataflowspec."""
import json
import logging
import dlt
from typing import Callable
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField
from src.__about__ import __version__
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
from src.pipeline_readers import PipelineReaders

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


class AppendFlowWriter:
    """Append Flow Writer class."""

    def __init__(self, spark, append_flow, target, struct_schema, table_properties=None, partition_cols=None,
                 cluster_by=None):
        """Init."""
        self.spark = spark
        self.target = target
        self.append_flow = append_flow
        self.struct_schema = struct_schema
        self.table_properties = table_properties
        self.partition_cols = partition_cols
        self.cluster_by = cluster_by

    def write_af_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(f"{self.append_flow.name}_view")

    def write_flow(self):
        """Write Append Flow."""
        if self.append_flow.create_streaming_table:
            dlt.create_streaming_table(
                name=self.target,
                table_properties=self.table_properties,
                partition_cols=DataflowSpecUtils.get_partition_cols(self.partition_cols),
                cluster_by=DataflowSpecUtils.get_partition_cols(self.cluster_by),
                schema=self.struct_schema,
                expect_all=None,
                expect_all_or_drop=None,
                expect_all_or_fail=None,
            )
        if self.append_flow.comment:
            comment = self.append_flow.comment
        else:
            comment = f"append_flow={self.append_flow.name} for target={self.target}"
        dlt.append_flow(name=self.append_flow.name,
                        target=self.target,
                        comment=comment,
                        spark_conf=self.append_flow.spark_conf,
                        once=self.append_flow.once,
                        )(self.write_af_to_delta)


class DataflowPipeline:
    """This class uses dataflowSpec to launch DLT.

    Raises:
        Exception: "Dataflow not supported!"

    Returns:
        [type]: [description]
    """

    def __init__(self, spark, dataflow_spec, view_name, view_name_quarantine=None,
                 custom_transform_func: Callable = None, next_snapshot_and_version: Callable = None):
        """Initialize Constructor."""
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        if isinstance(dataflow_spec, BronzeDataflowSpec) or isinstance(dataflow_spec, SilverDataflowSpec):
            self.__initialize_dataflow_pipeline(
                spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
            )
        else:
            raise Exception("Dataflow not supported!")

    def __initialize_dataflow_pipeline(
        self, spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func: Callable,
        next_snapshot_and_version: Callable
    ):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        dbp_enabled_str = spark.conf.get("pipelines.schema", None)
        spark.conf.set("databrickslab.dlt-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dpm_enabled = True if dbp_enabled_str else False
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        self.custom_transform_func = custom_transform_func
        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)
        else:
            self.cdcApplyChanges = None
        if dataflow_spec.appendFlows:
            self.appendFlows = DataflowSpecUtils.get_append_flows(dataflow_spec.appendFlows)
        else:
            self.appendFlows = None
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            self.next_snapshot_and_version = next_snapshot_and_version
            if self.next_snapshot_and_version:
                self.appy_changes_from_snapshot = DataflowSpecUtils.get_apply_changes_from_snapshot(
                    self.dataflowSpec.applyChangesFromSnapshot
                )
            else:
                if dataflow_spec.sourceFormat == "snapshot":
                    raise Exception(f"Snapshot reader function not provided for dataflowspec={dataflow_spec}!")
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        else:
            self.schema_json = None
            self.next_snapshot_and_version = None
            self.appy_changes_from_snapshot = None
        self.silver_schema = None

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and not self.next_snapshot_and_version:
            dlt.view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec) and not self.next_snapshot_and_version:
            dlt.view(
                self.read_silver,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        else:
            if not self.next_snapshot_and_version:
                raise Exception("Dataflow read not supported for {}".format(type(self.dataflowSpec)))
        if self.appendFlows:
            self.read_append_flows()

    def read_append_flows(self):
        if self.dataflowSpec.appendFlows:
            append_flows_schema_map = self.dataflowSpec.appendFlowsSchemas
            for append_flow in self.appendFlows:
                flow_schema = None
                if append_flows_schema_map:
                    flow_schema = append_flows_schema_map.get(append_flow.name)
                pipeline_reader = PipelineReaders(
                    self.spark,
                    append_flow.source_format,
                    append_flow.source_details,
                    append_flow.reader_options,
                    json.loads(flow_schema) if flow_schema else None
                )
                if append_flow.source_format == "cloudFiles":
                    dlt.view(pipeline_reader.read_dlt_cloud_files,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "delta":
                    dlt.view(pipeline_reader.read_dlt_delta,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
                elif append_flow.source_format == "eventhub" or append_flow.source_format == "kafka":
                    dlt.view(pipeline_reader.read_kafka,
                             name=f"{append_flow.name}_view",
                             comment=f"append flow input dataset view for {append_flow.name}_view"
                             )
        else:
            raise Exception(f"Append Flows not found for dataflowSpec={self.dataflowSpec}")

    def write(self):
        """Write DLT."""
        if isinstance(self.dataflowSpec, BronzeDataflowSpec):
            self.write_bronze()
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            self.write_silver()
        else:
            raise Exception(f"Dataflow write not supported for type= {type(self.dataflowSpec)}")

    def write_bronze(self):
        """Write Bronze tables."""
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        if bronze_dataflow_spec.sourceFormat and bronze_dataflow_spec.sourceFormat.lower() == "snapshot":
            if self.next_snapshot_and_version:
                self.apply_changes_from_snapshot()
            else:
                raise Exception("Snapshot reader function not provided!")
        elif bronze_dataflow_spec.dataQualityExpectations:
            self.write_bronze_with_dqe()
        elif bronze_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronze_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=f"{bronze_dataflow_spec.targetDetails['table']}",
                partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.clusterBy),
                table_properties=bronze_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"bronze dlt table{bronze_dataflow_spec.targetDetails['table']}",
            )
        if bronze_dataflow_spec.appendFlows:
            self.write_append_flows()

    def write_silver(self):
        """Write silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        if silver_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else silver_dataflow_spec.targetDetails["path"]
            dlt.table(
                self.write_to_delta,
                name=f"{silver_dataflow_spec.targetDetails['table']}",
                partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.clusterBy),
                table_properties=silver_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"silver dlt table{silver_dataflow_spec.targetDetails['table']}",
            )
        if silver_dataflow_spec.appendFlows:
            self.write_append_flows()

    def read_bronze(self) -> DataFrame:
        """Read Bronze Table."""
        logger.info("In read_bronze func")
        pipeline_reader = PipelineReaders(
            self.spark,
            self.dataflowSpec.sourceFormat,
            self.dataflowSpec.sourceDetails,
            self.dataflowSpec.readerConfigOptions,
            self.schema_json
        )
        bronze_dataflow_spec: BronzeDataflowSpec = self.dataflowSpec
        input_df = None
        if bronze_dataflow_spec.sourceFormat == "cloudFiles":
            input_df = pipeline_reader.read_dlt_cloud_files()
        elif bronze_dataflow_spec.sourceFormat == "delta":
            return pipeline_reader.read_dlt_delta()
        elif bronze_dataflow_spec.sourceFormat == "eventhub" or bronze_dataflow_spec.sourceFormat == "kafka":
            return pipeline_reader.read_kafka()
        else:
            raise Exception(f"{bronze_dataflow_spec.sourceFormat} source format not supported")
        return self.apply_custom_transform_fun(input_df)

    def apply_custom_transform_fun(self, input_df):
        if self.custom_transform_func:
            input_df = self.custom_transform_func(input_df, self.dataflowSpec)
        return input_df

    def get_silver_schema(self):
        """Get Silver table Schema."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
            path=silver_dataflow_spec.sourceDetails["path"],
            format="delta"
        ).selectExpr(*select_exp)
        raw_delta_table_stream = self.__apply_where_clause(where_clause, raw_delta_table_stream)
        return raw_delta_table_stream.schema

    def __apply_where_clause(self, where_clause, raw_delta_table_stream):
        """This method apply where clause provided in silver transformations

        Args:
            where_clause (_type_): _description_
            raw_delta_table_stream (_type_): _description_

        Returns:
            _type_: _description_
        """
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)
        return raw_delta_table_stream

    def read_silver(self) -> DataFrame:
        """Read Silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_database}.{source_table}"
        ).selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
            path=silver_dataflow_spec.sourceDetails["path"],
            format="delta"
        ).selectExpr(*select_exp)

        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)
        return self.apply_custom_transform_fun(raw_delta_table_stream)

    def write_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(self.view_name)

    def apply_changes_from_snapshot(self):
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        self.create_streaming_table(None, target_path)
        dlt.apply_changes_from_snapshot(
            target=f"{self.dataflowSpec.targetDetails['table']}",
            source=lambda latest_snapshot_version:
            self.next_snapshot_and_version(latest_snapshot_version,
                                           self.dataflowSpec
                                           ),
            keys=self.appy_changes_from_snapshot.keys,
            stored_as_scd_type=self.appy_changes_from_snapshot.scd_type,
            track_history_column_list=self.appy_changes_from_snapshot.track_history_column_list,
            track_history_except_column_list=self.appy_changes_from_snapshot.track_history_except_column_list,
        )

    def write_bronze_with_dqe(self):
        """Write Bronze table with data quality expectations."""
        bronzeDataflowSpec: BronzeDataflowSpec = self.dataflowSpec
        data_quality_expectations_json = json.loads(bronzeDataflowSpec.dataQualityExpectations)

        dlt_table_with_expectation = None
        expect_or_quarantine_dict = None
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        if "expect_or_quarantine" in data_quality_expectations_json:
            expect_or_quarantine_dict = data_quality_expectations_json["expect_or_quarantine"]
        if bronzeDataflowSpec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronzeDataflowSpec.targetDetails["path"]
            if expect_all_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_all_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{bronzeDataflowSpec.targetDetails['table']}",
                        table_properties=bronzeDataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                        path=target_path,
                        comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{bronzeDataflowSpec.targetDetails['table']}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt_table_with_expectation)
            if expect_all_or_drop_dict:
                if expect_all_dict is None and expect_all_or_fail_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{bronzeDataflowSpec.targetDetails['table']}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table{bronzeDataflowSpec.targetDetails['table']}",
                        )
                    )
                else:
                    dlt_table_with_expectation = dlt.expect_all_or_drop(expect_all_or_drop_dict)(
                        dlt_table_with_expectation)
            if expect_or_quarantine_dict:
                q_partition_cols = None
                q_cluster_by = None
                if (
                    "partition_columns" in bronzeDataflowSpec.quarantineTargetDetails
                    and bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]
                ):
                    q_partition_cols = [bronzeDataflowSpec.quarantineTargetDetails["partition_columns"]]

                if (
                        "cluster_by" in bronzeDataflowSpec.quarantineTargetDetails
                        and bronzeDataflowSpec.quarantineTargetDetails["cluster_by"]
                ):
                    q_cluster_by = DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.
                                                                        quarantineTargetDetails['cluster_by'])

                target_path = None if self.uc_enabled else bronzeDataflowSpec.quarantineTargetDetails["path"]
                dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{bronzeDataflowSpec.quarantineTargetDetails['table']}",
                        table_properties=bronzeDataflowSpec.quarantineTableProperties,
                        partition_cols=q_partition_cols,
                        cluster_by=q_cluster_by,
                        path=target_path,
                        comment=f"""bronze dlt quarantine_path table
                        {bronzeDataflowSpec.quarantineTargetDetails['table']}""",
                    )
                )

    def write_append_flows(self):
        """Creates an append flow for the target specified in the dataflowSpec.

        This method creates a streaming table with the given schema and target path.
        It then appends the flow to the table using the specified parameters.

        Args:
            None

        Returns:
            None
        """
        for append_flow in self.appendFlows:
            struct_schema = None
            if self.schema_json:
                struct_schema = (
                    StructType.fromJson(self.schema_json)
                    if isinstance(self.dataflowSpec, BronzeDataflowSpec)
                    else self.silver_schema
                )
            append_flow_writer = AppendFlowWriter(
                self.spark, append_flow,
                self.dataflowSpec.targetDetails['table'],
                struct_schema,
                self.dataflowSpec.tableProperties,
                self.dataflowSpec.partitionColumns,
                self.dataflowSpec.clusterBy
            )
            append_flow_writer.write_flow()

    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        struct_schema = None
        if self.schema_json:
            struct_schema = self.modify_schema_for_cdc_changes(cdc_apply_changes)

        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        self.create_streaming_table(struct_schema, target_path)

        apply_as_deletes = None
        if cdc_apply_changes.apply_as_deletes:
            apply_as_deletes = expr(cdc_apply_changes.apply_as_deletes)

        apply_as_truncates = None
        if cdc_apply_changes.apply_as_truncates:
            apply_as_truncates = expr(cdc_apply_changes.apply_as_truncates)
        target_table = (
            f"{self.dataflowSpec.targetDetails['database']}.{self.dataflowSpec.targetDetails['table']}"
            if self.uc_enabled and self.dpm_enabled
            else self.dataflowSpec.targetDetails['table']
        )
        dlt.apply_changes(
            target=target_table,
            source=self.view_name,
            keys=cdc_apply_changes.keys,
            sequence_by=cdc_apply_changes.sequence_by,
            where=cdc_apply_changes.where,
            ignore_null_updates=cdc_apply_changes.ignore_null_updates,
            apply_as_deletes=apply_as_deletes,
            apply_as_truncates=apply_as_truncates,
            column_list=cdc_apply_changes.column_list,
            except_column_list=cdc_apply_changes.except_column_list,
            stored_as_scd_type=cdc_apply_changes.scd_type,
            track_history_column_list=cdc_apply_changes.track_history_column_list,
            track_history_except_column_list=cdc_apply_changes.track_history_except_column_list,
            flow_name=cdc_apply_changes.flow_name,
            once=cdc_apply_changes.once,
            ignore_null_updates_column_list=cdc_apply_changes.ignore_null_updates_column_list,
            ignore_null_updates_except_column_list=cdc_apply_changes.ignore_null_updates_except_column_list
        )

    def modify_schema_for_cdc_changes(self, cdc_apply_changes):
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.schema_json is None:
            return None
        if isinstance(self.dataflowSpec, SilverDataflowSpec) and self.silver_schema is None:
            return None

        struct_schema = (
            StructType.fromJson(self.schema_json)
            if isinstance(self.dataflowSpec, BronzeDataflowSpec)
            else self.silver_schema
        )

        sequenced_by_data_type = None

        if cdc_apply_changes.except_column_list:
            modified_schema = StructType([])
            if struct_schema:
                for field in struct_schema.fields:
                    if field.name not in cdc_apply_changes.except_column_list:
                        modified_schema.add(field)
                    if field.name == cdc_apply_changes.sequence_by:
                        sequenced_by_data_type = field.dataType
                struct_schema = modified_schema
            else:
                raise Exception(f"Schema is None for {self.dataflowSpec} for cdc_apply_changes! ")

        if struct_schema and cdc_apply_changes.scd_type == "2":
            struct_schema.add(StructField("__START_AT", sequenced_by_data_type))
            struct_schema.add(StructField("__END_AT", sequenced_by_data_type))
        return struct_schema

    def create_streaming_table(self, struct_schema, target_path=None):
        expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict = self.get_dq_expectations()
        target_table = (
            f"{self.dataflowSpec.targetDetails['database']}.{self.dataflowSpec.targetDetails['table']}"
            if self.uc_enabled and self.dpm_enabled
            else self.dataflowSpec.targetDetails['table']
        )
        dlt.create_streaming_table(
            name=target_table,
            table_properties=self.dataflowSpec.tableProperties,
            partition_cols=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.partitionColumns),
            cluster_by=DataflowSpecUtils.get_partition_cols(self.dataflowSpec.clusterBy),
            path=target_path,
            schema=struct_schema,
            expect_all=expect_all_dict,
            expect_all_or_drop=expect_all_or_drop_dict,
            expect_all_or_fail=expect_all_or_fail_dict,
        )

    def get_dq_expectations(self):
        """
        Retrieves the data quality expectations for the table.

        Returns:
            A tuple containing three dictionaries:
            - expect_all_dict: A dictionary containing the 'expect_all' data quality expectations.
            - expect_all_or_drop_dict: A dictionary containing the 'expect_all_or_drop' data quality expectations.
            - expect_all_or_fail_dict: A dictionary containing the 'expect_all_or_fail' data quality expectations.
        """
        expect_all_dict = None
        expect_all_or_drop_dict = None
        expect_all_or_fail_dict = None
        if self.table_has_expectations():
            data_quality_expectations_json = json.loads(self.dataflowSpec.dataQualityExpectations)
            if "expect_all" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect_all"]
            if "expect" in data_quality_expectations_json:
                expect_all_dict = data_quality_expectations_json["expect"]
            if "expect_all_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_all_or_drop"]
            if "expect_or_drop" in data_quality_expectations_json:
                expect_all_or_drop_dict = data_quality_expectations_json["expect_or_drop"]
            if "expect_all_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_all_or_fail"]
            if "expect_or_fail" in data_quality_expectations_json:
                expect_all_or_fail_dict = data_quality_expectations_json["expect_or_fail"]
        return expect_all_dict, expect_all_or_drop_dict, expect_all_or_fail_dict

    def run_dlt(self):
        """Run DLT."""
        logger.info("in run_dlt function")
        self.read()
        self.write()

    @staticmethod
    def invoke_dlt_pipeline(spark,
                            layer,
                            bronze_custom_transform_func: Callable = None,
                            silver_custom_transform_func: Callable = None,
                            next_snapshot_and_version: Callable = None):
        """Invoke dlt pipeline will launch dlt with given dataflowspec.

        Args:
            spark (_type_): _description_
            layer (_type_): _description_
        """
        dataflowspec_list = None
        if "bronze" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", dataflowspec_list, bronze_custom_transform_func, next_snapshot_and_version
            )
        elif "silver" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", dataflowspec_list, silver_custom_transform_func, next_snapshot_and_version
            )
        elif "bronze_silver" == layer.lower():
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func
            )
            silver_dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", silver_dataflowspec_list, silver_custom_transform_func
            )

    @staticmethod
    def _launch_dlt_flow(
        spark, layer, dataflowspec_list, custom_transform_func=None, next_snapshot_and_version: Callable = None
    ):
        for dataflowSpec in dataflowspec_list:
            logger.info("Printing Dataflow Spec")
            logger.info(dataflowSpec)
            quarantine_input_view_name = None
            if isinstance(dataflowSpec, BronzeDataflowSpec) and dataflowSpec.quarantineTargetDetails is not None \
                    and dataflowSpec.quarantineTargetDetails != {}:
                quarantine_input_view_name = (
                    f"{dataflowSpec.quarantineTargetDetails['table']}"
                    f"_{layer}_quarantine_inputView"
                )
            else:
                logger.info("quarantine_input_view_name set to None")
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                f"{dataflowSpec.targetDetails['table']}_{layer}_inputView",
                quarantine_input_view_name,
                custom_transform_func,
                next_snapshot_and_version
            )
            dlt_data_flow.run_dlt()
