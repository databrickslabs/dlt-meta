"""DataflowPipeline provide generic DLT code using dataflowspec."""
import json
import logging
from typing import Callable
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils
from src.pipeline_writers import AppendFlowWriter, DLTSinkWriter
from src.__about__ import __version__
from src.pipeline_readers import PipelineReaders

logger = logging.getLogger('databricks.labs.dltmeta')
logger.setLevel(logging.INFO)


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
        spark.conf.set("databrickslab.dlt-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
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
        if dataflow_spec.applyChangesFromSnapshot:
            self.appy_changes_from_snapshot = DataflowSpecUtils.get_apply_changes_from_snapshot(
                self.dataflowSpec.applyChangesFromSnapshot
            )            
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        elif isinstance(dataflow_spec, SilverDataflowSpec):
            self.schema_json = None
        self.next_snapshot_and_version = None
        self.next_snapshot_and_version = next_snapshot_and_version
        self.next_snapshot_and_version_from_source_view = False
        if self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format", None):
            self.snapshot_source_format = self.dataflowSpec.sourceDetails["snapshot_format"]
        else:
            self.snapshot_source_format = None
        self.silver_schema = None

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def is_create_view(self):
        """Determine if a view should be created based on source details and snapshot configuration.

        Returns:
            bool: True if a view should be created, False otherwise.
        """
        # if sourceDetails is provided and snapshot_format is delta, then create a view
        # if next_snapshot_and_version is provided, then do not create a view
        # otherwise create a view
        print(f"self.dataflowSpec: {self.dataflowSpec}")
        print(f"self.next_snapshot_and_version: {self.next_snapshot_and_version}")
        print(f"self.dataflowSpec.sourceDetails: {self.dataflowSpec.sourceDetails}")
        print(f"self.dataflowSpec.sourceDetails.get('snapshot_format'): {self.dataflowSpec.sourceDetails.get('snapshot_format')}")
        if (self.dataflowSpec.sourceDetails and self.dataflowSpec.sourceDetails.get("snapshot_format") == "delta"):
            print(f"{self.dataflowSpec.sourceDetails.get('catalog')}.{self.dataflowSpec.sourceDetails.get('source_database')}.{self.dataflowSpec.sourceDetails.get('source_table')} view will be created")
            self.next_snapshot_and_version_from_source_view = True
            return True
        elif self.next_snapshot_and_version:
            print(f"{self.dataflowSpec.sourceDetails.get('catalog')}.{self.dataflowSpec.sourceDetails.get('source_database')}.{self.dataflowSpec.sourceDetails.get('source_table')} view will not be created")
            return False
        return True

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and self.is_create_view():
            dlt.view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec) and self.is_create_view():
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
        if self.dataflowSpec.sinks:
            dlt_sinks = DataflowSpecUtils.get_sinks(self.dataflowSpec.sinks, self.spark)
            for dlt_sink in dlt_sinks:
                DLTSinkWriter(dlt_sink, self.view_name).write_to_sink()
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
            target_cl = bronze_dataflow_spec.targetDetails.get('catalog', None)
            target_cl_name = f"{target_cl}." if target_cl is not None else ''
            target_db_name = bronze_dataflow_spec.targetDetails['database']
            target_table_name = bronze_dataflow_spec.targetDetails['table']

            target_table = (
                f"{target_cl_name}{target_db_name}.{target_table_name}"
            )
            dlt.table(
                self.write_to_delta,
                name=f"{target_table}",
                partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
                table_properties=bronze_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"bronze dlt table{target_table}",
            )
        if bronze_dataflow_spec.appendFlows:
            self.write_append_flows()

    def write_silver(self):
        """Write silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        if silver_dataflow_spec.applyChangesFromSnapshot:
            self.apply_changes_from_snapshot()
        elif silver_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else silver_dataflow_spec.targetDetails["path"]
            target_cl = silver_dataflow_spec.targetDetails.get('catalog', None)
            target_cl_name = f"{target_cl}." if target_cl is not None else ''
            target_db_name = silver_dataflow_spec.targetDetails['database']
            target_table_name = silver_dataflow_spec.targetDetails['table']
            target_table = (
                f"{target_cl_name}{target_db_name}.{target_table_name}"
            )
            target_comment = (
                silver_dataflow_spec.targetDetails.get('comment')
                if "comment" in silver_dataflow_spec.targetDetails
                else f"silver dlt table{target_table}"
            )
            dlt.table(
                self.write_to_delta,
                name=f"{target_table}",
                partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
                table_properties=silver_dataflow_spec.tableProperties,
                path=target_path,
                comment=target_comment,
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
        elif bronze_dataflow_spec.sourceFormat == "delta" or bronze_dataflow_spec.sourceFormat == "snapshot":
            return pipeline_reader.read_dlt_delta()
        elif bronze_dataflow_spec.sourceFormat == "eventhub" or bronze_dataflow_spec.sourceFormat == "kafka":
            return pipeline_reader.read_kafka()
        else:
            raise Exception(f"{bronze_dataflow_spec.sourceFormat} source format not supported")
        return self.apply_custom_transform_fun(input_df)

    def apply_custom_transform_fun(self, input_df):
        if self.custom_transform_func:
            input_df = self.custom_transform_func(input_df, self.dataflowSpec)
            input_df.show(5)
        return input_df

    def get_silver_schema(self):
        """Get Silver table Schema."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_cl = silver_dataflow_spec.sourceDetails.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        raw_delta_table_stream = self.spark.readStream.table(
            f"{source_cl_name}{source_database}.{source_table}"
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
        source_cl = silver_dataflow_spec.sourceDetails.get('catalog', None)
        source_cl_name = f"{source_cl}." if source_cl is not None else ''
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        silver_reader_config_opts = silver_dataflow_spec.readerConfigOptions
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        if silver_reader_config_opts:
            if silver_dataflow_spec.sourceFormat == "snapshot":
                bronze_df = self.spark.read.options(**silver_reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.options(
                    **silver_reader_config_opts
                ).load(
                    path=silver_dataflow_spec.sourceDetails["path"],
                    format="delta"
                )
            else:
                bronze_df = self.spark.readStream.options(**silver_reader_config_opts).table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.options(
                    **silver_reader_config_opts
                ).load(
                    path=silver_dataflow_spec.sourceDetails["path"],
                    format="delta"
                )
        else:
            if silver_dataflow_spec.sourceFormat == "snapshot":
                bronze_df = self.spark.read.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.read.load(
                    path=silver_dataflow_spec.sourceDetails["path"],
                    format="delta"
                )
            else:
                bronze_df = self.spark.readStream.table(
                    f"{source_cl_name}{source_database}.{source_table}"
                ) if self.uc_enabled else self.spark.readStream.load(
                    path=silver_dataflow_spec.sourceDetails["path"],
                    format="delta"
                )
        if select_exp:
            bronze_df = bronze_df.selectExpr(*select_exp)
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    bronze_df = bronze_df.where(where_clause)
        bronze_df = self.apply_custom_transform_fun(bronze_df)
        return bronze_df

    def write_to_delta(self):
        """Write to Delta."""
        return dlt.read_stream(self.view_name)

    def apply_changes_from_snapshot(self):
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]
        self.create_streaming_table(None, target_path)
        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']
        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
        )
        source = (
            (lambda latest_snapshot_version: self.next_snapshot_and_version(
                latest_snapshot_version, self.dataflowSpec
            ))
            if self.next_snapshot_and_version and not self.next_snapshot_and_version_from_source_view
            else self.view_name
        )

        dlt.apply_changes_from_snapshot(
            target=target_table,
            source=source,
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
            target_cl = bronzeDataflowSpec.targetDetails.get('catalog', None)
            target_cl_name = f"{target_cl}." if target_cl is not None else ''
            target_db_name = bronzeDataflowSpec.targetDetails['database']
            target_table_name = bronzeDataflowSpec.targetDetails['table']
            target_table = (
                f"{target_cl_name}{target_db_name}.{target_table_name}"
            )
            target_comment = (
                bronzeDataflowSpec.targetDetails.get('comment')
                if "comment" in bronzeDataflowSpec.targetDetails
                else f"bronze dlt table{target_table}"
            )
            if expect_all_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_all_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{target_table_name}",
                        table_properties=bronzeDataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                        path=target_path,
                        comment=target_comment,
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{target_table_name}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            path=target_path,
                            comment=target_comment,
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
                            name=f"{target_table_name}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            path=target_path,
                            comment=target_comment,
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
                bronze_cl = bronzeDataflowSpec.quarantineTargetDetails.get('catalog', None)
                bronze_cl_name = f"{bronze_cl}." if bronze_cl is not None else ''
                bronze_db = bronzeDataflowSpec.quarantineTargetDetails['database']
                bronze_table = bronzeDataflowSpec.quarantineTargetDetails['table']
                target_table = (
                    f"{bronze_cl_name}{bronze_db}.{bronze_table}"
                )
                target_comment = (
                    bronzeDataflowSpec.quarantineTargetDetails.get('comment')
                    if "comment" in bronzeDataflowSpec.quarantineTargetDetails
                    else f"bronze dlt quarantine_path table{target_table}"
                )
                dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{bronze_table}",
                        table_properties=bronzeDataflowSpec.quarantineTableProperties,
                        partition_cols=q_partition_cols,
                        cluster_by=q_cluster_by,
                        path=target_path,
                        comment=target_comment,
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

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']

        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
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

        target_cl = self.dataflowSpec.targetDetails.get('catalog', None)
        target_cl_name = f"{target_cl}." if target_cl is not None else ''
        target_db_name = self.dataflowSpec.targetDetails['database']
        target_table_name = self.dataflowSpec.targetDetails['table']

        target_table = (
            f"{target_cl_name}{target_db_name}.{target_table_name}"
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
                            bronze_next_snapshot_and_version: Callable = None,
                            silver_next_snapshot_and_version: Callable = None):
        """Invoke dlt pipeline will launch dlt with given dataflowspec.

        Args:
            spark (_type_): _description_
            layer (_type_): _description_
        """

        dataflowspec_list = None
        if "bronze" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", dataflowspec_list, bronze_custom_transform_func, bronze_next_snapshot_and_version
            )
        elif "silver" == layer.lower():
            dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", dataflowspec_list, silver_custom_transform_func, silver_next_snapshot_and_version
            )
        elif "bronze_silver" == layer.lower():
            bronze_dataflowspec_list = DataflowSpecUtils.get_bronze_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "bronze", bronze_dataflowspec_list, bronze_custom_transform_func,
                bronze_next_snapshot_and_version
            )
            silver_dataflowspec_list = DataflowSpecUtils.get_silver_dataflow_spec(spark)
            DataflowPipeline._launch_dlt_flow(
                spark, "silver", silver_dataflowspec_list, silver_custom_transform_func,
                silver_next_snapshot_and_version
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

                qrt_cl = dataflowSpec.quarantineTargetDetails.get('catalog', None)
                qrt_cl_str = f"{qrt_cl}_" if qrt_cl is not None else ''
                qrt_db = dataflowSpec.quarantineTargetDetails['database'].replace('.', '_')
                qrt_table = dataflowSpec.quarantineTargetDetails['table']
                quarantine_input_view_name = (
                    f"{qrt_cl_str}{qrt_db}_{qrt_table}"
                    f"_{layer}_quarantine_inputview"
                )
                quarantine_input_view_name = quarantine_input_view_name.replace(".", "").lower()
            else:
                logger.info("quarantine_input_view_name set to None")
            target_cl = dataflowSpec.targetDetails.get('catalog', None)
            target_cl_str = f"{target_cl}_" if target_cl is not None else ''
            target_db = dataflowSpec.targetDetails['database'].replace('.', '_')
            target_table = dataflowSpec.targetDetails['table']
            target_view_name = f"{target_cl_str}{target_db}_{target_table}_{layer}_inputview"
            target_view_name = target_view_name.replace(".", "").lower()
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                target_view_name,
                quarantine_input_view_name,
                custom_transform_func,
                next_snapshot_and_version
            )
            dlt_data_flow.run_dlt()
