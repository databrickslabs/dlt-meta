"""DataflowPipeline provide generic DLT code using dataflowspec."""
import json
import logging
from functools import reduce
from typing import Dict, Callable, Optional, List, Union, Any, Tuple

import dlt
from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.functions import expr, col
from pyspark.sql.types import StructType, StructField, NumericType, StringType
from src.__about__ import __version__
from src.dataflow_spec import BronzeDataflowSpec, SilverDataflowSpec, DataflowSpecUtils, \
    CDCApplyChanges, BronzeCDCApplyChangesFromSnapshot, SilverCDCApplyChangesFromSnapshot
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

    def __init__(
            self,
            spark,
            dataflow_spec,
            view_name,
            view_name_quarantine=None,
            custom_transform_func=None,
            next_snapshot_and_version: Optional[
                Callable[[Any, Union[BronzeDataflowSpec, SilverDataflowSpec]],  Optional[Tuple[DataFrame, Any]]]
            ] = None
    ):
        """Initialize Constructor."""
        logger.info(
            f"""dataflowSpec={dataflow_spec} ,
                view_name={view_name},
                view_name_quarantine={view_name_quarantine}"""
        )
        self.__full_refresh = None
        if isinstance(dataflow_spec, BronzeDataflowSpec) or isinstance(dataflow_spec, SilverDataflowSpec):
            self.__initialize_dataflow_pipeline(
                spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
            )
        else:
            raise Exception("Dataflow not supported!")

    def __initialize_dataflow_pipeline(
            self, spark, dataflow_spec, view_name, view_name_quarantine, custom_transform_func, next_snapshot_and_version
    ):
        """Initialize dataflow pipeline state."""
        self.spark = spark
        self.dbutils = self.get_db_utils()
        self.spark_df_list = []
        uc_enabled_str = spark.conf.get("spark.databricks.unityCatalog.enabled", "False")
        dbp_enabled_str = spark.conf.get("pipelines.schema", None)
        spark.conf.set("databrickslab.dlt-meta.version", f"{__version__}")
        uc_enabled_str = uc_enabled_str.lower()
        self.uc_enabled = True if uc_enabled_str == "true" else False
        self.dataflowSpec: Union[BronzeDataflowSpec, SilverDataflowSpec] = dataflow_spec
        self.dpm_enabled = True if dbp_enabled_str else False
        self.dataflowSpec = dataflow_spec
        self.view_name = view_name
        if view_name_quarantine:
            self.view_name_quarantine = view_name_quarantine
        self.custom_transform_func = custom_transform_func
        self.apply_changes_from_snapshot: (
            Optional[Union[BronzeCDCApplyChangesFromSnapshot, SilverCDCApplyChangesFromSnapshot]]) = None
        self.cdcApplyChanges: Optional[CDCApplyChanges] = None
        self.snapshot_df = None
        self.next_snapshot_and_version = next_snapshot_and_version

        if dataflow_spec.cdcApplyChanges:
            self.cdcApplyChanges = DataflowSpecUtils.get_cdc_apply_changes(self.dataflowSpec.cdcApplyChanges)

        if dataflow_spec.appendFlows:
            self.appendFlows = DataflowSpecUtils.get_append_flows(dataflow_spec.appendFlows)
        else:
            self.appendFlows = None
        if isinstance(dataflow_spec, BronzeDataflowSpec):
            if dataflow_spec.cdcApplyChangesFromSnapshot:
                self.cdcApplyChanges = None
                self.apply_changes_from_snapshot = BronzeCDCApplyChangesFromSnapshot.from_json(
                    dataflow_spec.cdcApplyChangesFromSnapshot
                )
            if dataflow_spec.schema is not None:
                self.schema_json = json.loads(dataflow_spec.schema)
            else:
                self.schema_json = None
        else:
            if dataflow_spec.cdcApplyChangesFromSnapshot:
                self.cdcApplyChanges = None
                self.apply_changes_from_snapshot = SilverCDCApplyChangesFromSnapshot.from_json(
                    dataflow_spec.cdcApplyChangesFromSnapshot
                )
            self.schema_json = None
        if isinstance(dataflow_spec, SilverDataflowSpec):
            self.silver_schema = self.get_silver_schema()
        else:
            self.silver_schema = None
            self.next_snapshot_and_version = None
            self.appy_changes_from_snapshot = None
        self.silver_schema = None

    @property
    def full_refresh(self):
        if self.__full_refresh is None:
            w = WorkspaceClient()
            pipeline_id = self.spark.conf.get('pipelines.id')
            update_id = w.pipelines.get(pipeline_id).latest_updates[0].update_id
            update_info = w.pipelines.get_update(pipeline_id, update_id).update
            self.__full_refresh = update_info.full_refresh
        return self.__full_refresh

    def table_has_expectations(self):
        """Table has expectations check."""
        return self.dataflowSpec.dataQualityExpectations is not None

    def read(self):
        """Read DLT."""
        logger.info("In read function")
        if isinstance(self.dataflowSpec, BronzeDataflowSpec) and not self.snapshot_function:
            dlt.view(
                self.read_bronze,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        elif isinstance(self.dataflowSpec, SilverDataflowSpec) and not self.snapshot_function:
            dlt.view(
                self.read_silver,
                name=self.view_name,
                comment=f"input dataset view for {self.view_name}",
            )
        else:
            if not self.snapshot_function:
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
            if self.snapshot_function:
                self.apply_changes_from_snapshot_bronze()
            else:
                raise Exception("Snapshot reader function not provided!")
        elif bronze_dataflow_spec.dataQualityExpectations:
            self.write_bronze_with_dqe()
        elif bronze_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        else:
            target_path = None if self.uc_enabled else bronze_dataflow_spec.targetDetails["path"]
            target_table = (
                f"{bronze_dataflow_spec.targetDetails['database']}.{bronze_dataflow_spec.targetDetails['table']}"
                if self.uc_enabled and self.dpm_enabled
                else bronze_dataflow_spec.targetDetails['table']
            )
            dlt.table(
                self.write_to_delta,
                name=f"{target_table}",
                partition_cols=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(bronze_dataflow_spec.clusterBy),
                table_properties=bronze_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"bronze dlt table{target_table}",
            )
        if bronze_dataflow_spec.appendFlows:
            self.write_append_flows()

    def write_silver(self):
        """Write silver tables."""
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        if silver_dataflow_spec.cdcApplyChanges:
            self.cdc_apply_changes()
        elif silver_dataflow_spec.cdcApplyChangesFromSnapshot:
            self.cdc_apply_changes_from_snapshot()
        else:
            target_path = None if self.uc_enabled else silver_dataflow_spec.targetDetails["path"]
            target_table = (
                f"{silver_dataflow_spec.targetDetails['database']}.{silver_dataflow_spec.targetDetails['table']}"
                if self.uc_enabled and self.dpm_enabled
                else silver_dataflow_spec.targetDetails['table']
            )
            dlt.table(
                self.write_to_delta,
                name=f"{target_table}",
                partition_cols=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.partitionColumns),
                cluster_by=DataflowSpecUtils.get_partition_cols(silver_dataflow_spec.clusterBy),
                table_properties=silver_dataflow_spec.tableProperties,
                path=target_path,
                comment=f"silver dlt table{target_table}",
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

    def read_silver(self, options: Optional[Dict[str, str]] = None) -> DataFrame:
        """Read Silver tables."""
        options = options or {}
        silver_dataflow_spec: SilverDataflowSpec = self.dataflowSpec
        source_database = silver_dataflow_spec.sourceDetails["database"]
        source_table = silver_dataflow_spec.sourceDetails["table"]
        select_exp = silver_dataflow_spec.selectExp
        where_clause = silver_dataflow_spec.whereClause
        logger.info(f"Reading custom stream for {source_database}.{source_table}")

        raw_delta_table_stream = self.spark.readStream
        for key, value in options.items():
            raw_delta_table_stream = raw_delta_table_stream.option(key, value)
        raw_delta_table_stream = (
            raw_delta_table_stream
            .table(f"{source_database}.{source_table}")
            .selectExpr(*select_exp) if self.uc_enabled else self.spark.readStream.load(
                path=silver_dataflow_spec.sourceDetails["path"],
                format="delta"
            ).selectExpr(*select_exp)
        )
        if where_clause:
            where_clause_str = " ".join(where_clause)
            if len(where_clause_str.strip()) > 0:
                for where_clause in where_clause:
                    raw_delta_table_stream = raw_delta_table_stream.where(where_clause)

        # Apply change from snapshot, but no primary keys are defined. Default to hashing all non-metadata columns.
        if self.apply_changes_from_snapshot and not self.apply_changes_from_snapshot.keys:
            if self.apply_changes_from_snapshot.track_history_except_column_list:
                raw_delta_table_stream = self.add_hash_column(
                    raw_delta_table_stream,
                    except_columns=self.apply_changes_from_snapshot.track_history_except_column_list
                )
            elif self.apply_changes_from_snapshot.track_history_column_list:
                raw_delta_table_stream = self.add_hash_column(
                    raw_delta_table_stream,
                    include_columns=self.apply_changes_from_snapshot.track_history_column_list
                )
            self.apply_changes_from_snapshot.keys = ["__DLT_HASH__"]
        return self.apply_custom_transform_fun(raw_delta_table_stream)

    def write_to_delta(self) -> DataFrame:
        """Write to Delta."""
        return dlt.read_stream(self.view_name)

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
            target_table = (
                f"{bronzeDataflowSpec.targetDetails['database']}.{bronzeDataflowSpec.targetDetails['table']}"
                if self.uc_enabled and self.dpm_enabled
                else bronzeDataflowSpec.targetDetails['table']
            )
            if expect_all_dict:
                dlt_table_with_expectation = dlt.expect_all(expect_all_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{target_table}",
                        table_properties=bronzeDataflowSpec.tableProperties,
                        partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                        cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                        path=target_path,
                        comment=f"bronze dlt table{target_table}",
                    )
                )
            if expect_all_or_fail_dict:
                if expect_all_dict is None:
                    dlt_table_with_expectation = dlt.expect_all_or_fail(expect_all_or_fail_dict)(
                        dlt.table(
                            self.write_to_delta,
                            name=f"{target_table}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table{target_table}",
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
                            name=f"{target_table}",
                            table_properties=bronzeDataflowSpec.tableProperties,
                            partition_cols=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.partitionColumns),
                            cluster_by=DataflowSpecUtils.get_partition_cols(bronzeDataflowSpec.clusterBy),
                            path=target_path,
                            comment=f"bronze dlt table{target_table}",
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
                bronze_db = bronzeDataflowSpec.quarantineTargetDetails['database']
                bronze_table = bronzeDataflowSpec.quarantineTargetDetails['table']

                target_table = (
                    f"{bronze_db}.{bronze_table}"
                    if self.uc_enabled and self.dpm_enabled
                    else bronze_table
                )
                dlt.expect_all_or_drop(expect_or_quarantine_dict)(
                    dlt.table(
                        self.write_to_delta,
                        name=f"{target_table}",
                        table_properties=bronzeDataflowSpec.quarantineTableProperties,
                        partition_cols=q_partition_cols,
                        cluster_by=q_cluster_by,
                        path=target_path,
                        comment=f"""bronze dlt quarantine_path table{target_table}""",
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

    @staticmethod
    def add_hash_column(df, include_columns: Optional[List[str]] = None, except_columns: Optional[List[str]] = None):
        if include_columns and except_columns:
            raise ValueError("Cannot set both include and except columns - pick to either exclude or include.")
        if not include_columns and not except_columns:
            raise ValueError ("Must set either include or except columns.")

        if include_columns:
            query = F.concat_ws("", *(
                F.col(f"`{c}`").cast("string") for c in df.columns if c in include_columns
            )
                                )
        else:
            query = F.concat_ws("", *(
                F.col(f"`{c}`").cast("string") for c in df.columns if c not in except_columns
            )
                                )

        return df.withColumn(
            "__DLT_HASH__",
            F.sha2(query, 256)
        )

    def cdc_get_schema(self, except_column_list, sequence_by, scd_type, no_keys: bool):
        struct_schema = (
            StructType.fromJson(self.schema_json)
            if isinstance(self.dataflowSpec, BronzeDataflowSpec)
            else self.silver_schema
        )

        sequenced_by_data_type = None

        if struct_schema:
            logger.info(f"Field names: {struct_schema.fieldNames()}")
            modified_schema = StructType([]) if except_column_list else None

            for field in struct_schema.fields:
                if modified_schema is not None and (field.name not in except_column_list):
                    modified_schema.add(field)
                if field.name == sequence_by:
                    sequenced_by_data_type = field.dataType
        else:
            raise ValueError(f"Schema is None for {self.dataflowSpec} for cdc_apply_changes! ")

        if sequenced_by_data_type is None:
            raise ValueError(f"Sequence by column not found in schema: "
                             f"{sequence_by} || {struct_schema} || {except_column_list}")

        if modified_schema is not None:
            struct_schema = modified_schema

        if struct_schema and int(scd_type) == 2:
            struct_schema.add(StructField("__START_AT", sequenced_by_data_type))
            struct_schema.add(StructField("__END_AT", sequenced_by_data_type))
        struct_schema = None
        if self.schema_json:
            struct_schema = self.modify_schema_for_cdc_changes(cdc_apply_changes)

        if no_keys is True:
            struct_schema.add(StructField("__DLT_HASH__", StringType()))

        return struct_schema

    def next_snapshot_and_version_silver(self, latest_snapshot_version: Any, _) -> Optional[Tuple[DataFrame, Any]]:
        logger.info(f"Received latest_snapshot_version {latest_snapshot_version} for {self.view_name}")
        spark_df = self.apply_changes_from_snapshot.agg_snapshot
        sequence_by = self.apply_changes_from_snapshot.sequence_by

        if spark_df is None or sequence_by is None:
            logger.info(f"No spark_df or sequence_by: {str(spark_df)} {str(sequence_by)}")
            return None

        sequence_by_type = spark_df.schema[sequence_by].dataType

        if isinstance(sequence_by_type, NumericType):
            latest_snapshot_version = latest_snapshot_version or 0
        else:
            latest_snapshot_version = latest_snapshot_version or '1900-01-01T00:00:00.000'
        df = spark_df.filter(col(sequence_by) > latest_snapshot_version)
        try:
            res = df.select(sequence_by).orderBy(sequence_by).first()
            if res:
                next_time = res[sequence_by]
            else:
                return None

            new_df = df.filter((col(sequence_by) == next_time))
            return new_df, next_time
        except Exception:
            raise ValueError(f"Failed to parse 'next_time'. Sequence_by '{sequence_by}'")

    def foreach_func(self, df, _):
        self.spark_df_list.append(df)

    def cdc_apply_changes_from_snapshot(self):
        logger.info(f"In cdc_apply_changes_from_snapshot\n"
                    f"{self.apply_changes_from_snapshot.sequence_by} :: "
                    f"{self.apply_changes_from_snapshot.scd_type}")

        no_keys = True if not self.apply_changes_from_snapshot.keys else False

        struct_schema = self.cdc_get_schema(None,
                                            self.apply_changes_from_snapshot.sequence_by,
                                            self.apply_changes_from_snapshot.scd_type,
                                            no_keys)
        logger.info(f"[{self.view_name}] cdc_apply_changes_from_snapshot schema {struct_schema}")
        target_path = None if self.uc_enabled else self.dataflowSpec.targetDetails["path"]

        if self.snapshot_function:
            snapshot_function = self.snapshot_function
        elif isinstance(self.dataflowSpec, SilverDataflowSpec):
            snapshot_function = self.next_snapshot_and_version_silver
        else:
            raise ValueError(f"Bronze dataflows require a next snapshot function to be passed. "
                             f"Dataflowspec type: {type(self.dataflowSpec)}")

        track_history_column_list = self.apply_changes_from_snapshot.track_history_column_list
        track_history_except_column_list = self.apply_changes_from_snapshot.track_history_except_column_list

        # Need to make sure that only one of track_history_column_list or track_history_except_column_list is set
        if not track_history_column_list:
            track_history_column_list = None
        if not track_history_except_column_list:
            track_history_except_column_list = None

        if all([track_history_column_list, track_history_except_column_list]):
            raise ValueError(
                f"Cannot set both track history column list and track history except column list.\n"
                f"track_history_column_list: {self.apply_changes_from_snapshot.track_history_column_list}\n"
                f"track_history_except_column_list: "
                f"{self.apply_changes_from_snapshot.track_history_except_column_list}"
            )

        self.create_streaming_table(struct_schema, target_path)
        if isinstance(self.dataflowSpec, SilverDataflowSpec) and (
                snapshot_function == self.next_snapshot_and_version_silver):
            self.apply_changes_from_snapshot: SilverCDCApplyChangesFromSnapshot
            # Source table in bronze to pull from
            source_table = self.dataflowSpec.sourceDetails["table"]
            checkpoint_path = self.apply_changes_from_snapshot.checkpoint_path
            if not checkpoint_path.startswith('dbfs:'):
                checkpoint_path = f'dbfs:{checkpoint_path}'
            checkpoint_path = checkpoint_path + f"/silver_{self.dataflowSpec.targetDetails['table']}"
            merge_schema = self.apply_changes_from_snapshot.merge_schema
            # [AV]: Two options are added here for bronze -> silver snapshots:
            # 1. Schema Tracking Location protects DLT from crashing if column name mapping was set in bronze. Ref:
            # https://docs.databricks.com/en/delta/column-mapping.html#streaming-with-column-mapping-and-schema-changes
            #
            # 2. Skip Change Commits will ignore any form of data changing operations in bronze. That is what you'd
            # expect when applying changes from snapshot - since the logic to apply a non-append-only change is too
            # complex to handle automatically.
            # Ref: https://docs.databricks.com/en/structured-streaming/delta-lake.html#ignore-updates-and-deletes
            snapshot_options = {
                "schemaTrackingLocation": checkpoint_path + f"/{source_table}",
                "skipChangeCommits": "true"
            }
            # Remove checkpoints if full refresh was requested
            if self.full_refresh is True:
                self.dbutils.fs.rm(checkpoint_path, recurse=True)

            (
                self.read_silver(snapshot_options)
                    .writeStream
                    .option("mergeSchema", merge_schema)
                    .option("checkpointLocation", checkpoint_path)
                    .trigger(availableNow=True)
                    .foreachBatch(self.foreach_func)
                    .start()
                    .awaitTermination(self.apply_changes_from_snapshot.timeout)
            )

            if self.spark_df_list:
                self.apply_changes_from_snapshot.agg_snapshot = reduce(DataFrame.unionAll, self.spark_df_list)
            else:
                logger.info("No updates to the stream.")
        else:
            raise ValueError("DataflowSpec must be Silver or Bronze.")

        if self.apply_changes_from_snapshot.scd_type == 1:
            track_history_except_column_list = track_history_column_list = None

        dlt.apply_changes_from_snapshot(
            target=f"{self.dataflowSpec.targetDetails['table']}",
            source=(
                lambda latest_snapshot_version: snapshot_function(latest_snapshot_version, self.dataflowSpec)
            ),
            keys=self.apply_changes_from_snapshot.keys,
            stored_as_scd_type=self.apply_changes_from_snapshot.scd_type,
            track_history_column_list=track_history_column_list,
            track_history_except_column_list=track_history_except_column_list
        )

    def cdc_apply_changes(self):
        """CDC Apply Changes against dataflowspec."""
        cdc_apply_changes = self.cdcApplyChanges
        if cdc_apply_changes is None:
            raise Exception("cdcApplychanges is None! ")

        logger.info(f"In cdc_apply_changes_from_snapshot\n"
                    f"{self.cdcApplyChanges.track_history_except_column_list} :: "
                    f"{self.cdcApplyChanges.sequence_by} :: "
                    f"{self.cdcApplyChanges.scd_type}")

        no_keys = True if not self.cdcApplyChanges.keys else False

        struct_schema = self.cdc_get_schema(cdc_apply_changes.except_column_list,
                                            cdc_apply_changes.sequence_by,
                                            cdc_apply_changes.scd_type,
                                            no_keys)

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

    def get_db_utils(self):
        """Get databricks utils using DBUtils package."""
        try:
            from pyspark.dbutils import DBUtils
        except ImportError:
            raise ImportError("DBUtils is not available - DLT Meta must be running in a notebook.")
        return DBUtils(self.spark)

    @staticmethod
    def invoke_dlt_pipeline(
            spark,
            layer,
            bronze_custom_transform_func: Callable = None,
            silver_custom_transform_func: Callable = None,
            next_snapshot_and_version: Optional[Union[Callable, Dict[str, Callable]]] = None
    ):
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
                qrt_db = dataflowSpec.quarantineTargetDetails['database']
                qrt_table = dataflowSpec.quarantineTargetDetails['table']
                quarantine_input_view_name = (
                    f"{qrt_db}_{qrt_table}"
                    f"_{layer}_quarantine_inputView"
                )
            else:
                logger.info("quarantine_input_view_name set to None")

            snapshot_function = None
            if next_snapshot_and_version:
                if isinstance(next_snapshot_and_version, Callable):
                    snapshot_function = next_snapshot_and_version
                else:
                    target_table = dataflowSpec.targetDetails["table"]
                    fq_target_table = f"{dataflowSpec.targetDetails['database'].database}.{target_table}"
                    if target_table in next_snapshot_and_version:
                        snapshot_function = next_snapshot_and_version[target_table]
                    elif fq_target_table in next_snapshot_and_version:
                        snapshot_function = next_snapshot_and_version[fq_target_table]
                    else:
                        snapshot_function = None
            target_db = dataflowSpec.targetDetails['database']
            target_table = dataflowSpec.targetDetails['table']
            dlt_data_flow = DataflowPipeline(
                spark,
                dataflowSpec,
                f"{target_db}_{target_table}_{layer}_inputView",
                quarantine_input_view_name,
                custom_transform_func,
                snapshot_function
            )
            dlt_data_flow.run_dlt()
