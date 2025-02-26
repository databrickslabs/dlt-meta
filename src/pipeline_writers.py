
"""
This module contains classes for writing data to Delta Live Tables (DLT) and other sinks.

Classes:
    AppendFlowWriter: A class for writing append flows to Delta Live Tables.
    DLTSinkWriter: A class for writing data to various sinks using Delta Live Tables.

"""
from pyspark.sql.functions import to_json, struct
from src.dataflow_spec import DataflowSpecUtils, DLTSink
import dlt


class AppendFlowWriter:
    """Append Flow Writer class."""

    def __init__(self, spark, append_flow, target, struct_schema, table_properties=None,
                 partition_cols=None, cluster_by=None):
        """Init."""
        self.spark = spark
        self.target = target
        self.append_flow = append_flow
        self.struct_schema = struct_schema
        self.table_properties = table_properties
        self.partition_cols = partition_cols
        self.cluster_by = cluster_by

    def read_af_view(self):
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
                        )(self.read_af_view)


class DLTSinkWriter:
    """DLT Sink Writer class."""

    def __init__(self, dlt_sink: DLTSink, source_view_name):
        """Init."""
        self.dlt_sink = dlt_sink
        self.source_view_name = source_view_name

    def read_input_view(self):
        """Write to Sink."""
        input_df = dlt.read_stream(self.source_view_name)
        if self.dlt_sink.format == "kafka":
            input_df = input_df.select(to_json(struct("*")).alias("value"))
        if self.dlt_sink.select_exp:
            input_df = input_df.selectExpr(*self.dlt_sink.select_exp)
        if self.dlt_sink.where_clause:
            input_df = input_df.where(self.dlt_sink.where_clause)
        return input_df

    def write_to_sink(self):
        """Write to Sink."""
        dlt.create_sink(self.dlt_sink.name, self.dlt_sink.format, self.dlt_sink.options)
        dlt.append_flow(name=f"{self.dlt_sink.name}_flow", target=self.dlt_sink.name)(self.read_input_view)
