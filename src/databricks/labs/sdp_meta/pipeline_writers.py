"""
This module contains classes for writing data to Lakeflow Spark Declarative Pipelines and other sinks.

Classes:
    AppendFlowWriter: A class for writing append flows to Lakeflow Spark Declarative Pipelines.
    DLTSinkWriter: A class for writing data to various sinks using Lakeflow Spark Declarative Pipelines.

"""
from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils, DLTSink
from pyspark import pipelines as dp


class AppendFlowWriter:
    """Append Flow Writer class."""

    def __init__(self, spark, append_flow, target, struct_schema, table_properties=None,
                 partition_cols=None, cluster_by=None, cluster_by_auto=False, row_filter=None):
        """Init."""
        self.spark = spark
        self.target = target
        self.append_flow = append_flow
        self.struct_schema = struct_schema
        self.table_properties = table_properties
        self.partition_cols = partition_cols
        self.cluster_by = cluster_by
        self.cluster_by_auto = cluster_by_auto
        self.row_filter = row_filter

    def read_af_view(self):
        """Write to Delta."""
        return self.spark.readStream.table(f"{self.append_flow.name}_view")

    def write_flow(self):
        """Write Append Flow."""
        if self.append_flow.create_streaming_table:
            # Default cluster_by_auto to False if None
            cluster_by_auto = self.cluster_by_auto if self.cluster_by_auto is not None else False

            dp.create_streaming_table(
                name=self.target,
                table_properties=self.table_properties,
                partition_cols=DataflowSpecUtils.get_partition_cols(self.partition_cols),
                cluster_by=DataflowSpecUtils.get_partition_cols(self.cluster_by),
                cluster_by_auto=cluster_by_auto,
                schema=self.struct_schema,
                expect_all=None,
                expect_all_or_drop=None,
                expect_all_or_fail=None,
                row_filter=self.row_filter,
            )
        comment = (
            self.append_flow.comment
            if self.append_flow.comment
            else f"append_flow={self.append_flow.name} for target={self.target}"
        )
        spark_conf = self.append_flow.spark_conf if self.append_flow.spark_conf else {}
        dp.append_flow(
            name=self.append_flow.name,
            target=self.target,
            comment=comment,
            spark_conf=spark_conf,
            once=self.append_flow.once
        )(self.read_af_view)


class DLTSinkWriter:
    """DLT Sink Writer class."""

    def __init__(self, spark, dlt_sink: DLTSink, source_view_name):
        """Init."""
        self.spark = spark
        self.dlt_sink = dlt_sink
        self.source_view_name = source_view_name

    def read_input_view(self):
        """Write to Sink."""
        input_df = self.spark.readStream.table(self.source_view_name)
        if self.dlt_sink.select_exp:
            input_df = input_df.selectExpr(*self.dlt_sink.select_exp)
        if self.dlt_sink.where_clause:
            input_df = input_df.where(self.dlt_sink.where_clause)
        return input_df

    def write_to_sink(self):
        """Write to Sink."""
        dp.create_sink(
            name=self.dlt_sink.name,
            format=self.dlt_sink.format,
            options=self.dlt_sink.options
        )
        dp.append_flow(
            name=f"{self.dlt_sink.name}_flow",
            target=self.dlt_sink.name,
            comment=f"Sink flow for {self.dlt_sink.name}"
        )(self.read_input_view)
