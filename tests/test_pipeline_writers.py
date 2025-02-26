import copy
from src.dataflow_pipeline import DataflowPipeline
from src.dataflow_spec import BronzeDataflowSpec, DataflowSpecUtils
from src.onboard_dataflowspec import OnboardDataflowspec
from unittest.mock import MagicMock, patch
from src.pipeline_writers import AppendFlowWriter, DLTSinkWriter
from src.dataflow_spec import DLTSink
from tests.utils import DLTFrameworkTestCase


class TestAppendFlowWriter(DLTFrameworkTestCase):

    @patch('src.pipeline_writers.dlt.read_stream')
    def test_read_af_view(self, mock_read_stream):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )
        appendflow_writer.read_af_view()
        mock_read_stream.assert_called_once()

    @patch('src.pipeline_writers.dlt.create_streaming_table')
    @patch('src.pipeline_writers.dlt.append_flow')
    def test_write_flow(self, mock_append_flow, mock_create_streaming_table):
        appendflow_writer = AppendFlowWriter(
            self.spark, MagicMock(), "test_target", "test_schema",
            {"property": "value"}, ["col1"], ["col2"]
        )        
        appendflow_writer.write_flow()
        mock_create_streaming_table.assert_called_once()
        mock_append_flow.assert_called_once()


class TestDLTSinkWriter(DLTFrameworkTestCase):

    @patch('src.pipeline_writers.dlt.read_stream')
    def test_read_input_view(self, mock_read_stream):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")        
        sink_writer.read_input_view()
        mock_read_stream.assert_called_once_with("test_view")

    @patch('src.pipeline_writers.dlt.create_sink')
    @patch('src.pipeline_writers.dlt.append_flow')
    def test_write_to_sink(self, mock_append_flow, mock_create_sink):
        dlt_sink = DLTSink(
            name="test_sink",
            format="kafka",
            options={},
            select_exp=["col1", "col2"],
            where_clause="col1 > 0"
        )
        sink_writer = DLTSinkWriter(dlt_sink, "test_view")
        sink_writer.write_to_sink()
        mock_create_sink.assert_called_once_with("test_sink", "kafka", {})
        mock_append_flow.assert_called_once()

    @patch('dlt.create_sink', new_callable=MagicMock)
    @patch('dlt.append_flow', new_callable=MagicMock)
    @patch('dlt.table', new_callable=MagicMock)
    def test_dataflowpipeline_bronze_sink_write(self, mock_dlt_table, mock_append_flow, mock_create_sink):
        local_params = copy.deepcopy(self.onboarding_bronze_silver_params_map)
        local_params["onboarding_file_path"] = self.onboarding_sink_json_file
        local_params["bronze_dataflowspec_table"] = "bronze_dataflowspec_sink"
        del local_params["silver_dataflowspec_table"]
        del local_params["silver_dataflowspec_path"]
        onboardDataFlowSpecs = OnboardDataflowspec(self.spark, local_params)
        onboardDataFlowSpecs.onboard_bronze_dataflow_spec()
        bronze_dataflowSpec_df = self.spark.read.table(
            f"{self.onboarding_bronze_silver_params_map['database']}.bronze_dataflowspec_sink")
        bronze_dataflowSpec_df.show(truncate=False)
        self.assertEqual(bronze_dataflowSpec_df.count(), 1)
        bronze_dataflow_spec = DataflowSpecUtils._get_dataflow_spec(
            spark=self.spark,
            dataflow_spec_df=bronze_dataflowSpec_df,
            layer="bronze"
        ).collect()[0]
        self.spark.conf.set("spark.databricks.unityCatalog.enabled", "True")
        view_name = f"{bronze_dataflow_spec.targetDetails['table']}_inputView"
        pipeline = DataflowPipeline(self.spark, BronzeDataflowSpec(**bronze_dataflow_spec.asDict()), view_name, None)
        pipeline.write()
        assert mock_create_sink.called_with(
            name="sink",
            target="sink",
            comment="sink dlt table sink"
        )
        assert mock_append_flow.called_with(
            name="sink",
            target="sink"
        )
        assert mock_dlt_table.called_with(
            pipeline.write_to_delta,
            name="sink",
            partition_cols=[],
            table_properties={},
            path=None,
            comment="sink dlt table sink"
        )
