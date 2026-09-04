"""Persistence models and accessors for Lakeflow Connect ingestion."""

import json
from dataclasses import dataclass, fields
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class IngestionDataflowSpec:
    """Persistence schema for a Lakeflow Connect ingestion definition.

    Complex API payloads are stored as JSON strings. This keeps the persisted
    schema stable as Lakeflow Connect adds connector-specific options.
    """

    dataFlowId: str
    dataFlowGroup: str
    sourceType: str
    connectionName: str
    connectionSpec: str
    manageConnection: bool
    gatewayDetails: Dict[str, Any]
    sourceConfigurations: str
    objects: str
    targetDetails: Dict[str, Any]
    schedule: str
    deploy: bool
    gatewayPipelineConfiguration: str
    ingestionPipelineConfiguration: str
    gatewayCompute: str
    version: str
    createDate: datetime
    createdBy: str
    updateDate: datetime
    updatedBy: str


class IngestionDataflowSpecUtils:
    """Helpers for JSON-backed ingestion fields and persisted spec rows."""

    ingestion_json_fields = [
        "connectionSpec",
        "sourceConfigurations",
        "objects",
        "schedule",
        "gatewayPipelineConfiguration",
        "ingestionPipelineConfiguration",
        "gatewayCompute",
    ]

    @staticmethod
    def parse_json_field(value, field_name, expected_type=None):
        """Parse a JSON-backed field while accepting native values."""
        if value is None or value == "":
            return None
        parsed = json.loads(value) if isinstance(value, str) else value
        if expected_type is not None and not isinstance(parsed, expected_type):
            raise ValueError(
                f"{field_name} must contain JSON {expected_type.__name__}, "
                f"got {type(parsed).__name__}"
            )
        return parsed

    @staticmethod
    def get_ingestion_json(spec, field_name, expected_type=None):
        """Return one parsed JSON payload from an ingestion spec."""
        if field_name not in IngestionDataflowSpecUtils.ingestion_json_fields:
            raise ValueError(f"{field_name} is not an ingestion JSON field")
        value = spec.get(field_name) if isinstance(spec, dict) else getattr(spec, field_name)
        return IngestionDataflowSpecUtils.parse_json_field(
            value, field_name, expected_type
        )

    @staticmethod
    def ingestion_spec_from_row(row) -> IngestionDataflowSpec:
        """Create an ingestion spec from a Spark Row or persistence dictionary."""
        payload = row.asDict() if hasattr(row, "asDict") else dict(row)
        # The persisted table is written with mergeSchema, so rows may carry
        # columns newer than this model; ignore them instead of raising.
        field_names = {field.name for field in fields(IngestionDataflowSpec)}
        return IngestionDataflowSpec(
            **{key: value for key, value in payload.items() if key in field_names}
        )

    @staticmethod
    def get_ingestion_dataflow_spec(
        spark,
        dataflow_spec_df=None,
        group=None,
        dataflow_ids=None,
    ) -> List[IngestionDataflowSpec]:
        """Read latest ingestion specs using the core version-selection path."""
        # Keep pyspark and the core dataflow module out of package import time.
        from databricks.labs.sdp_meta.dataflow_spec import DataflowSpecUtils

        rows = DataflowSpecUtils._get_dataflow_spec(
            spark,
            "ingestion",
            dataflow_spec_df=dataflow_spec_df,
            group=group,
            dataflow_ids=dataflow_ids,
        ).collect()
        return [
            IngestionDataflowSpecUtils.ingestion_spec_from_row(row)
            for row in rows
        ]
