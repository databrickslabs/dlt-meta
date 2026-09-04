"""Pure rendering for Lakeflow Connect PostgreSQL CDC pipelines."""
from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional


GATEWAY_ID_PLACEHOLDER = "${gateway_pipeline_id}"
POSTGRES_SOURCE_TYPE = "POSTGRESQL"


def _as_mapping(value: Any, field: str = "spec") -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if hasattr(value, "as_dict") and callable(value.as_dict):
        result = value.as_dict()
        if isinstance(result, Mapping):
            return result
    if hasattr(value, "__dict__"):
        return vars(value)
    raise TypeError("%s must be a mapping or mapping-like object" % field)


def _get(container: Any, *names: str, default: Any = None) -> Any:
    if container is None:
        return default
    mapping = _as_mapping(container)
    for name in names:
        if name in mapping and mapping[name] is not None:
            return mapping[name]
    return default


def _require(container: Any, *names: str) -> Any:
    value = _get(container, *names)
    if value is None or value == "":
        raise ValueError("missing required ingestion field: %s" % names[0])
    return value


def _string_configuration(value: Any, field: str) -> Dict[str, str]:
    if value is None:
        return {}
    mapping = _as_mapping(value, field)
    result = {}
    for key, item in mapping.items():
        if not isinstance(key, str) or not isinstance(item, str):
            raise TypeError("%s must contain only string keys and values" % field)
        result[key] = item
    return result


def _canonical_source_type(value: Any) -> str:
    source_type = str(value or POSTGRES_SOURCE_TYPE).upper().replace("-", "_")
    if source_type not in {"POSTGRES", "POSTGRESQL", "POSTGRES_CDC"}:
        raise ValueError("only PostgreSQL CDC is supported")
    return POSTGRES_SOURCE_TYPE


def _boolean(value: Any, field: str) -> bool:
    """Parse native and persisted boolean values without truthy-string bugs."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
    raise TypeError("%s must be a boolean" % field)


def _json_value(value: Any, field: str, default: Any) -> Any:
    """Decode JSON-backed dataflow-spec fields while accepting native values."""
    if value is None or value == "":
        return deepcopy(default)
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as err:
            raise ValueError("%s contains invalid JSON" % field) from err
    return deepcopy(value)


def _render_gateway_compute(compute: Any) -> Optional[list]:
    if not compute:
        return None
    dbr_version = _get(compute, "dbr_version", "spark_version")
    node_type = _get(compute, "cluster_node_type", "node_type_id", "node_type")
    autoscale = _get(compute, "autoscale")
    apply_defaults = _get(compute, "apply_policy_default_values")
    if not any(
        value is not None
        for value in (dbr_version, node_type, autoscale, apply_defaults)
    ):
        return None

    cluster: Dict[str, Any] = {"label": "default"}
    if dbr_version is not None:
        cluster["spark_version"] = dbr_version
    if node_type is not None:
        cluster["node_type_id"] = node_type
    if autoscale is not None:
        cluster["autoscale"] = deepcopy(dict(_as_mapping(autoscale, "autoscale")))
    if apply_defaults is not None:
        cluster["apply_policy_default_values"] = bool(apply_defaults)
    return [cluster]


@dataclass(frozen=True)
class RenderedIngestionDefinitions:
    """The two independent pipeline payloads produced from one desired spec."""

    gateway: Dict[str, Any]
    ingestion: Dict[str, Any]
    deploy: bool = True
    spec_version: str = "1"

    @property
    def gateway_definition(self) -> Dict[str, Any]:
        return self.gateway

    @property
    def ingestion_definition(self) -> Dict[str, Any]:
        return self.ingestion

    def as_dict(self) -> Dict[str, Any]:
        return {
            "gateway": deepcopy(self.gateway),
            "ingestion": deepcopy(self.ingestion),
            "deploy": self.deploy,
            "spec_version": self.spec_version,
        }


class IngestionRenderer:
    """Render a desired PostgreSQL CDC spec without workspace side effects."""

    def render(self, spec: Any) -> RenderedIngestionDefinitions:
        root = _as_mapping(spec)
        persisted = "gatewayDetails" in root or "targetDetails" in root
        if persisted:
            gateway_input = dict(_get(root, "gatewayDetails", default={}))
            target_details = dict(_get(root, "targetDetails", default={}))
            gateway_input["configuration"] = _json_value(
                _get(root, "gatewayPipelineConfiguration"),
                "gatewayPipelineConfiguration",
                {},
            )
            gateway_input["compute"] = _json_value(
                _get(root, "gatewayCompute"), "gatewayCompute", {}
            )
            ingestion_input = dict(target_details)
            ingestion_input.update({
                "source_type": _get(root, "sourceType"),
                "objects": _json_value(_get(root, "objects"), "objects", []),
                "source_configurations": _json_value(
                    _get(root, "sourceConfigurations"),
                    "sourceConfigurations",
                    [],
                ),
                "configuration": _json_value(
                    _get(root, "ingestionPipelineConfiguration"),
                    "ingestionPipelineConfiguration",
                    {},
                ),
            })
        else:
            gateway_input = _get(root, "gateway", default={})
            ingestion_input = _get(root, "ingestion", default={})
        source_type = _canonical_source_type(
            _get(ingestion_input, "source_type", default=_get(root, "source_type"))
        )

        base_name = _get(root, "name", "dataflow_id", "dataFlowId")
        gateway_name = _get(
            gateway_input, "name", default=_get(root, "gateway_name")
        )
        ingestion_name = _get(
            ingestion_input, "name", default=_get(root, "ingestion_name")
        )
        if gateway_name is None and base_name:
            gateway_name = "%s-gateway" % base_name
        if ingestion_name is None and base_name:
            ingestion_name = "%s-ingestion" % base_name
        if not gateway_name or not ingestion_name:
            raise ValueError("gateway and ingestion pipeline names are required")

        storage = _get(
            gateway_input, "storage", default=_get(root, "gateway_storage", default={})
        )
        connection_name = _get(
            gateway_input,
            "connection_name",
            "connection",
            default=_get(root, "connection_name", "connectionName", "connection"),
        )
        if connection_name is None:
            raise ValueError("missing required ingestion field: connection_name")

        gateway_definition: Dict[str, Any] = {
            "connection_name": connection_name,
        }
        storage_fields = {
            "gateway_storage_catalog": (
                "storageCatalog",
                "catalog",
                "gateway_storage_catalog",
            ),
            "gateway_storage_schema": (
                "storageSchema",
                "schema",
                "gateway_storage_schema",
            ),
            "gateway_storage_name": ("storageName", "gateway_storage_name"),
        }
        for output_name, aliases in storage_fields.items():
            value = _get(
                storage,
                *aliases,
                default=_get(
                    gateway_input, *aliases, default=_get(root, *aliases)
                ),
            )
            if output_name == "gateway_storage_name":
                value = _get(
                    storage,
                    "name",
                    *aliases,
                    default=_get(
                        gateway_input, *aliases, default=_get(root, *aliases)
                    ),
                )
            if value is not None:
                gateway_definition[output_name] = value

        gateway: Dict[str, Any] = {
            "name": gateway_name,
            "gateway_definition": gateway_definition,
            "configuration": _string_configuration(
                _get(
                    gateway_input,
                    "configuration",
                    default=_get(root, "gateway_configuration"),
                ),
                "gateway_configuration",
            ),
            "continuous": _boolean(
                _get(gateway_input, "continuous", default=True),
                "gateway.continuous",
            ),
            "channel": _get(gateway_input, "channel", default="CURRENT"),
        }
        data_flow_id = _get(root, "data_flow_id", "dataFlowId")
        if data_flow_id is not None:
            gateway["tags"] = {
                "sdp_meta_data_flow_id": str(data_flow_id),
                "sdp_meta_pipeline_kind": "gateway",
            }
        if gateway_definition.get("gateway_storage_catalog") is not None:
            gateway["catalog"] = gateway_definition["gateway_storage_catalog"]
        if gateway_definition.get("gateway_storage_schema") is not None:
            gateway["schema"] = gateway_definition["gateway_storage_schema"]
        compute = _get(
            gateway_input, "compute", default=_get(root, "gateway_compute")
        )
        clusters = _render_gateway_compute(compute)
        if clusters:
            gateway["clusters"] = clusters

        objects = _get(ingestion_input, "objects", default=_get(root, "objects"))
        if objects is None:
            raise ValueError("missing required ingestion field: objects")
        source_configurations = _get(
            ingestion_input,
            "source_configurations",
            default=_get(root, "source_configurations", default=[]),
        )
        ingestion_definition = {
            "ingestion_gateway_id": _get(
                ingestion_input,
                "ingestion_gateway_id",
                "gateway_id",
                default=GATEWAY_ID_PLACEHOLDER,
            ),
            "source_type": source_type,
            "objects": deepcopy(list(objects)),
            "source_configurations": deepcopy(list(source_configurations)),
        }
        ingestion: Dict[str, Any] = {
            "name": ingestion_name,
            "ingestion_definition": ingestion_definition,
            "configuration": _string_configuration(
                _get(
                    ingestion_input,
                    "configuration",
                    default=_get(root, "ingestion_configuration"),
                ),
                "ingestion_configuration",
            ),
            "channel": _get(ingestion_input, "channel", default="CURRENT"),
        }
        if data_flow_id is not None:
            ingestion["tags"] = {
                "sdp_meta_data_flow_id": str(data_flow_id),
                "sdp_meta_pipeline_kind": "ingestion",
            }
        continuous = _get(
            ingestion_input,
            "continuous",
            default=_get(root, "continuous"),
        )
        if continuous is not None:
            ingestion["continuous"] = _boolean(
                continuous, "ingestion.continuous"
            )
        catalog = _get(ingestion_input, "catalog", default=_get(root, "catalog"))
        schema = _get(
            ingestion_input,
            "schema",
            "target",
            default=_get(root, "schema", "target"),
        )
        if catalog is not None:
            ingestion["catalog"] = catalog
        if schema is not None:
            ingestion["schema"] = schema

        return RenderedIngestionDefinitions(
            gateway=gateway,
            ingestion=ingestion,
            deploy=_boolean(_get(root, "deploy", default=True), "deploy"),
            spec_version=str(
                _get(root, "spec_version", "version", default="1")
            ),
        )


def render_ingestion_definitions(spec: Any) -> RenderedIngestionDefinitions:
    """Convenience entry point for callers that do not need a renderer object."""
    return IngestionRenderer().render(spec)
