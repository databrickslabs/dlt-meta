"""Pure onboarding helpers for Lakeflow Connect ingestion metadata."""
import copy
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Mapping, Optional, Tuple

from databricks.labs.sdp_meta.lfc.models import IngestionDataflowSpec


_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")
_CREDENTIAL_WORDS = {
    "access_key",
    "api_key",
    "client_secret",
    "credential",
    "credentials",
    "password",
    "private_key",
    "secret",
    "token",
}
_STRUCTURAL_CONFIGURATION_KEYS = {
    "catalog",
    "cluster",
    "clusters",
    "compute",
    "connection_name",
    "gateway_definition",
    "host",
    "ingestion_definition",
    "libraries",
    "name",
    "objects",
    "pipeline_type",
    "port",
    "schema",
    "source",
    "source_type",
    "target",
    "tables",
}
_VALID_SCD_TYPES = {"SCD_TYPE_1", "SCD_TYPE_2"}


class IngestionValidationError(ValueError):
    """Raised after all ingestion onboarding validation errors are collected."""

    def __init__(self, errors):
        self.errors = list(errors)
        super().__init__("Invalid ingestion metadata:\n- " + "\n- ".join(self.errors))


@dataclass
class IngestionOnboardingResult:
    """Pure, persistence-ready output from ingestion onboarding."""

    ingestion_specs: List[dict]
    ingestion_registry: Dict[Tuple[str, str], dict]


@dataclass
class PreparedOnboarding:
    """Validated ingestion specs plus downstream-ready onboarding rows."""

    rows: List[dict]
    ingestion_specs: List[dict]
    ingestion_registry: Dict[Tuple[str, str], dict]


def _json(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _is_non_empty_string(value):
    return isinstance(value, str) and bool(value.strip())


def _derived_name(value):
    normalized = _NAME_RE.sub("_", str(value).strip()).strip("_")
    return normalized or "ingestion"


def _bundle_key(value):
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def resolve_environment_field(
    values, field, env, required=False, errors=None, path=None
):
    """Resolve ``<field>_<env>`` with an unsuffixed fallback."""
    errors = errors if errors is not None else []
    env_field = f"{field}_{env}" if env else field
    value = values.get(env_field)
    if value is None or value == "":
        value = values.get(field)
    if required and (value is None or value == ""):
        errors.append(f"{path or field}: missing {env_field!r} (or {field!r})")
    return value


def validate_configuration(configuration, path="configuration"):
    """Validate a scalar, non-secret pipeline configuration map."""
    errors = []
    if configuration is None:
        return errors
    if not isinstance(configuration, Mapping):
        return [f"{path}: must be a map"]
    for key, value in configuration.items():
        key_text = str(key)
        lowered = key_text.lower()
        segments = set(re.split(r"[^a-z0-9_]+", lowered))
        if lowered in _STRUCTURAL_CONFIGURATION_KEYS:
            errors.append(f"{path}.{key_text}: structural keys are not allowed")
        has_credential_word = bool(segments.intersection(_CREDENTIAL_WORDS))
        if not has_credential_word:
            has_credential_word = any(
                lowered == word
                or lowered.startswith(word + "_")
                or lowered.endswith("_" + word)
                or ("_" + word + "_") in lowered
                for word in _CREDENTIAL_WORDS
            )
        if has_credential_word:
            errors.append(f"{path}.{key_text}: credential values are not allowed")
        if not isinstance(value, str):
            errors.append(
                f"{path}.{key_text}: pass-through values must be strings"
            )
    return errors


def _table_entries(tables, errors):
    if tables == "*":
        return ["*"]
    if not isinstance(tables, list) or not tables:
        errors.append("ingestion.tables: must be a non-empty list or '*'")
        return []
    return tables


def _merge_table(table, defaults, index, errors):
    if isinstance(table, str):
        table = {"name": table}
    elif isinstance(table, Mapping):
        table = copy.deepcopy(dict(table))
    else:
        errors.append(f"ingestion.tables[{index}]: must be a table name or map")
        return None

    name = table.get("name")
    if not _is_non_empty_string(name):
        errors.append(
            f"ingestion.tables[{index}].name: must be a non-empty string"
        )
        return None

    merged = copy.deepcopy(defaults)
    explicit_configuration = table.get(
        "table_configuration",
        table.get("configuration", {}),
    )
    if explicit_configuration is not None and not isinstance(
        explicit_configuration, Mapping
    ):
        errors.append(f"ingestion.tables[{index}].configuration: must be a map")
        explicit_configuration = {}
    merged.update(copy.deepcopy(dict(explicit_configuration or {})))

    explicit_scd_type = table.get("scd_type")
    if (
        explicit_scd_type is not None
        and explicit_scd_type not in _VALID_SCD_TYPES
    ):
        errors.append(
            f"ingestion.tables[{index}].scd_type: must be "
            "SCD_TYPE_1 or SCD_TYPE_2"
        )

    scd = table.get("scd")
    if scd is not None:
        if str(scd) not in ("1", "2"):
            errors.append(f"ingestion.tables[{index}].scd: must be 1 or 2")
        else:
            merged["scd_type"] = f"SCD_TYPE_{scd}"
    elif explicit_scd_type is not None:
        merged["scd_type"] = explicit_scd_type
    if (
        merged.get("scd_type") is not None
        and merged["scd_type"] not in _VALID_SCD_TYPES
        and explicit_scd_type is None
    ):
        errors.append(
            f"ingestion.tables[{index}].scd_type: must be "
            "SCD_TYPE_1 or SCD_TYPE_2"
        )

    destination = table.get(
        "destination", table.get("destination_table", name)
    )
    if not _is_non_empty_string(destination):
        errors.append(
            f"ingestion.tables[{index}].destination: must be a non-empty string"
        )
        destination = name
    source_schema = table.get("source_schema", table.get("schema"))
    return name, destination, source_schema, merged


def _build_objects(ingestion, source, target, errors):
    table_defaults = ingestion.get(
        "table_configuration",
        ingestion.get("table_defaults", {}),
    )
    if table_defaults is None:
        table_defaults = {}
    if not isinstance(table_defaults, Mapping):
        errors.append("ingestion.table_configuration: must be a map")
        table_defaults = {}
    elif (
        table_defaults.get("scd_type") is not None
        and table_defaults["scd_type"] not in _VALID_SCD_TYPES
    ):
        errors.append(
            "ingestion.table_configuration.scd_type: must be "
            "SCD_TYPE_1 or SCD_TYPE_2"
        )

    entries = _table_entries(ingestion.get("tables"), errors)
    if entries == ["*"]:
        return [{
            "schema": {
                "source_catalog": source.get("catalog"),
                "source_schema": source.get("schema"),
                "destination_catalog": target.get("catalog"),
                "destination_schema": target.get("schema"),
            }
        }]

    objects = []
    for index, entry in enumerate(entries):
        resolved = _merge_table(entry, table_defaults, index, errors)
        if resolved is None:
            continue
        name, destination, source_schema, configuration = resolved
        objects.append({
            "table": {
                "source_catalog": source.get("catalog"),
                "source_schema": source_schema or source.get("schema"),
                "source_table": name,
                "destination_catalog": target.get("catalog"),
                "destination_schema": target.get("schema"),
                "destination_table": destination,
                "table_configuration": configuration,
            }
        })
    return objects


def _source_configurations(source):
    slot = source.get("slot")
    if not slot:
        return []
    slot_config = {}
    publication = slot.get("publication_name", slot.get("publication"))
    slot_name = slot.get("slot_name", slot.get("slot"))
    if publication is not None:
        slot_config["publication_name"] = publication
    if slot_name is not None:
        slot_config["slot_name"] = slot_name
    return [{
        "catalog": {
            "source_catalog": source.get("catalog"),
            "postgres": {"slot_config": slot_config},
        }
    }]


def _validate_required_strings(values, fields, prefix, errors):
    for field in fields:
        if not _is_non_empty_string(values.get(field)):
            errors.append(f"{prefix}.{field}: must be a non-empty string")


def parse_ingestion_row(
    row,
    env,
    version="1",
    created_by="sdp-meta",
    now=None,
):
    """Parse and validate one row containing an ``ingestion`` block."""
    if not isinstance(row, Mapping):
        raise IngestionValidationError(["row: must be a map"])

    errors = []
    ingestion = row.get("ingestion")
    if not isinstance(ingestion, Mapping):
        raise IngestionValidationError(
            ["ingestion: block is required and must be a map"]
        )
    ingestion = copy.deepcopy(dict(ingestion))

    data_flow_id = row.get("data_flow_id")
    data_flow_group = row.get("data_flow_group")
    if not _is_non_empty_string(data_flow_id):
        errors.append("data_flow_id: must be a non-empty string")
    if not _is_non_empty_string(data_flow_group):
        errors.append("data_flow_group: must be a non-empty string")

    source = ingestion.get("source")
    target = ingestion.get("target")
    gateway = ingestion.get("gateway", {})
    pipeline = ingestion.get("ingestion_pipeline", {})
    if not isinstance(source, Mapping):
        errors.append("ingestion.source: must be a map")
        source = {}
    else:
        source = copy.deepcopy(dict(source))
    if not isinstance(target, Mapping):
        errors.append("ingestion.target: must be a map")
        target = {}
    else:
        target = copy.deepcopy(dict(target))
    if not isinstance(gateway, Mapping):
        errors.append("ingestion.gateway: must be a map")
        gateway = {}
    else:
        gateway = copy.deepcopy(dict(gateway))
    if not isinstance(pipeline, Mapping):
        errors.append("ingestion.ingestion_pipeline: must be a map")
        pipeline = {}
    else:
        pipeline = copy.deepcopy(dict(pipeline))

    source["host"] = resolve_environment_field(
        source, "host", env, errors=errors, path="ingestion.source.host"
    )
    if source.get("schema") is None or source.get("schema") == "":
        source["schema"] = "public"
    target["catalog"] = resolve_environment_field(
        target,
        "catalog",
        env,
        required=True,
        errors=errors,
        path="ingestion.target.catalog",
    )
    target["schema"] = resolve_environment_field(
        target,
        "schema",
        env,
        required=True,
        errors=errors,
        path="ingestion.target.schema",
    )
    _validate_required_strings(
        source, ("type", "catalog", "schema"), "ingestion.source", errors
    )
    source_type = source.get("type")
    if _is_non_empty_string(source_type):
        normalized_type = source_type.strip().upper().replace("-", "_")
        if normalized_type not in {"POSTGRES", "POSTGRESQL", "POSTGRES_CDC"}:
            # Fail at onboarding time with the same contract the renderer
            # enforces at generate/deploy time, instead of persisting a spec
            # that can never render.
            errors.append(
                "ingestion.source.type: only PostgreSQL CDC is supported "
                f"(got {source_type!r})"
            )

    storage_catalog = resolve_environment_field(
        gateway,
        "storage_catalog",
        env,
        errors=errors,
        path="ingestion.gateway.storage_catalog",
    )
    storage_schema = resolve_environment_field(
        gateway,
        "storage_schema",
        env,
        errors=errors,
        path="ingestion.gateway.storage_schema",
    )

    gateway_configuration = gateway.get("pipeline_configuration", {})
    ingestion_configuration = pipeline.get("pipeline_configuration", {})
    errors.extend(
        validate_configuration(
            gateway_configuration,
            "ingestion.gateway.pipeline_configuration",
        )
    )
    errors.extend(
        validate_configuration(
            ingestion_configuration,
            "ingestion.ingestion_pipeline.pipeline_configuration",
        )
    )

    manage_connection = ingestion.get(
        "manage_connection",
        source.get("manage_connection", bool(source.get("host"))),
    )
    if not isinstance(manage_connection, bool):
        errors.append("ingestion.manage_connection: must be a boolean")
        manage_connection = False
    connection_name = ingestion.get(
        "connection_name",
        source.get("connection_name", source.get("connection")),
    )
    if not manage_connection and not _is_non_empty_string(connection_name):
        errors.append(
            "ingestion.connection_name: required when manage_connection "
            "is false"
        )
    if not connection_name:
        connection_name = f"{_derived_name(data_flow_group)}_connection"
    if manage_connection and not _is_non_empty_string(source.get("host")):
        errors.append(
            "ingestion.source.host: required when manage_connection is true"
        )

    deploy = ingestion.get("deploy", True)
    if not isinstance(deploy, bool):
        errors.append("ingestion.deploy: must be a boolean")
        deploy = True

    objects = _build_objects(ingestion, source, target, errors)
    if errors:
        raise IngestionValidationError(errors)

    group_name = _derived_name(data_flow_group)
    # Timezone-aware so Spark's TimestampType never reinterprets the value
    # through the session timezone (utcnow() is also deprecated in 3.12).
    timestamp = now or datetime.now(timezone.utc)
    connection_spec = {
        key: source[key]
        for key in ("host", "port", "secret")
        if source.get(key) is not None
    }
    gateway_details = {
        "name": gateway.get("name", f"{group_name}_gateway"),
        "storageCatalog": storage_catalog or target["catalog"],
        "storageSchema": storage_schema or target["schema"],
        "continuous": gateway.get("continuous", True),
        "channel": gateway.get("channel", "CURRENT"),
    }
    target_details = {
        "catalog": target["catalog"],
        "schema": target["schema"],
        "name": pipeline.get("name", f"{group_name}_ingestion"),
        "channel": pipeline.get("channel", "CURRENT"),
    }
    if "continuous" in pipeline:
        target_details["continuous"] = pipeline["continuous"]
    spec = IngestionDataflowSpec(
        dataFlowId=data_flow_id,
        dataFlowGroup=data_flow_group,
        sourceType=source["type"],
        connectionName=connection_name,
        connectionSpec=_json(connection_spec),
        manageConnection=manage_connection,
        gatewayDetails=gateway_details,
        sourceConfigurations=_json(_source_configurations(source)),
        objects=_json(objects),
        targetDetails=target_details,
        schedule=_json(ingestion.get("schedule", {})),
        deploy=deploy,
        gatewayPipelineConfiguration=_json(gateway_configuration or {}),
        ingestionPipelineConfiguration=_json(ingestion_configuration or {}),
        gatewayCompute=_json(gateway.get("compute", {})),
        version=str(version),
        createDate=timestamp,
        createdBy=created_by,
        updateDate=timestamp,
        updatedBy=created_by,
    )
    return asdict(spec)


def build_ingestion_registry(specs):
    """Build ``(data_flow_id, source_table)`` lookup entries from specs."""
    registry = {}
    errors = []
    for spec_index, raw_spec in enumerate(specs):
        spec = (
            asdict(raw_spec)
            if isinstance(raw_spec, IngestionDataflowSpec)
            else dict(raw_spec)
        )
        try:
            objects = (
                json.loads(spec["objects"])
                if isinstance(spec["objects"], str)
                else spec["objects"]
            )
        except (KeyError, TypeError, json.JSONDecodeError) as err:
            errors.append(
                f"ingestion spec[{spec_index}].objects: invalid JSON ({err})"
            )
            continue
        for object_index, obj in enumerate(objects):
            table = obj.get("table") if isinstance(obj, Mapping) else None
            if not table:
                continue
            key = (spec.get("dataFlowId"), table.get("source_table"))
            if key in registry:
                errors.append(f"duplicate ingestion registry key {key!r}")
                continue
            registry[key] = {
                "data_flow_id": spec.get("dataFlowId"),
                "data_flow_group": spec.get("dataFlowGroup"),
                "table": table.get("source_table"),
                "sourceDetails": {
                    "catalog": table.get("destination_catalog"),
                    "database": table.get("destination_schema"),
                    "table": table.get("destination_table"),
                },
                "object": copy.deepcopy(obj),
                "ingestion_spec": spec,
            }
            if not all(key):
                errors.append(
                    f"ingestion spec[{spec_index}].objects[{object_index}]: "
                    "source table and data flow id are required"
                )
    if errors:
        raise IngestionValidationError(errors)
    return registry


def resolve_ingestion_ref(ingestion_ref, registry):
    """Resolve a reference to the existing ``sourceDetails`` map shape."""
    if not isinstance(ingestion_ref, Mapping):
        raise IngestionValidationError(["ingestion_ref: must be a map"])
    data_flow_id = ingestion_ref.get("data_flow_id")
    table = ingestion_ref.get("table")
    errors = []
    if not _is_non_empty_string(data_flow_id):
        errors.append(
            "ingestion_ref.data_flow_id: must be a non-empty string"
        )
    if not _is_non_empty_string(table):
        errors.append("ingestion_ref.table: must be a non-empty string")
    if errors:
        raise IngestionValidationError(errors)
    key = (data_flow_id, table)
    if key not in registry:
        raise IngestionValidationError(
            [f"ingestion_ref: no ingestion object found for {key!r}"]
        )
    return copy.deepcopy(registry[key]["sourceDetails"])


def resolve_row_ingestion_ref(row, registry):
    """Resolve and inject ``sourceDetails`` without mutating the input row."""
    resolved = copy.deepcopy(dict(row))
    if "ingestion_ref" in resolved:
        resolved["sourceDetails"] = resolve_ingestion_ref(
            resolved.pop("ingestion_ref"),
            registry,
        )
    return resolved


def prepare_onboarding_rows(
    rows,
    env,
    version="1",
    created_by="sdp-meta",
    now=None,
    persisted_ingestion_specs=None,
):
    """Validate ingestion metadata and resolve every downstream reference.

    ``ingestion_ref`` is intentionally authoring syntax only.  Bronze expects
    a Delta ``source_details`` block, while Silver expects the existing
    Bronze-target field shape.  This adapter fills the appropriate shape
    without changing either persisted downstream model.
    """
    source_rows = [
        copy.deepcopy(dict(row)) if isinstance(row, Mapping) else row
        for row in rows
    ]
    ingestion = onboard_ingestion_rows(
        source_rows,
        env,
        version=version,
        created_by=created_by,
        now=now,
    )
    registry = {}
    if persisted_ingestion_specs:
        registry.update(build_ingestion_registry(persisted_ingestion_specs))
    # Current-file definitions intentionally replace older persisted entries.
    registry.update(ingestion.ingestion_registry)

    errors = []
    resolved_sources = {}
    for index, row in enumerate(source_rows):
        if not isinstance(row, Mapping) or "ingestion_ref" not in row:
            continue
        conflicts = [
            field
            for field in ("source_details", "source_format")
            if row.get(field) not in (None, "", {})
        ]
        if conflicts:
            errors.append(
                f"row[{index}]: ingestion_ref is mutually exclusive with "
                f"explicit Bronze source fields {conflicts!r}"
            )
            continue
        try:
            resolved_sources[index] = resolve_ingestion_ref(
                row["ingestion_ref"], registry
            )
        except IngestionValidationError as err:
            errors.extend(f"row[{index}]: {message}" for message in err.errors)
    if errors:
        raise IngestionValidationError(errors)

    prepared = []
    for index, row in enumerate(source_rows):
        if not isinstance(row, Mapping):
            prepared.append(row)
            continue
        current = copy.deepcopy(dict(row))
        source = resolved_sources.get(index)
        if source is not None:
            current.pop("ingestion_ref", None)
            bronze_db = f"bronze_database_{env}"
            bronze_catalog = f"bronze_catalog_{env}"
            has_bronze = bool(
                current.get(bronze_db) and current.get("bronze_table")
            )
            has_silver = bool(
                current.get(f"silver_database_{env}")
                and current.get("silver_table")
            )
            if has_bronze:
                # LFC -> Bronze.  Silver, when also declared, keeps using the
                # row's Bronze target through the existing Bronze -> Silver
                # path.
                current["source_format"] = "delta"
                current["source_details"] = {
                    "source_database": source["database"],
                    "source_table": source["table"],
                    f"source_catalog_{env}": source.get("catalog"),
                }
            elif has_silver:
                # LFC -> Silver, intentionally skipping Bronze persistence.
                current[bronze_db] = source["database"]
                current["bronze_table"] = source["table"]
                if source.get("catalog"):
                    current[bronze_catalog] = source["catalog"]
            # With neither target this is an LFC-only row.
        # Spark does not need the authoring block after ingestion specs have
        # been derived, and dropping it avoids heterogeneous nested inference.
        current.pop("ingestion", None)
        prepared.append(current)
    return PreparedOnboarding(
        prepared,
        ingestion.ingestion_specs,
        registry,
    )


def onboard_ingestion_rows(
    rows,
    env,
    version="1",
    created_by="sdp-meta",
    now=None,
):
    """Validate all rows before returning any persistence-ready metadata."""
    parsed = []
    errors = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or "ingestion" not in row:
            continue
        try:
            parsed.append(
                parse_ingestion_row(
                    row,
                    env,
                    version=version,
                    created_by=created_by,
                    now=now,
                )
            )
        except IngestionValidationError as err:
            errors.extend(
                f"row[{index}]: {message}" for message in err.errors
            )
    if errors:
        raise IngestionValidationError(errors)
    ids = {}
    groups = {}
    destinations = {}
    for index, spec in enumerate(parsed):
        ids.setdefault(spec["dataFlowId"], []).append(index)
        groups.setdefault(_bundle_key(spec["dataFlowGroup"]), []).append(index)
        target = spec["targetDetails"]
        destination = (
            str(target["catalog"]).strip().lower(),
            str(target["schema"]).strip().lower(),
        )
        destinations.setdefault(destination, []).append(index)
    for data_flow_id, owners in ids.items():
        if len(owners) > 1:
            errors.append(
                f"duplicate ingestion data_flow_id {data_flow_id!r} "
                f"at rows {owners}"
            )
    for group, owners in groups.items():
        if len(owners) > 1:
            errors.append(
                f"ingestion pipeline key collision {group!r} at rows {owners}"
            )
    for destination, owners in destinations.items():
        if len(owners) > 1:
            errors.append(
                f"ingestion destination collision {destination!r} "
                f"at rows {owners}"
            )
    if errors:
        raise IngestionValidationError(errors)
    registry = build_ingestion_registry(parsed)
    return IngestionOnboardingResult(parsed, registry)


class IngestionOnboarder:
    """Small state-free facade for callers that prefer an object API."""

    def __init__(self, env, version="1", created_by="sdp-meta"):
        self.env = env
        self.version = version
        self.created_by = created_by

    def parse_row(self, row, now=None):
        return parse_ingestion_row(
            row,
            self.env,
            version=self.version,
            created_by=self.created_by,
            now=now,
        )

    def parse_rows(
        self, rows: Iterable[dict], now: Optional[datetime] = None
    ):
        return onboard_ingestion_rows(
            rows,
            self.env,
            version=self.version,
            created_by=self.created_by,
            now=now,
        )
