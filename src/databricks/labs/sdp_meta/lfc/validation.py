"""Pure validation for row-oriented Lakeflow Connect onboarding metadata.

Errors are structural problems that cannot produce safe persisted ingestion
specs. Warnings identify suspicious input which onboarding can carry, such as
unknown keys and unfilled placeholders. Strict callers treat warnings as
failures by using :func:`strict_failures`.
"""
from __future__ import annotations

import re
from collections.abc import Mapping

from databricks.labs.sdp_meta.lfc.onboarding import (
    IngestionValidationError,
    parse_ingestion_row,
)


_VALID_SCD_TYPES = {None, "SCD_TYPE_1", "SCD_TYPE_2"}
_PLACEHOLDER_RE = re.compile(r"REPLACE_ME", re.IGNORECASE)

_INGESTION_KEYS = {
    "connection_name",
    "deploy",
    "gateway",
    "ingestion_pipeline",
    "manage_connection",
    "schedule",
    "source",
    "table_configuration",
    "table_defaults",
    "tables",
    "target",
}
_SOURCE_KEYS = {
    "catalog",
    "connection",
    "connection_name",
    "host",
    "manage_connection",
    "port",
    "schema",
    "secret",
    "slot",
    "type",
}
_SOURCE_ENV_KEYS = {"host"}
_TARGET_KEYS = {"catalog", "schema"}
_TARGET_ENV_KEYS = {"catalog", "schema"}
_GATEWAY_KEYS = {
    "channel",
    "compute",
    "continuous",
    "name",
    "pipeline_configuration",
    "storage_catalog",
    "storage_schema",
}
_GATEWAY_ENV_KEYS = {"storage_catalog", "storage_schema"}
_PIPELINE_KEYS = {"channel", "continuous", "name", "pipeline_configuration"}
_TABLE_KEYS = {
    "configuration",
    "destination",
    "destination_table",
    "name",
    "scd",
    "scd_type",
    "schema",
    "source_schema",
    "table_configuration",
}
_SLOT_KEYS = {"publication", "publication_name", "slot", "slot_name"}
_COMPUTE_KEYS = {
    "apply_policy_default_values",
    "autoscale",
    "cluster_node_type",
    "dbr_version",
    "node_type",
    "node_type_id",
    "spark_version",
}
_AUTOSCALE_KEYS = {"max_workers", "min_workers", "mode"}
_SCHEDULE_KEYS = {
    "pause_status",
    "quartz_cron_expression",
    "timezone_id",
}


def _label(row, index):
    group = row.get("data_flow_group") if isinstance(row, Mapping) else None
    return str(group or "row[%d]" % index)


def _is_allowed_key(key, allowed, environment_fields):
    if key in allowed:
        return True
    return any(key.startswith("%s_" % field) for field in environment_fields)


def _unknown(mapping, allowed, path, warnings, environment_fields=()):
    if not isinstance(mapping, Mapping):
        return
    for key in mapping:
        if not isinstance(key, str) or not _is_allowed_key(
            key, allowed, environment_fields
        ):
            warnings.append(
                "%s: unknown key %r (typo?) - it is ignored" % (path, key)
            )


def _lint_unknown_keys(row, index, warnings):
    ingestion = row.get("ingestion") if isinstance(row, Mapping) else None
    if not isinstance(ingestion, Mapping):
        return
    label = _label(row, index)
    root = "%s.ingestion" % label
    _unknown(ingestion, _INGESTION_KEYS, root, warnings)

    source = ingestion.get("source")
    target = ingestion.get("target")
    gateway = ingestion.get("gateway")
    pipeline = ingestion.get("ingestion_pipeline")
    _unknown(
        source,
        _SOURCE_KEYS,
        "%s.source" % root,
        warnings,
        _SOURCE_ENV_KEYS,
    )
    _unknown(
        target,
        _TARGET_KEYS,
        "%s.target" % root,
        warnings,
        _TARGET_ENV_KEYS,
    )
    _unknown(
        gateway,
        _GATEWAY_KEYS,
        "%s.gateway" % root,
        warnings,
        _GATEWAY_ENV_KEYS,
    )
    _unknown(
        pipeline,
        _PIPELINE_KEYS,
        "%s.ingestion_pipeline" % root,
        warnings,
    )
    _unknown(
        source.get("slot") if isinstance(source, Mapping) else None,
        _SLOT_KEYS,
        "%s.source.slot" % root,
        warnings,
    )
    compute = gateway.get("compute") if isinstance(gateway, Mapping) else None
    _unknown(compute, _COMPUTE_KEYS, "%s.gateway.compute" % root, warnings)
    _unknown(
        compute.get("autoscale") if isinstance(compute, Mapping) else None,
        _AUTOSCALE_KEYS,
        "%s.gateway.compute.autoscale" % root,
        warnings,
    )
    _unknown(
        ingestion.get("schedule"),
        _SCHEDULE_KEYS,
        "%s.schedule" % root,
        warnings,
    )

    tables = ingestion.get("tables")
    if isinstance(tables, list):
        for table_index, table in enumerate(tables):
            if isinstance(table, Mapping):
                table_name = table.get("name") or table_index
                _unknown(
                    table,
                    _TABLE_KEYS,
                    "%s.tables.%s" % (root, table_name),
                    warnings,
                )


def _contains_placeholder(value):
    if isinstance(value, str):
        return bool(_PLACEHOLDER_RE.search(value))
    if isinstance(value, Mapping):
        return any(_contains_placeholder(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_contains_placeholder(item) for item in value)
    return False


def _check_scd_type(value, path, errors):
    if value not in _VALID_SCD_TYPES:
        errors.append(
            "%s: invalid scd_type %r (expected SCD_TYPE_1 or SCD_TYPE_2)"
            % (path, value)
        )


def _check_scd(ingestion, label, errors):
    for defaults_key in ("table_configuration", "table_defaults"):
        defaults = ingestion.get(defaults_key)
        if isinstance(defaults, Mapping) and "scd_type" in defaults:
            _check_scd_type(
                defaults.get("scd_type"),
                "%s.ingestion.%s.scd_type" % (label, defaults_key),
                errors,
            )

    tables = ingestion.get("tables")
    if not isinstance(tables, list):
        return
    names = []
    destinations = []
    for table_index, table in enumerate(tables):
        if isinstance(table, str):
            names.append(table)
            destinations.append(table)
            continue
        if not isinstance(table, Mapping):
            continue
        name = table.get("name")
        names.append(name)
        destinations.append(
            table.get("destination", table.get("destination_table", name))
        )
        path = "%s.ingestion.tables[%d]" % (label, table_index)
        if "scd" in table and str(table.get("scd")) not in {"1", "2"}:
            errors.append("%s.scd: must be 1 or 2" % path)
        if "scd_type" in table:
            _check_scd_type(table.get("scd_type"), "%s.scd_type" % path, errors)
        for config_key in ("configuration", "table_configuration"):
            config = table.get(config_key)
            if isinstance(config, Mapping) and "scd_type" in config:
                _check_scd_type(
                    config.get("scd_type"),
                    "%s.%s.scd_type" % (path, config_key),
                    errors,
                )

    duplicate_names = sorted(
        {name for name in names if name and names.count(name) > 1}
    )
    if duplicate_names:
        errors.append("%s: duplicate tables %r" % (label, duplicate_names))
    duplicate_destinations = sorted(
        {
            destination
            for destination in destinations
            if destination and destinations.count(destination) > 1
        }
    )
    if duplicate_destinations and duplicate_destinations != duplicate_names:
        errors.append(
            "%s: duplicate destination tables %r"
            % (label, duplicate_destinations)
        )


def _normalized_destination(target):
    return (
        str(target.get("catalog", "")).strip().lower(),
        str(target.get("schema", "")).strip().lower(),
    )


def _normalized_bundle_key(value):
    return re.sub(r"[^a-z0-9_]+", "_", str(value).strip().lower()).strip("_")


def validate(rows, env):
    """Return ``(errors, warnings)`` for raw row-oriented onboarding data.

    Parsing is delegated to :mod:`lfc.onboarding`, so required-field,
    connection, deploy, and pipeline-configuration checks exactly match the
    persistence model.
    """
    if isinstance(rows, Mapping):
        rows = [rows]
    else:
        rows = list(rows)

    errors = []
    warnings = []
    parsed = []
    parsed_row_indexes = []

    for index, row in enumerate(rows):
        if not isinstance(row, Mapping) or "ingestion" not in row:
            continue
        _lint_unknown_keys(row, index, warnings)
        ingestion = row.get("ingestion")
        label = _label(row, index)
        if isinstance(ingestion, Mapping):
            _check_scd(ingestion, label, errors)
            if _contains_placeholder(ingestion):
                warnings.append(
                    "%s: unfilled REPLACE_ME placeholder - not deployable "
                    "until filled" % label
                )
        try:
            parsed.append(parse_ingestion_row(row, env))
            parsed_row_indexes.append(index)
        except IngestionValidationError as err:
            errors.extend("row[%d]: %s" % (index, item) for item in err.errors)

    ids = {}
    bundle_keys = {}
    destinations = {}
    for spec, row_index in zip(parsed, parsed_row_indexes):
        ids.setdefault(spec["dataFlowId"], []).append(row_index)
        key = _normalized_bundle_key(spec["dataFlowGroup"])
        bundle_keys.setdefault(key, []).append(row_index)
        destinations.setdefault(
            _normalized_destination(spec["targetDetails"]), []
        ).append(row_index)

    for data_flow_id, owners in ids.items():
        if len(owners) > 1:
            errors.append(
                "duplicate ingestion data_flow_id %r at rows %r"
                % (data_flow_id, owners)
            )
    for key, owners in bundle_keys.items():
        if len(owners) > 1:
            errors.append(
                "ingestion pipeline key collision %r at rows %r"
                % (key, owners)
            )
    for destination, owners in destinations.items():
        if all(destination) and len(owners) > 1:
            errors.append(
                "ingestion destination collision %r at rows %r"
                % (destination, owners)
            )
    return errors, warnings


def strict_failures(errors, warnings, strict=False):
    """Return the messages that should fail validation for the selected mode."""
    return list(errors) + (list(warnings) if strict else [])


def is_valid(errors, warnings, strict=False):
    """Return whether validation passes, optionally treating warnings as errors."""
    return not strict_failures(errors, warnings, strict=strict)


def validate_strict(rows, env):
    """Validate rows and return all errors plus strict-mode warnings."""
    errors, warnings = validate(rows, env)
    return strict_failures(errors, warnings, strict=True)
