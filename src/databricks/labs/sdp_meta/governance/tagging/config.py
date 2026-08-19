"""Load, validate, and expand catalog-scoped tag configuration."""

import re
from typing import Dict, Optional, Tuple

import yaml

from databricks.labs.sdp_meta.governance.tagging.models import Desired, Key
from databricks.labs.sdp_meta.identifiers import validate_uc_identifier

TAG_KEY_FORBIDDEN = set(".,-=/:")
RESERVED_PREFIXES = ("system.", "class.", "sap.")
MAX_TAG_LEN = 256
MAX_TAGS_PER_OBJECT = 50
MAX_COLUMN_TAGS_PER_TABLE = 1000
SOURCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class TaggingError(ValueError):
    """A user-correctable tagging configuration or preflight error."""


def fail(message: str) -> None:
    raise TaggingError(message)


def check_source_id(source_id: Optional[str]) -> Optional[str]:
    if source_id is None:
        return None
    if not isinstance(source_id, str) or not SOURCE_ID_RE.fullmatch(source_id):
        fail(
            "source_id must be 1-128 characters using letters, numbers, "
            "periods, underscores, or hyphens"
        )
    return source_id


def check_ident(name: str, what: str) -> str:
    try:
        return validate_uc_identifier(name, kind=what)
    except ValueError as error:
        fail(str(error))


def check_tag_map(tags: dict, where: str) -> dict:
    if not isinstance(tags, dict):
        fail(f"{where}: expected a map of tag key -> string value")
    if len(tags) > MAX_TAGS_PER_OBJECT:
        fail(f"{where}: more than {MAX_TAGS_PER_OBJECT} tags")
    output = {}
    for key, value in tags.items():
        if (
            not isinstance(key, str)
            or not key
            or key != key.strip()
            or len(key) > MAX_TAG_LEN
        ):
            fail(f"{where}: bad tag key {key!r}")
        reserved = key.startswith(RESERVED_PREFIXES)
        for char in key:
            if ord(char) < 32 or (
                char in TAG_KEY_FORBIDDEN and not (reserved and char == ".")
            ):
                fail(f"{where}: forbidden char {char!r} in tag key {key!r}")
        value = "" if value is None else value
        if (
            not isinstance(value, str)
            or value != value.strip()
            or len(value) > MAX_TAG_LEN
        ):
            fail(f"{where}: bad value for tag {key!r}")
        output[key] = value
    return output


def load_tags_file(path: str) -> dict:
    class StrictLoader(yaml.SafeLoader):
        pass

    def no_duplicates(loader, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            key = loader.construct_object(key_node, deep=deep)
            if key in seen:
                raise ValueError(f"duplicate key {key!r} in {path}")
            seen.add(key)
        return yaml.SafeLoader.construct_mapping(loader, node, deep)

    StrictLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, no_duplicates
    )
    with open(path, encoding="utf-8") as handle:
        document = yaml.load(handle, Loader=StrictLoader)
    if not isinstance(document, dict) or str(document.get("version")) != "1":
        fail(f'{path}: expected a mapping with version: "1"')
    unknown = set(document) - {"version", "source_id", "defaults", "tables"}
    if unknown:
        fail(f"{path}: unknown top-level nodes {sorted(unknown)}")
    if not isinstance(document.get("tables"), dict):
        fail(f"{path}: expected tables to be a mapping")
    defaults = document.get("defaults") or {}
    if not isinstance(defaults, dict):
        fail(f"{path}: defaults must be a mapping")
    unknown_defaults = set(defaults) - {"catalog", "schema"}
    if unknown_defaults:
        fail(f"{path}: unknown defaults {sorted(unknown_defaults)}")
    check_source_id(document.get("source_id"))
    return document


def _add(
    desired: Dict[Key, Desired],
    key: Key,
    value: str,
    contributor: tuple,
    where: str,
) -> None:
    current = desired.get(key)
    if current is None:
        desired[key] = Desired(value=value, contributors={contributor})
    elif current.value == value:
        current.contributors.add(contributor)
    else:
        fail(
            f"conflicting values for {key.label()}: "
            f"{current.value!r} (from {sorted(current.contributors)}) vs "
            f"{value!r} (from {where})"
        )


def node_assignments(
    node: dict,
    catalog: str,
    schema: str,
    table: str,
    contributor: tuple,
    desired: Dict[Key, Desired],
    where: str,
) -> None:
    if not isinstance(node, dict):
        fail(f"{where}: expected a mapping")
    unknown = set(node) - {"table", "columns"}
    if unknown:
        fail(f"{where}: unknown nodes {sorted(unknown)} (allowed: table, columns)")
    table_tags = check_tag_map(node.get("table") or {}, f"{where}.table")
    for key, value in table_tags.items():
        _add(
            desired,
            Key(catalog, schema, table, None, key),
            value,
            contributor,
            where,
        )
    columns = node.get("columns") or {}
    if not isinstance(columns, dict):
        fail(f"{where}.columns: expected a mapping")
    total_column_tags = 0
    for column, tag_map in columns.items():
        check_ident(column, f"{where} column")
        tag_map = check_tag_map(tag_map or {}, f"{where}.columns.{column}")
        total_column_tags += len(tag_map)
        for key, value in tag_map.items():
            _add(
                desired,
                Key(catalog, schema, table, column, key),
                value,
                contributor,
                where,
            )
    if total_column_tags > MAX_COLUMN_TAGS_PER_TABLE:
        fail(f"{where}: column tags exceed {MAX_COLUMN_TAGS_PER_TABLE}")


def resolve_target(
    target: str,
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> Tuple[str, str, str]:
    if not isinstance(target, str):
        fail(f"target name must be a string, got {target!r}")
    parts = target.split(".")
    if len(parts) == 1:
        if not default_catalog or not default_schema:
            fail(f"target {target!r} requires both --catalog and --schema")
        parts = [default_catalog, default_schema, parts[0]]
    elif len(parts) == 2:
        if not default_catalog:
            fail(f"target {target!r} requires --catalog")
        parts = [default_catalog, parts[0], parts[1]]
    elif len(parts) != 3:
        fail(
            f"target {target!r}: expected table, schema.table, or catalog.schema.table"
        )
    return tuple(check_ident(part, "target identifier") for part in parts)


def expand_desired(
    document: dict,
    default_catalog: Optional[str],
    default_schema: Optional[str],
    source_id: Optional[str] = None,
) -> Tuple[Dict[Key, Desired], set, set]:
    if not default_catalog:
        fail("one catalog is required via defaults.catalog or --catalog")
    default_catalog = check_ident(default_catalog, "default catalog")
    if default_schema:
        default_schema = check_ident(default_schema, "default schema")
    desired: Dict[Key, Desired] = {}
    contributors = set()
    tables = set()
    effective_source_id = check_source_id(
        source_id if source_id is not None else document.get("source_id")
    )
    if not effective_source_id:
        fail("source_id is required in the tags file or via --source-id")
    for target_name, node in document["tables"].items():
        target = resolve_target(target_name, default_catalog, default_schema)
        if target[0] != default_catalog:
            fail(
                f"table {target_name!r} resolves to catalog {target[0]!r}; "
                f"this file is scoped to catalog {default_catalog!r}"
            )
        contributor = ("target", effective_source_id, ".".join(target))
        contributors.add(contributor)
        tables.add(target)
        node_assignments(
            node or {},
            *target,
            contributor,
            desired,
            f"tables.{target_name}",
        )
    return desired, contributors, tables
