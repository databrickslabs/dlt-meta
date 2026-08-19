"""Generate a flat tags YAML skeleton from SDP-META target metadata."""

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import yaml

from databricks.labs.sdp_meta.governance.tagging.config import check_source_id

PHYSICAL_TARGETS = (
    ("bronze", False),
    ("bronze", True),
    ("silver", False),
    ("silver", True),
)


def fail(message: str) -> None:
    raise ValueError(message)


class StrictLoader(yaml.SafeLoader):
    """YAML loader that rejects duplicate mapping keys."""


def _no_duplicate_keys(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            fail(f"duplicate key {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _no_duplicate_keys,
)


def _json_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail(f"duplicate key {key!r}")
        result[key] = value
    return result


def load_onboarding(path: str) -> List[dict]:
    text = Path(path).read_text(encoding="utf-8")
    suffix = Path(path).suffix.lower()
    if suffix in (".yaml", ".yml"):
        payload = yaml.load(text, Loader=StrictLoader)
    else:
        payload = json.loads(text, object_pairs_hook=_json_pairs)
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list) or not all(
        isinstance(row, dict) for row in payload
    ):
        fail("onboarding input must be a row object or a list of row objects")
    return payload


def _resolved(value, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{field} is required")
    value = value.strip()
    if "{" in value or "}" in value:
        fail(
            f"{field} contains unresolved placeholder {value!r}; "
            "render the environment-specific onboarding file first"
        )
    return value


def resolve_onboarding_target(
    row: dict,
    layer: str,
    quarantine: bool,
    environment: str,
) -> Tuple[str, str, str]:
    table_field = f"{layer}_quarantine_table" if quarantine else f"{layer}_table"
    schema_field = (
        f"{layer}_database_quarantine_{environment}"
        if quarantine
        else f"{layer}_database_{environment}"
    )
    catalog_field = (
        f"{layer}_catalog_quarantine_{environment}"
        if quarantine
        else f"{layer}_catalog_{environment}"
    )
    table = _resolved(row.get(table_field), table_field)
    schema_value = _resolved(row.get(schema_field), schema_field)
    catalog_value = row.get(catalog_field)
    schema_parts = schema_value.split(".")
    if len(schema_parts) == 2:
        packed_catalog, schema = schema_parts
        if catalog_value:
            catalog = _resolved(catalog_value, catalog_field)
            if catalog != packed_catalog:
                fail(
                    f"{catalog_field}={catalog!r} conflicts with "
                    f"{schema_field}={schema_value!r}"
                )
        else:
            catalog = packed_catalog
    elif len(schema_parts) == 1:
        schema = schema_parts[0]
        catalog = _resolved(catalog_value, catalog_field)
    else:
        fail(f"{schema_field} must be schema or catalog.schema, got {schema_value!r}")
    return catalog, schema, table


def output_name(
    target: Tuple[str, str, str],
    default_catalog: Optional[str],
    default_schema: Optional[str],
) -> str:
    catalog, schema, table = target
    if catalog == default_catalog:
        if schema == default_schema:
            return table
        return f"{schema}.{table}"
    return f"{catalog}.{schema}.{table}"


def convert(
    rows: List[dict],
    environment: str,
    default_catalog: Optional[str] = None,
    default_schema: Optional[str] = None,
    source_id: Optional[str] = None,
) -> Tuple[str, List[str]]:
    discovered = set()
    for row in rows:
        for layer, quarantine in PHYSICAL_TARGETS:
            table_field = (
                f"{layer}_quarantine_table" if quarantine else f"{layer}_table"
            )
            if not row.get(table_field):
                continue
            discovered.add(
                resolve_onboarding_target(row, layer, quarantine, environment)
            )

    catalogs = {target[0] for target in discovered}
    if default_catalog:
        selected = {target for target in discovered if target[0] == default_catalog}
        if not selected:
            fail(f"onboarding contains no targets in catalog {default_catalog!r}")
    else:
        if len(catalogs) > 1:
            fail(
                "onboarding contains multiple catalogs; run once per catalog "
                "with --catalog"
            )
        default_catalog = next(iter(catalogs), None)
        selected = discovered

    defaults = {}
    if default_catalog:
        defaults["catalog"] = default_catalog
    if default_schema:
        defaults["schema"] = default_schema
    targets = [
        output_name(target, default_catalog, default_schema)
        for target in sorted(selected)
    ]
    document = {"version": "1"}
    effective_source_id = check_source_id(
        source_id
        or (
            f"{default_catalog}-{environment}-tags"
            if default_catalog
            else None
        )
    )
    if effective_source_id:
        document["source_id"] = effective_source_id
    if defaults:
        document["defaults"] = defaults
    document["tables"] = {}
    rendered = yaml.safe_dump(
        document,
        sort_keys=False,
        default_flow_style=False,
    )
    if targets:
        rendered += (
            "\n# Discovered targets are comments because an active empty target "
            "requests cleanup.\n"
            "# Replace `tables: {}` above with `tables:` and uncomment only "
            "targets with desired tags.\n"
        )
        for target in targets:
            rendered += (
                f"#   {target}:\n"
                "#     table:\n"
                "#       <tag-key>: <tag-value>\n"
                "#     columns:\n"
                "#       <column-name>:\n"
                "#         <tag-key>: <tag-value>\n"
            )
    return rendered, targets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="onboarding JSON/YAML")
    parser.add_argument(
        "--output",
        required=True,
        help="output customer-editable tags YAML skeleton",
    )
    parser.add_argument(
        "--environment",
        required=True,
        help="onboarding environment suffix, such as dev or prod",
    )
    parser.add_argument(
        "--catalog",
        help="catalog to select; generate a separate file for each catalog",
    )
    parser.add_argument(
        "--schema",
        help="default schema used to shorten matching output target names",
    )
    parser.add_argument(
        "--source-id",
        help=(
            "stable configuration owner ID; defaults to "
            "<catalog>-<environment>-tags"
        ),
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="replace an existing output file after explicit review",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        output_path = Path(args.output)
        if output_path.exists() and not args.overwrite:
            fail(
                f"output already exists: {args.output}; choose a new path or "
                "pass --overwrite explicitly"
            )
        rendered, targets = convert(
            load_onboarding(args.input),
            args.environment,
            args.catalog,
            args.schema,
            args.source_id,
        )
        output_path.write_text(
            rendered,
            encoding="utf-8",
        )
    except (OSError, ValueError, json.JSONDecodeError, yaml.YAMLError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(
        f"Wrote {len(targets)} commented table examples to {args.output}; "
        "uncomment targets only after adding desired tags"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
