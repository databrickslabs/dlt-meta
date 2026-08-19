"""Reusable governance-tag fixture generation for pipeline integration tests."""

from pathlib import Path
from typing import Iterable, List

import yaml

from databricks.labs.sdp_meta.governance.tagging.generator import (
    convert,
    load_onboarding,
)

INTEGRATION_TAG_KEY = "sdp_meta_it_managed"
INTEGRATION_TAG_VALUE = "true"


def generate_integration_tags_file(
    onboarding_paths: Iterable[str],
    output_path: str,
    environment: str,
    catalog: str,
    default_schema: str,
    source_id: str,
) -> List[str]:
    """Discover onboarding targets and create an active tags file for IT use.

    Product ``generate-tags`` intentionally emits only commented target
    examples. Integration tests need active, non-empty assignments so the
    post-pipeline ``apply_tags`` task exercises real Unity Catalog DDL.
    """
    rows = []
    for onboarding_path in onboarding_paths:
        if onboarding_path and Path(onboarding_path).exists():
            rows.extend(load_onboarding(onboarding_path))
    if not rows:
        raise ValueError("no rendered onboarding rows available for tag generation")

    rendered, targets = convert(
        rows,
        environment,
        default_catalog=catalog,
        default_schema=default_schema,
        source_id=source_id,
    )
    if not targets:
        raise ValueError("onboarding contains no taggable physical targets")

    document = yaml.safe_load(rendered)
    document["tables"] = {
        target: {
            "table": {
                INTEGRATION_TAG_KEY: INTEGRATION_TAG_VALUE,
            }
        }
        for target in targets
    }

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        yaml.safe_dump(document, sort_keys=False, default_flow_style=False),
        encoding="utf-8",
    )
    return targets
