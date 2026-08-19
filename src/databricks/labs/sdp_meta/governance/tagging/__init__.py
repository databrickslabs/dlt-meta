"""Governed-tag assignment and configuration generation."""

from databricks.labs.sdp_meta.governance.tagging.applier import apply_tags
from databricks.labs.sdp_meta.governance.tagging.models import Action, Desired, Key

__all__ = ["Action", "Desired", "Key", "apply_tags"]
