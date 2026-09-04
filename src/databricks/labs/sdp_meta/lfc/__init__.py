"""Lakeflow Connect support for SDP-META.

The package root intentionally exposes only lightweight persistence models.
Rendering, deployment, onboarding, and credential helpers remain available
from their dedicated modules without importing optional SDK dependencies here.
"""

from databricks.labs.sdp_meta.lfc.models import IngestionDataflowSpec

__all__ = ["IngestionDataflowSpec"]
