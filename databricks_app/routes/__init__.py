"""Flask Blueprint registry for the SDP-META Databricks App.

Each route cluster lives in a sibling module that defines a Flask
``Blueprint``. ``register_blueprints(app)`` is the single entry point
``app.py`` calls to wire everything together \u2014 add a new route group
by creating ``routes/<name>.py`` and appending it to ``_BLUEPRINTS``.
"""

from __future__ import annotations

from flask import Flask

from .onboarding import bp as _onboarding_bp
from .deploy import bp as _deploy_bp
from .demo import bp as _demo_bp
from .pipelines import bp as _pipelines_bp
from .metadata_browse import bp as _metadata_browse_bp
from .spec_editor import bp as _spec_editor_bp
from .warehouse import bp as _warehouse_bp
from .dataflowspecs import bp as _dataflowspecs_bp


# Order is irrelevant for correctness \u2014 every route prefix is
# already unique. Kept in a stable order for log readability.
_BLUEPRINTS = (
    _onboarding_bp,
    _deploy_bp,
    _demo_bp,
    _pipelines_bp,
    _metadata_browse_bp,
    _spec_editor_bp,
    _warehouse_bp,
    _dataflowspecs_bp,
)


def register_blueprints(app: Flask) -> None:
    """Register every blueprint listed in ``_BLUEPRINTS`` on ``app``."""
    for bp in _BLUEPRINTS:
        app.register_blueprint(bp)
