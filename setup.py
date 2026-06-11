"""Setup file for SDP-META (primary package).

This is the primary package following Databricks Labs namespace conventions.
Package structure: databricks.labs.sdp_meta

For backwards compatibility wrapper (dlt-meta package), see the compat/ directory.

Legacy v0.0.10 ``src.*`` import compatibility
---------------------------------------------
The wheel produced by this setup also bundles two compat surfaces
sourced from ``compat/``:

  * ``dlt_meta`` (from ``compat/dlt_meta/``) -- flat re-export
    package for v0.0.10 users who already migrated to
    ``from dlt_meta import …``. Shipping it inside the v0.0.11 main
    wheel means ``pip install databricks-labs-sdp-meta`` is the only
    thing customers need; they don't have to track a second package.
  * ``src`` (from ``compat/src/``) -- real Python package whose
    ``__init__.py`` populates ``sys.modules`` with ``src.<sub>`` ->
    ``databricks.labs.sdp_meta.<sub>`` aliases at import time. This
    is what makes a v0.0.10 customer's runner notebook keep working
    unchanged after their wheel is upgraded to v0.0.11. The
    customer's ``from src.dataflow_pipeline import …`` line resolves
    through normal Python import machinery: load ``src/__init__.py``,
    register aliases, fetch the canonical module from
    ``sys.modules``.

The earlier iteration of this surface relied on a ``dlt_meta.pth``
file at the wheel's purelib root to lazy-load the alias map at
interpreter startup via CPython's ``site.py``. That worked on a
normal ``pip install`` followed by a fresh ``python`` process, but
serverless DLT's ``%pip install`` magic does NOT trigger a fresh
``site.py`` ``.pth`` scan -- it lays files into site-packages and
hands the next cell back to the SAME interpreter, so the freshly-
installed ``.pth`` is silently ignored. Resolving through a real
package (``compat/src/``) sidesteps the ``.pth`` lifecycle entirely.

The standalone ``compat/`` package remains as a no-op PyPI redirect
(``install_requires=["databricks-labs-sdp-meta>=0.0.11"]``) so
``pip install dlt-meta`` keeps working from PyPI; it ships a
duplicate of the same shim it would otherwise install transitively,
which pip detects as already-satisfied and no-ops.
"""
from setuptools import setup, find_namespace_packages, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = [
    "setuptools", 
    "databricks-sdk", 
    "PyYAML>=6.0",
    "dbldatagen>=0.3.0",  # For synthetic data generation
    "sqlalchemy>=1.4.0",  # For PostgreSQL slot management
    "psycopg2-binary>=2.9.0"  # PostgreSQL driver
]

DEV_REQUIREMENTS = [
    "flake8==6.0",
    "delta-spark==3.0.0",
    "pytest>=7.0.0",
    "coverage>=7.0.0",
    "pyspark==3.5.5"
]

IT_REQUIREMENTS = ["typer[all]==0.6.1"]

MCP_REQUIREMENTS = ["mcp>=1.0,<2.0"]


setup(
    name="databricks-labs-sdp-meta",
    version="0.0.11",
    python_requires=">=3.8",
    setup_requires=["wheel>=0.37.1,<=0.42.0"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "IT": IT_REQUIREMENTS, "mcp": MCP_REQUIREMENTS},
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="Databricks Labs SDP-META Framework (formerly DLT-META)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # Three roots, one wheel:
    #   ``""`` -> ``src/``           : canonical ``databricks.labs.sdp_meta``
    #   ``dlt_meta`` -> ``compat/dlt_meta``  : flat re-export shim
    #   ``src`` -> ``compat/src``    : v0.0.10 ``src.*`` re-export package
    #
    # The exact-match keys (``dlt_meta``, ``src``) win over the ``""``
    # glob, so the source-layout ``src/`` dir (which holds the
    # canonical namespace) is scanned via ``find_namespace_packages``
    # while the legacy ``src`` PACKAGE is sourced from
    # ``compat/src/__init__.py``. There is no on-disk collision -- the
    # source-layout ``src/`` directory has no ``__init__.py`` so
    # ``find_namespace_packages(where="src")`` can't accidentally pick
    # it up as a package itself.
    package_dir={
        "": "src",
        "dlt_meta": "compat/dlt_meta",
        "src": "compat/src",
    },
    packages=(
        find_namespace_packages(where="src", include=["databricks.*"])
        + find_packages(
            where="compat",
            include=["dlt_meta", "dlt_meta.*", "src", "src.*"],
        )
    ),
    entry_points={
        "console_scripts": [
            "sdp-meta=databricks.labs.sdp_meta.__main__:main",
        ],
        "group_1": "run=databricks.labs.sdp_meta.__main__:main",
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"
    ],
)
