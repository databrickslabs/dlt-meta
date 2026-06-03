"""Setup file for SDP-META (primary package).

This is the primary package following Databricks Labs namespace conventions.
Package structure: databricks.labs.sdp_meta

For backwards compatibility wrapper (dlt-meta package), see the compat/ directory.
"""
from setuptools import setup, find_namespace_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = ["setuptools", "databricks-sdk", "PyYAML>=6.0"]

DEV_REQUIREMENTS = [
    "flake8==6.0",
    "delta-spark==3.0.0",
    "pytest>=7.0.0",
    "coverage>=7.0.0",
    "pyspark==3.5.5"
]

IT_REQUIREMENTS = ["typer[all]==0.6.1"]

# Opt-in toolchain for the OSS Apache Spark Declarative Pipelines path
# (Tier 2: ``spark-pipelines run``). Kept out of the baseline ``dev``
# pin on purpose — Tier-1 unit tests stub ``pyspark.pipelines`` and run
# on pyspark 3.5.5 / delta-spark 3.0.0, while the OSS runtime needs
# Spark 4.1+ (where ``pyspark.pipelines`` lives) and the Scala 2.13
# delta-spark 4.x build. Install with ``pip install -e ".[oss]"``.
# Requires Python >=3.9 (Spark 4 dropped 3.8).
OSS_REQUIREMENTS = ["pyspark[pipelines]>=4.1.0", "delta-spark>=4.0.0"]

setup(
    name="databricks-labs-sdp-meta",
    version="0.0.11",
    # Baseline (Lakeflow) supports 3.8. The ``[oss]`` extra additionally
    # needs Python >=3.9 (Spark 4 dropped 3.8) — pip can't express a
    # per-extra floor, so installing ``.[oss]`` on 3.8 fails at import,
    # not at install. Documented in OSS_REQUIREMENTS and the OSS guide.
    python_requires=">=3.8",
    setup_requires=["wheel>=0.37.1,<=0.42.0"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "IT": IT_REQUIREMENTS, "oss": OSS_REQUIREMENTS},
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="Databricks Labs SDP-META Framework (formerly DLT-META)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_dir={"": "src"},
    packages=find_namespace_packages(where="src", include=["databricks.*"]),
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
