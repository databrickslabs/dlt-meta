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

setup(
    name="databricks-labs-sdp-meta",
    version="0.0.11",
    python_requires=">=3.8",
    setup_requires=["wheel>=0.37.1,<=0.42.0"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "IT": IT_REQUIREMENTS},
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
