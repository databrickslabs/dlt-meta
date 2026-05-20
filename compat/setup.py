"""Setup file for dlt-meta compatibility wrapper.

This package provides backwards compatibility for users migrating from dlt-meta to databricks-labs-sdp-meta.
It re-exports all functionality from databricks-labs-sdp-meta and logs deprecation warnings.
"""
from setuptools import setup, find_packages

setup(
    name="dlt-meta",
    version="0.0.11",
    python_requires=">=3.8",
    install_requires=[
        "databricks-labs-sdp-meta>=0.0.11",  # Depends on the new primary package
    ],
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="DLT-META Framework (Compatibility wrapper - please migrate to sdp-meta)",
    long_description="""
# DLT-META Compatibility Package

**DEPRECATED**: This package is a compatibility wrapper. Please migrate to `sdp-meta`.

## Migration

Replace:
```bash
pip install dlt-meta
```

With:
```bash
pip install sdp-meta
```

All functionality is identical. This wrapper package will be maintained for backwards
compatibility but new features will only be added to `sdp-meta`.
    """,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    entry_points={"group_1": "run=dlt_meta:main"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Development Status :: 7 - Inactive",  # Indicates deprecated
        "Intended Audience :: Developers",
    ],
)
