"""Setup file."""
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = ["setuptools"]

DEV_REQUIREMENTS = [
    "flake8>=5.0",
    "delta-spark==2.0.2"
]

IT_REQUIREMENTS = ["typer[all]==0.6.1"]

package_long_description = """###Databricks Labs DLT-META Framework###
    The Databricks Labs DLT META is a metadata-driven Databricks Delta Live Tables (aka DLT) framework
     which lets you automate your bronze and silver pipelines.
    """
setup(
    name="dlt_meta",
    version="0.0.21",
    python_requires=">=3.8",
    setup_requires=["wheel>=0.37.1,<=0.40.0"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "IT": IT_REQUIREMENTS},
    author="Ravi Gawai",
    author_email="databrickslabs@databricks.com",
    license="Databricks License",
    description="DLT-META Framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=["src"]),
    entry_points={"group_1": "run=src.__main__:main"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Testing",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators"
    ],
)
