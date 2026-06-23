<p align="center">
  <img src="docs/static/img/sdp-meta-readme-banner.png" alt="SDP-META — Metadata-driven Lakeflow Spark Declarative Pipelines" width="900"/>
</p>

<!-- Top bar will be removed from PyPi packaged versions -->
<!-- Dont remove: exclude package -->

[Documentation](https://databrickslabs.github.io/dlt-meta/) |
[Release Notes](CHANGELOG.md) |
[Examples](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf)

<!-- Dont remove: end exclude package -->

---

[![Documentation](https://img.shields.io/badge/docs-passing-green)](https://databrickslabs.github.io/dlt-meta/) [![PyPI](https://img.shields.io/pypi/v/databricks-labs-sdp-meta?label=pypi)](https://pypi.org/project/databricks-labs-sdp-meta/) [![Build](https://img.shields.io/github/actions/workflow/status/databrickslabs/dlt-meta/onpush.yml?branch=main)](https://github.com/databrickslabs/dlt-meta/actions/workflows/onpush.yml) [![Coverage](https://img.shields.io/codecov/c/github/databrickslabs/dlt-meta)](https://codecov.io/gh/databrickslabs/dlt-meta) [![Style](https://img.shields.io/badge/code%20style-flake8-blue)](https://github.com/PyCQA/flake8) [![PyPI Downloads](https://static.pepy.tech/badge/dlt-meta/month)](https://pepy.tech/projects/dlt-meta)

---

## Project Overview

`SDP-META` is a metadata-driven framework for [Lakeflow Spark Declarative Pipelines](https://www.databricks.com/product/data-engineering/spark-declarative-pipelines). Define your Bronze and Silver pipelines in a JSON or YAML onboarding file — a single generic Declarative Pipeline reads the resulting DataflowSpec at runtime and builds the full processing graph automatically. No pipeline code to write.

### Components

#### Metadata Interface

- **Onboarding file** (JSON or YAML) — sources, targets, CDC config, DQE rules. Examples: [`demo/conf/json/onboarding.template`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/onboarding.template) · [`demo/conf/yml/onboarding.template.yml`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/onboarding.template.yml)
- **Data Quality Expectations** — per-table JSON or YAML rule files. Examples: [`demo/conf/json/dqe/customers/`](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/json/dqe/customers) · [`demo/conf/yml/dqe/customers/`](https://github.com/databrickslabs/dlt-meta/tree/main/demo/conf/yml/dqe/customers)
- **Silver transformation file** — SQL `select_exp` and `where_clause` definitions. Examples: [`demo/conf/json/silver_transformations.json`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/json/silver_transformations.json) · [`demo/conf/yml/silver_transformations.yml`](https://github.com/databrickslabs/dlt-meta/blob/main/demo/conf/yml/silver_transformations.yml)

#### Generic Lakeflow Spark Declarative Pipeline

- Reads DataflowSpec at runtime and dynamically wires sources, transformations, expectations, CDC flows, and sinks
- Supports Autoloader, Delta, Kafka, Eventhub, and Snapshot sources
- Applies [`create_auto_cdc_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes), [`append_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow), and [`create_sink`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink) based on metadata

## High-Level Process Flow

![SDP-META Architecture](https://raw.githubusercontent.com/databrickslabs/dlt-meta/feature/sdp-meta/docs/static/img/sdp-meta-architecture.svg)

## Feature Matrix

### Pipeline Capabilities

| Feature | Layers |
|---|---|
| Input sources — Autoloader, Delta, Kafka, Eventhub, Snapshot | Bronze, Silver |
| Medallion architecture | Bronze → Silver |
| Bronze ↔ Silver pipeline chaining (`layer=bronze_silver`) | Both |
| Custom transformation functions | Bronze, Silver |
| Data Quality Expectations | Bronze, Silver |
| Quarantine table | Bronze, Silver |
| Liquid clustering | Bronze, Bronze Quarantine, Silver |
| [`create_auto_cdc_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes) — CDC via `bronze_cdc_apply_changes` | Bronze, Silver |
| Multi-source CDC (`bronze_cdc_apply_changes_flows` / `silver_cdc_apply_changes_flows`) | Bronze, Silver |
| [`create_auto_cdc_from_snapshot_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot) — Snapshot CDC | Bronze |
| [`append_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) — via `bronze_append_flows` | Bronze |
| [`create_sink`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-sink) — Delta and Kafka sinks | Bronze, Silver |
| Row filters | Bronze, Silver |

### Deployment & Tooling

| Tool | Description |
|---|---|
| [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/) | Git-tracked pipelines, `dev`/`prod` targets, CI/CD-ready. Commands: `bundle-init`, `bundle-prepare-wheel`, `bundle-add-flow`, `bundle-validate`. See [`DAB_README.md`](DAB_README.md). |
| [SDP-META CLI](https://databrickslabs.github.io/dlt-meta/docs/getting-started/cli) | `databricks labs sdp-meta onboard` · `deploy` · `bundle-*` |
| [SDP-META App](https://github.com/databrickslabs/dlt-meta/tree/main/databricks_app) | Browser-based UI for onboarding, deployment, and pipeline monitoring |
| [MCP Server](https://databrickslabs.github.io/dlt-meta/getting-started/mcp) | AI-assisted pipeline scaffolding via Claude, Cursor, and compatible tools |

## Getting Started

Refer to the [Getting Started](https://databrickslabs.github.io/dlt-meta/docs/getting-started) docs for the long form. The short form, in order of recommendation:

1. **Use the [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) interface** for any real work — `dev`/`prod` targets, git-tracked state, CI/CD-ready. New developers can use `bundle-init --quickstart` to skip every prompt and get a working bundle in one command. This is the recommended path; the interactive `onboard`/`deploy` CLI below is kept for first-touch exploration only.
2. **Use the interactive `onboard` + `deploy` CLI** if you just want to kick the tires against a single workspace.

### Pre-requisites (both paths)

- Python 3.10, 3.11, or 3.12. The pinned `pyspark==3.5.5` test stack does not support Python 3.13+; using 3.13 / 3.14 will surface as cloudpickle / recursion errors at test time. See [Troubleshooting](#troubleshooting) below.
- Databricks CLI v0.213 or later. See [install instructions](https://docs.databricks.com/en/dev-tools/cli/tutorial.html).
  - macOS: ![macos_install_databricks](https://raw.githubusercontent.com/databrickslabs/dlt-meta/feature/sdp-meta/docs/static/img/macos_1_databrickslabsmac_installdatabricks.gif)
  - Windows: ![windows_install_databricks.png](https://raw.githubusercontent.com/databrickslabs/dlt-meta/feature/sdp-meta/docs/static/img/windows_install_databricks.png)
- Authenticate your machine to a workspace:
  ```bash
  databricks auth login --host WORKSPACE_HOST
  ```
  (Add `--debug` to any sdp-meta command to enable debug logs.)
- Install the labs plugin:
  ```bash
  databricks labs install sdp-meta
  ```

### Path A — Declarative Automation Bundle (recommended)

For developer-onramp and any non-exploration use (multi-target promotion, git-tracked pipeline state, CI/CD), scaffold a bundle:

```bash
# Zero-prompt fast path: scaffolds ./my_sdp_meta_pipeline with developer-friendly
# defaults (cloudFiles + bronze_silver + split + pypi). Edit
# resources/variables.yml afterwards to point at your real catalog/schema and
# replace the __SET_ME__ sentinel for sdp_meta_dependency.
databricks labs sdp-meta bundle-init --quickstart

# Or interactive, walking through every knob (recommended the first time):
databricks labs sdp-meta bundle-init

cd <bundle_name>

# Optional: build a local wheel and upload to a UC volume instead of using PyPI.
# Paste the printed /Volumes/... path into resources/variables.yml as the
# default for `sdp_meta_dependency`.
databricks labs sdp-meta bundle-prepare-wheel

# Append flows interactively, or in bulk from CSV / generated by recipes
# (see recipes/README.md inside the bundle).
databricks labs sdp-meta bundle-add-flow

# sdp-meta-specific sanity checks (placeholder values in onboarding *and*
# in databricks.yml, layer/topology consistency, wheel_source vs
# sdp_meta_dependency, dataflow_group references) on top of
# `databricks bundle validate`.
databricks labs sdp-meta bundle-validate

# Deploy + run end-to-end.
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev
databricks bundle run pipelines --target dev
```

What you get with the bundle path:

- **Git-tracked pipeline state** — every onboarding row, expectation, transformation, and pipeline definition lives in YAML/JSON files inside the bundle.
- **`dev` and `prod` targets** out of the box, with development-mode overrides (single-node clusters, no schedules, prefixed table names) and a commented `run_as: { service_principal_name: <your-...> }` block in prod for CI/CD.
- **`pipeline_mode` switch** — render bronze + silver as two separate Lakeflow Spark Declarative Pipelines (`split`, the default) or as a single combined pipeline (`combined`).
- **Recipes** for programmatically generating onboarding entries from real workspace state: `from_uc.py` (existing UC tables), `from_volume.py` (CSVs in a UC volume), `from_topics.py` (Kafka / Event Hub topic lists), `from_inventory.py` (inventory CSV).
- **`bundle-validate` static checks** that catch authoring mistakes the upstream `databricks bundle validate` doesn't (unedited `<your-...>` placeholders in either onboarding or `databricks.yml`, mis-typed `dataflow_group` references, `pipeline_mode` mismatches, sentinel `__SET_ME__` left in place, wheel_source vs sdp_meta_dependency drift, etc.).

Full reference: [`DAB_README.md`](DAB_README.md). Runnable end-to-end walkthrough with sample data: [`demo/README.md#dab-demo`](demo/README.md#dab-demo).

### Path B — Interactive `onboard` + `deploy` CLI (exploration only)

For first-touch exploration against a single workspace. State lives in the workspace, not in git, and there's no native multi-target promotion — graduate to Path A as soon as you want any of those.

If you want to run the existing demo files, set up the repo first:

1. Clone & enter the repo, create a venv, install dependencies:
   ```bash
   git clone https://github.com/databrickslabs/sdp-meta.git
   cd sdp-meta

   # Use Python 3.11 or 3.12 — pyspark==3.5.5 (pinned in setup.py) does
   # not support Python 3.13+ and will surface as cloudpickle / recursion
   # errors at test time.
   python -m venv .venv && source .venv/bin/activate

   # Runtime-only install (mirrors INSTALL_REQUIRES in setup.py):
   pip install -r requirements.txt

   # Or, for development + running the test suite (also installs the
   # project itself in editable mode so `from databricks.labs.sdp_meta...`
   # resolves to the working tree):
   pip install -r requirements-dev.txt
   ```
2. Onboard:
   ```bash
   databricks labs sdp-meta onboard
   ```
   ![onboardingDLTMeta_2.gif](https://raw.githubusercontent.com/databrickslabs/dlt-meta/feature/sdp-meta/docs/static/img/sdp_meta_onboard.gif)

   Pushes code+data to your workspace, creates an onboarding job, and opens the job URL in your browser.

3. Deploy:
   ```bash
   databricks labs sdp-meta deploy
   ```
   ![deployingDLTMeta_bronze_silver.gif](https://raw.githubusercontent.com/databrickslabs/dlt-meta/feature/sdp-meta/docs/static/img/sdp_meta_deploy.gif)

   Deploys the Lakeflow Spark Declarative Pipeline and opens its URL in your browser.

### Local development & tests

```bash
flake8 src tests
python -m coverage run -m pytest tests/ -v
python -m coverage report -m
```

`setup.py` is the source of truth for dependency versions. `requirements.txt` mirrors `INSTALL_REQUIRES`, `requirements-dev.txt` mirrors the `dev` / `IT` / `mcp` extras plus `-e .`. Update all three together when you change a pin.

### Troubleshooting

- **`_pickle.PicklingError: ... RecursionError: Stack overflow` on most tests** — your venv is on Python 3.13/3.14. The pinned `pyspark==3.5.5` doesn't support 3.13+. Rebuild the venv on 3.10, 3.11, or 3.12 (`python3.11 -m venv .venv && source .venv/bin/activate && pip install -r requirements-dev.txt`).
- **`ModuleNotFoundError: No module named 'databricks.labs'` at test collection** — the venv has the third-party deps but not the project. Run `pip install -e .` (or `pip install -r requirements-dev.txt`, which already does this).

## Resources

- [Documentation](https://databrickslabs.github.io/dlt-meta/)
- [FAQ](https://databrickslabs.github.io/dlt-meta/faq)
- [Release Notes](CHANGELOG.md)
- [GitHub Issues](https://github.com/databrickslabs/dlt-meta/issues)

## Project Support

Please note that all projects released under [`Databricks Labs`](https://www.databricks.com/learn/labs)
are provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements
(SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket
relating to any issues arising from the use of these projects.

Any issues discovered through the use of this project should be filed as issues on the Github Repo.  
They will be reviewed as time permits, but there are no formal SLAs for support.
