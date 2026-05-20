---
title: "Deploy with Declarative Automation Bundles"
date: 2026-04-21
weight: 8
draft: false
---

sdp-meta ships a [Declarative Automation Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) (DAB) template so you can deploy the onboarding job and the Lakeflow Spark Declarative Pipelines (LDP) from a plain bundle — no interactive `onboard`/`deploy` CLI, state lives in git, promotes across `dev`/`prod`.

## Prerequisites

- Python 3.8+
- Databricks CLI v0.213 or later on `PATH`
- `databricks labs install sdp-meta`

## Scaffold a new bundle

Two ways to scaffold:

```bash
# Fast path: zero prompts, dev-friendly defaults.
# Equivalent to walking through every prompt and accepting the default,
# except `sdp_meta_dependency` is intentionally left as the __SET_ME__
# sentinel so bundle-validate forces you to make a conscious choice
# (PyPI coordinate vs UC volume wheel) before deploy.
databricks labs sdp-meta bundle-init --quickstart

# Or interactive (recommended the first time so you understand the knobs):
databricks labs sdp-meta bundle-init
```

`--quickstart` accepts an `--output-dir <path>` flag too; the interactive mode also asks for the output directory. The non-quickstart path then invokes `databricks bundle init` against the packaged template. Prompts:

| Prompt | Description |
| --- | --- |
| `bundle_name` | Folder name and job/pipeline prefix. |
| `uc_catalog_name` | Unity Catalog catalog holding the sdp-meta schema and target schemas. |
| `sdp_meta_schema` | Schema for `bronze_dataflowspec` / `silver_dataflowspec` tables. |
| `bronze_target_schema` / `silver_target_schema` | Schemas for bronze/silver LDP outputs. |
| `layer` | `bronze`, `silver`, or `bronze_silver`. |
| `pipeline_mode` | Only used when `layer=bronze_silver`. `split` (default) deploys two LDP pipelines (silver depends on bronze); `combined` deploys ONE LDP pipeline that materializes both layers in the same DAG. |
| `source_format` | `cloudFiles` / `delta` / `kafka` / `eventhub` / `snapshot` — used to seed the onboarding example. |
| `onboarding_file_format` | `yaml` or `json`. |
| `dataflow_group` | `data_flow_group` that ties flows in the onboarding file to the LDP pipeline's `*.group`. |
| `wheel_source` | `pypi` or `volume_path`. Picks the install style; `bundle-validate` enforces that `sdp_meta_dependency` matches. |
| `sdp_meta_dependency` | Concrete install spec. PyPI coordinate or `/Volumes/...` wheel path. The default `__SET_ME__` sentinel is rejected by validate AND by the runner notebook so a placeholder can't slip through. |
| `author` | Written to the `import_author` column on dataflowspec rows. |

The rendered bundle looks like:

```
<bundle_name>/
├── databricks.yml
├── README.md
├── resources/
│   ├── variables.yml
│   ├── sdp_meta_onboarding_job.yml
│   └── sdp_meta_pipelines.yml
├── notebooks/
│   └── init_sdp_meta_pipeline.py
└── conf/
    ├── onboarding.yml                     # edit me
    ├── silver_transformations.yml         # silver layers only
    └── dqe/example_table/bronze_expectations.yml
```

## Installing sdp-meta on the pipelines

The job and both pipelines install sdp-meta from the `sdp_meta_dependency` variable. The `wheel_source` prompt records which install style this bundle uses so `bundle-validate` can enforce consistency.

### Option A — `wheel_source=pypi` (recommended once published)

At the prompts, set `wheel_source=pypi` and `sdp_meta_dependency=databricks-labs-sdp-meta==0.0.11` (pin a real version).

```yaml
# resources/variables.yml
wheel_source:
  default: pypi
sdp_meta_dependency:
  default: databricks-labs-sdp-meta==0.0.11
```

### Option B — `wheel_source=volume_path` (today, while sdp-meta isn't on PyPI)

At the prompts, set `wheel_source=volume_path` and accept the `__SET_ME__` default for `sdp_meta_dependency`. Then:

```bash
cd <bundle_name>
databricks labs sdp-meta bundle-prepare-wheel
# prompts for uc-catalog, uc-schema, uc-volume
```

The command builds the local wheel, uploads it to the volume, and prints the resulting path. Paste it into `resources/variables.yml`:

```yaml
wheel_source:
  default: volume_path
sdp_meta_dependency:
  default: /Volumes/<catalog>/<schema>/<volume>/databricks_labs_sdp_meta-0.0.11-py3-none-any.whl
```

## Validate before deploy

```bash
cd <bundle_name>
databricks labs sdp-meta bundle-validate
```

Runs `databricks bundle validate` plus sdp-meta-specific checks:

- onboarding file exists under `conf/`
- `dataflow_group` variable is referenced by at least one flow in the onboarding file
- `layer` variable matches the pipelines actually declared (`bronze`/`silver`/both)
- `sdp_meta_dependency` is not the `__SET_ME__` placeholder
- `sdp_meta_dependency` shape matches `wheel_source` (PyPI coordinate vs `/Volumes/...` path)
- no `<your-...>` placeholders left in `conf/onboarding.{yml,json}` (eventhub keys, kafka brokers, etc.)
- no `<your-...>` placeholders left in `databricks.yml` itself — typically an uncommented `run_as.service_principal_name` that still says `<your-prod-service-principal-application-id>`
- all YAML/JSON files parse cleanly

The runner notebook also fails fast on the `__SET_ME__` sentinel before `%pip install`, so a placeholder can't slip past validate.

## Deploy and run

```bash
databricks bundle deploy --target dev
databricks bundle run onboarding --target dev   # writes dataflow specs
databricks bundle run pipelines --target dev    # runs bronze + silver LDP
```

## Promote to prod

The template ships with `dev` and `prod` targets. Edit `databricks.yml` to pin the prod workspace and group permissions, then:

```bash
databricks bundle deploy --target prod
```

Per-target overrides (e.g. different catalogs per environment) go under `targets.<name>.variables` in `databricks.yml`.

### CI/CD: `run_as` for prod

For automated deploys you almost always want prod jobs and pipelines to run under a service principal so the bundle is decoupled from any one user. The template ships a commented `run_as` block in the prod target as a starting point:

```yaml
targets:
  prod:
    mode: production
    # run_as:
    #   service_principal_name: <your-prod-service-principal-application-id>
```

To opt in:

1. Uncomment the two `run_as` lines.
2. Replace `<your-prod-service-principal-application-id>` with the **application_id** (a UUID, not the display name) of a workspace-admin-granted service principal. `databricks service-principals list` shows both columns.
3. Re-run `databricks labs sdp-meta bundle-validate` — it walks the parsed `databricks.yml` and fails loudly if you uncommented but forgot to substitute. Comments are stripped at YAML parse time, so the shipped commented block stays silent until you uncomment.

The same `<your-...>` convention extends to anything you add — admin user_name on `permissions`, a workspace host pin, and so on.

## Choosing split vs combined pipelines

When `layer=bronze_silver`:

- **`pipeline_mode=split` (default)** — bundle ships two LDP pipelines (`bronze`, `silver`) and a job that runs silver after bronze. Each pipeline has its own update history, can be rolled back independently, and writes to its own LDP-managed schema. Recommended when the two layers have very different SLAs or when you want to drop/recompute one without disturbing the other.
- **`pipeline_mode=combined`** — bundle ships one LDP pipeline (`bronze_silver`) whose `configuration.layer = bronze_silver` makes sdp-meta materialize both layers in a single DAG. Lower fixed overhead (one pipeline to schedule and monitor), one update cycle, single set of expectations metrics. Best when bronze→silver is a tight unit and you don't need independent lifecycle management.

You can switch later: change `pipeline_mode` in `resources/variables.yml`, redeploy, and re-run.

## Adding flows with `bundle-add-flow`

Hand-authoring 50 flow entries is tedious. Use the helper:

```bash
# Single flow (interactive prompts per source_format)
databricks labs sdp-meta bundle-add-flow

# Bulk from CSV — starter template at conf/samples/flows.csv
databricks labs sdp-meta bundle-add-flow
# (pick "csv" mode and point at the file)
```

The command:

- pulls bundle defaults (catalog, target schemas, layer, dataflow_group, file format) from `resources/variables.yml`, so new flows match the surrounding bundle's conventions;
- auto-increments `data_flow_id` (`auto` = `max(existing)+1`, defaulting to `100` if empty);
- refuses to write on `data_flow_id` collisions;
- preserves YAML or JSON format of the existing onboarding file;
- supports a `--dry-run`-style preview via the "Dry run" prompt;
- validates source-format-specific required args (`kafka` needs bootstrap+topic; `eventhub` needs a name).

After adding flows, run `bundle-validate` to confirm the bundle is still consistent.

## Extending the seeded bundle

- **Add more flows** — use `bundle-add-flow` (above), or hand-edit `conf/onboarding.*`. Each flow needs `data_flow_id`, `data_flow_group`, `source_format`, `source_details`, plus the appropriate `bronze_*` / `silver_*` fields. Re-run `databricks bundle run onboarding`.
- **Add DQE rules** — create `conf/dqe/<table>/bronze_expectations.*` and point `bronze_data_quality_expectations_json_*` at it in the onboarding file.
- **Silver projections** — edit `conf/silver_transformations.*`, one entry per target table.
- **Multiple groups** — you can deploy multiple LDP pipelines in one bundle by duplicating the resource in `resources/sdp_meta_pipelines.yml` and pointing each at a different `*.group` value.

## Customizing the runner notebook

The seeded `notebooks/init_sdp_meta_pipeline.py` invokes `DataflowPipeline.invoke_dlt_pipeline(spark, layer)`. That call accepts optional kwargs that hook user code into the pipeline:

| Use case | kwarg | Example |
| --- | --- | --- |
| Snapshot ingestion (`source_format: snapshot`) — return next snapshot DataFrame + version on each tick | `bronze_next_snapshot_and_version` | [`examples/sdp_meta_pipeline_snapshot.ipynb`](https://github.com/databrickslabs/dlt-meta/blob/main/examples/sdp_meta_pipeline_snapshot.ipynb) |
| CDC into silver via `apply_changes_from_snapshot` | `silver_next_snapshot_and_version` | [`examples/sdp_meta_pipeline_apply_changes_from_snapshot.ipynb`](https://github.com/databrickslabs/dlt-meta/blob/main/examples/sdp_meta_pipeline_apply_changes_from_snapshot.ipynb) |
| Custom bronze / silver transformations beyond `silver_transformations.*` | `bronze_custom_transform_func`, `silver_custom_transform_func` | [`examples/sdp_meta_pipeline_custom_transform.ipynb`](https://github.com/databrickslabs/dlt-meta/blob/main/examples/sdp_meta_pipeline_custom_transform.ipynb) |

Add the callbacks in **cell 2** of the runner notebook (or a new third cell) and pass them to `invoke_dlt_pipeline(...)`. **Do not modify cell 1** — serverless LDP requires the exact `spark.conf.get(...)` + `%pip install $var` layout it ships with; adding multi-line Python before `%pip install` causes `SyntaxError: incomplete input` at pipeline startup.

See the *Extending the runner notebook* section of [`DAB_README.md`](https://github.com/databrickslabs/dlt-meta/blob/main/DAB_README.md#extending-the-runner-notebook) in the repo for the full copy-paste-ready code blocks per pattern.
