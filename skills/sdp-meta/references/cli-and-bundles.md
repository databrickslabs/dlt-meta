# CLI commands and the DAB workflow

Commands are Databricks Labs CLI extensions (see `labs.yml`). Install once with
`databricks labs install sdp-meta`, then invoke `databricks labs sdp-meta <cmd>`.

> **Boolean flags:** always use the `=` syntax (`--build-and-upload-whl=true`,
> `--quickstart=true`). The Labs CLI string-flag parser otherwise consumes the
> next CLI token as the flag's value.

## Core lifecycle

### `onboard`
Reads the onboarding file and writes the dataflowspec into bronze/silver Delta
tables. Interactive prompts collect catalog, schema, layer
(`bronze` | `silver` | `bronze_silver`), and dataflowspec table names.

```bash
databricks labs sdp-meta onboard --profile <profile>
```

### `deploy`
Creates a Lakeflow SDP pipeline that reads the dataflowspec and materializes the
tables, then starts an update. Run **after** `onboard`.

```bash
databricks labs sdp-meta deploy --profile <profile>
```

### Wheel-delivery flags (shared by `onboard` and `deploy`)
Used when the pipeline/onboarding-job cluster can't reach PyPI to install
`databricks-labs-sdp-meta`:

| Flag | Purpose |
|------|---------|
| `--build-and-upload-whl=true` | Build the local wheel, upload to a UC Volume, and use/bake that wheel. Requires UC. |
| `--whl-file-path=/Volumes/.../x.whl` | Use an existing uploaded wheel. Mutually exclusive with `--build-and-upload-whl`. |
| `--git-branch`, `--git-url` | Build the wheel from a specific git ref instead of local tree. |
| `--uc-schema` / `--uc-schema-name`, `--uc-volume` / `--uc-volume-name` | Where the wheel volume lives. |
| `--no-create-missing-uc` | Do not auto-create the UC schema/volume. |
| `--pip-index-url`, `--pip-extra-index-url` | Forwarded to `pip wheel`. |
| `--profile` | Databricks CLI profile for auth + upload. |

> On **serverless** pipelines the wheel is delivered through the runner
> notebook's `%pip install` (baked in by these flags), **not** as a pipeline
> `whl` library — serverless DLT rejects whl-typed pipeline libraries.

## Asset Bundle (DAB) workflow — recommended

Infrastructure-as-code path: one bundle wires the onboarding job + the SDP
pipeline + variables + recipes.

### `bundle-init`
Scaffold a new bundle from the packaged template.

```bash
# Guided:
databricks labs sdp-meta bundle-init --output-dir=.
# Zero-prompt developer default (cloudFiles + bronze_silver + split + pypi):
databricks labs sdp-meta bundle-init --quickstart=true --output-dir=.
```
Quickstart lands the bundle at `<output-dir>/my_sdp_meta_pipeline`. Edit
`resources/variables.yml` to point at your real catalog/schema and
`sdp_meta_dependency` (the wheel source) afterward.

### `bundle-add-flow`
Append flow entries to the bundle's `conf/onboarding.{yml,json}` — interactive
single-flow prompts or batch from CSV. Auto-increments `data_flow_id` and seeds
`silver_transformations.*` for new silver flows.

```bash
databricks labs sdp-meta bundle-add-flow          # interactive
databricks labs sdp-meta bundle-add-flow --csv flows.csv   # batch
```

### `bundle-prepare-wheel`
Build the local wheel and upload it to a UC Volume (for
`wheel_source=volume_path` bundles); prints the `/Volumes/...` path to paste into
`resources/variables.yml`.

### `bundle-validate`
Run `databricks bundle validate` **plus** sdp-meta sanity checks: layer/topology
consistency, `wheel_source` vs `sdp_meta_dependency`, unresolved onboarding
placeholders, and `dataflow_group` references.

```bash
databricks labs sdp-meta bundle-validate
```

### Deploy the bundle
Once validation passes, use the standard DAB commands:

```bash
databricks bundle deploy --profile <profile>
databricks bundle run    --profile <profile>
```

## Typical end-to-end (bundle) sequence

```bash
databricks labs sdp-meta bundle-init --quickstart=true --output-dir=.
cd my_sdp_meta_pipeline
$EDITOR resources/variables.yml                 # catalog / schema / sdp_meta_dependency
databricks labs sdp-meta bundle-add-flow --csv new_tables.csv
databricks labs sdp-meta bundle-validate
databricks bundle deploy && databricks bundle run
```
