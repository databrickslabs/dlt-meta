# Getting started with the SDP-META Databricks App

A browser-based front-end for SDP-META: onboard pipelines, preview the
generated DataflowSpec rows, deploy Lakeflow Spark Declarative Pipelines,
and monitor them — all without leaving the workspace.

> **Looking for something else?**
>
> - **I need to deploy the app into a workspace** → see [`README.md`](./README.md)
>   (prerequisites, `scripts/deploy_app.sh`, service-principal permissions,
>   local-dev setup).
> - **I'm already using the app and want the full panel-by-panel reference** →
>   see [`USER_GUIDE.md`](./USER_GUIDE.md).

---

## Who this is for

Pick the App path if any of these match:

- You'd rather **click than type CLI commands** for onboarding and deploy.
- You want **non-developers on your team** (analysts, PMs, customers)
  to be able to register pipelines without learning `databricks labs sdp-meta`.
- You're **demoing SDP-META** and want a visual story (the app ships
  five bundled demos — Cars, Multi-Source CDC, Silver Fanout, Cloud Files,
  and the DAIS end-to-end demo — that work out of the box).
- You want a **single deployable Databricks App** your org can share
  instead of every user installing the labs CLI locally.

If you're building a production CI/CD pipeline, **Path A (DAB) is still
the right answer** — the App is for exploration, demos, and human-in-the-loop
onboarding. Pick the right tool for the job.

---

## What you get

| Panel | What it does |
|---|---|
| **Onboarding** | Pick a bundled demo or upload your own spec → preview → submit. Renders `{uc_volume_path}` / `{uc_catalog_name}` / `{bronze_schema}` / `{silver_schema}` placeholders for you. |
| **DataflowSpecs** | Browse the bronze / silver DataflowSpec rows onboarding just wrote, by `data_flow_group`. Edit specs in-place with 3-layer validation (syntax → schema → semantics). |
| **Deployment** | Generate a Lakeflow Spark Declarative Pipeline from the selected `data_flow_group`. One click. |
| **Demos** | Five pre-built end-to-end examples that auto-fill all the required fields. **DAIS** is the default — exercises CDC, DQE, silver transformations in one pipeline. |
| **Monitor** | Pipeline list filtered to your SDP-META pipelines, with start / stop, in-app event stream, and click-through to the native Databricks pipeline UI. |
| **Metadata** | Browse UC catalogs / schemas / tables alongside the spec editor — no context-switching to the workspace catalog explorer. |

The canonical flow is **Onboarding → DataflowSpecs → Deployment**.
Each step auto-fills the next, so you only type the catalog / schema / table
names once.

---

## Two ways to run it

### Recommended: Deploy to Databricks Apps

```bash
# From the sdp-meta repo root
databricks auth login --host <WORKSPACE_HOST>
databricks apps create demo-sdp-meta

./scripts/deploy_app.sh \
  --profile <DATABRICKS_CLI_PROFILE> \
  --app     demo-sdp-meta \
  --path    /Workspace/Users/<you@databricks.com>/sdp-meta-app
```

That's it. Open the app's URL in the workspace's **Compute → Apps** tab
and you're done. The script handles wheel build, `app.yaml` injection, and
the interactive-demo notebook quirk for you; see [`README.md`](./README.md#deploy-to-databricks)
for what each step does and how to grant the App service principal the
Unity Catalog permissions it needs.

### Local dev (try before deploy)

```bash
git clone https://github.com/databrickslabs/sdp-meta.git
cd sdp-meta
python3 -m venv .venv && source .venv/bin/activate
pip install -r databricks_app/requirements.txt
pip install -e .

databricks auth login --host <WORKSPACE_HOST>
export SDP_META_HOME="$PWD"
export SDP_META_NO_BROWSER=1
flask --app databricks_app/app.py run --host 127.0.0.1 --port 8000
```

Open <http://127.0.0.1:8000>. Demos and deploys hit your real workspace —
just identity-wise they run as **you** locally, vs the App's service
principal when deployed. See [`README.md`](./README.md#run-locally-macos--linux)
for the full local-dev story including auth options and the identity caveat.

---

## 5-minute walkthrough

After the app loads (the onboarding page is the default landing):

1. **Pick a bundled demo.** The dropdown defaults to **DAIS Demo
   (end-to-end)** — the most comprehensive demo, exercises CDC + DQE +
   silver transforms in a single pipeline. If you want to start smaller,
   the dropdown also offers Cars (simplest), Multi-Source CDC, Silver
   Fanout, and Cloud Files.
2. **Fill in three names** — Unity Catalog, SDP Meta Schema (will hold
   the DataflowSpec rows), and Bronze / Silver schemas (where pipeline
   output lands). Everything else is pre-filled.
3. **Click Preview** to confirm the rendered template looks right.
4. **Click Onboard.** Watch the in-app log stream. Onboarding writes the
   `bronze_dataflowspec` and `silver_dataflowspec` rows into the schema
   you just named.
5. **Navigate to DataflowSpecs.** Pick the `data_flow_group` that was
   just written, browse the rows, and confirm they look right.
6. **Click Deploy.** Pipeline name, group, and target schema auto-fill
   from your onboarding session. Click Deploy → Databricks creates the
   pipeline → the success toast links you to its native UI.
7. **Navigate to Monitor.** Your new pipeline appears in the filtered
   list. Start it, watch events stream in, click through to the native
   pipeline UI when you want graph view / table lineage / SQL profiler.

---

## Where to next

- **Full feature reference** with screenshots of every panel:
  [`USER_GUIDE.md`](./USER_GUIDE.md)
- **Deploy-the-app docs** (App SP permissions, `scripts/deploy_app.sh`
  internals, local dev gotchas): [`README.md`](./README.md)
- **Move from demo to your own data:** open Onboarding → switch the
  source mode toggle from "Bundled demo" to "Unity Catalog Volume" or
  "Manual paths", point at your own onboarding template, and proceed
  with the same five-step flow.
- **Graduate to git-tracked CI/CD:** when the App's exploration value
  runs out, scaffold a Declarative Automation Bundle from the same
  onboarding spec — `databricks labs sdp-meta bundle-init --quickstart`
  ([Path A in `GETTING_STARTED.md`](../GETTING_STARTED.md#path-a--declarative-automation-bundle-recommended)).

---

## Path comparison

| Aspect | Path A (DAB) | Path B (CLI) | Path C (App) |
|---|---|---|---|
| **Best for** | Production, multi-target, CI/CD | First-touch exploration | Non-devs, demos, click-driven onboarding |
| **State** | Git-tracked YAML/JSON | Workspace only | Workspace only |
| **Multi-target promotion** | Yes (`dev`/`prod` targets) | Manual | Manual |
| **Auth** | OAuth / PAT via CLI | OAuth / PAT via CLI | App service principal (deployed) or your user (local) |
| **Setup cost** | Bundle scaffold + edits | `pip install` + 2 commands | One `scripts/deploy_app.sh` invocation |
| **Required skill** | Bundles / git / YAML | CLI | Browser |
