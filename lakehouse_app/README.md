# DLT-META Lakehouse App

## Prerequisites

### System Requirements
- Python 3.10 or higher
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) (v0.244.0 or later)
- Configured Databricks workspace access

### How paths work

The app relies on an environment variable `PYTHONPATH` that must point to the **root of the
dlt-meta repository** — the directory that contains both `src/` and `demo/`.  Every demo script
and the onboarding/deploy CLI live inside that tree, so this must be set correctly before the
app will work.

| Deployment | Where to set `PYTHONPATH` |
|---|---|
| Databricks App | App environment variable in the workspace UI |
| Local dev | Export in your shell before starting the server |

---

## Deploy to Databricks

### 1. Authenticate

```bash
databricks auth login --host WORKSPACE_HOST
```

### 2. Create the app

```bash
databricks apps create demo-dlt-meta
```

> Wait a couple of minutes for the app compute to provision.

### 3. Sync and deploy the **full dlt-meta repo**

The root-level `app.yaml` tells Databricks Apps to run `lakehouse_app/start.sh`,
which builds and installs the sdp-meta wheel and then launches Flask.
Deploy from the **repo root** so `demo/`, `src/`, and `integration_tests/` are
all present inside the container alongside the app.

```bash
# Run from the dlt-meta repo root
cd /path/to/dlt-meta

# Sync the full repo to your workspace
databricks sync . /Workspace/Users/<you@databricks.com>/dlt-meta

# Deploy using the repo root as the source path
databricks apps deploy demo-dlt-meta \
  --source-code-path /Workspace/Users/<you@databricks.com>/dlt-meta
```

**What happens at container startup (`start.sh`):**
1. Installs `wheel` and `setuptools` build tools
2. Runs `python setup.py bdist_wheel` to build `databricks_labs_sdp_meta-*.whl`
3. `pip install`s the wheel (brings in `databricks-sdk`, `PyYAML`, etc.)
4. Verifies `demo/` and `integration_tests/` are present
5. Starts Flask from the repo root so relative paths in demo scripts resolve correctly

**Directory layout inside the container (`/app/python/source_code/`):**
```
setup.py
app.yaml
requirements.txt
src/                   ← sdp-meta source (built into wheel by start.sh)
demo/                  ← demo scripts (no copy needed — already here)
integration_tests/     ← imported by demo scripts
lakehouse_app/
    app.py
    start.sh
    templates/
```

> **No extra environment variable needed.** The app derives the repo root from
> `lakehouse_app/../` automatically.  Set `DLT_META_HOME` only if you use a
> non-standard layout.

### 6. Access the app

Open the URL shown in step 2, or navigate:
**Databricks Web UI → New → App → search for `demo-dlt-meta`**

---

## Run locally

### 1. Clone and install

```bash
git clone https://github.com/databrickslabs/dlt-meta.git
cd dlt-meta

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install Flask
pip install -r requirements.txt

# Build and install the sdp-meta wheel
python setup.py bdist_wheel
pip install dist/databricks_labs_sdp_meta-*.whl
```

### 2. Configure Databricks

```bash
databricks configure --host <WORKSPACE_HOST> --token <PAT>
```

### 3. Start the server from the repo root

```bash
# Must run from repo root so demo/ and integration_tests/ are on sys.path
flask --app lakehouse_app/app.py run
```

Access the app at **http://127.0.0.1:5000**

---

## Using the app

### Pipeline Setup tab

Two-step workflow:

1. **Onboarding (Step 1)** — fills the DLT-META dataflow spec tables in Unity Catalog with your
   pipeline metadata. Configure schemas, table names, and paths, then click **Run Onboarding**.

2. **Deployment (Step 2)** — creates and launches the Lakeflow Declarative Pipeline from the
   registered spec. Set the pipeline name, target schema, and layer, then click **Deploy Pipeline**.

### Demos tab

Pre-built end-to-end examples that exercise different DLT-META features.  Enter your Unity
Catalog name and click a demo card to launch it.

| Demo | What it runs |
|---|---|
| Cloud Files | Auto Loader ingestion from cloud file sources |
| Apply Changes Snapshot | CDC with SCD Type 1 from full snapshots |
| Silver Fanout | Fan-out from bronze into multiple silver tables |
| DAIS Demo | Databricks AI Summit end-to-end walkthrough |
| DLT Sink | Pipeline output to an external sink destination |
| DABs | Full CI/CD deploy via Databricks Asset Bundles |

### App service principal permissions

The app runs as a dedicated service principal whose name follows the form
`app-XXXXXX_<app-name>` (the prefix is platform-assigned, the suffix matches
the App resource name).
Grant it the following in your Unity Catalog:

- `USE CATALOG` on the target catalog
- `CREATE SCHEMA`, `USE SCHEMA` on the target schemas
- `CREATE TABLE` on the bronze and silver schemas
