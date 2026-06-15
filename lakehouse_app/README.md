# SDP-META Lakehouse App

## Prerequisites

### System Requirements
- Python 3.10 or higher
- [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/tutorial.html) (v0.244.0 or later)
- Configured Databricks workspace access

### How paths work

The app relies on an environment variable `PYTHONPATH` that must point to the **root of the
sdp-meta repository** — the directory that contains both `src/` and `demo/`.  Every demo script
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
databricks apps create demo-sdp-meta
```

> Wait a couple of minutes for the app compute to provision.

### 3. Sync and deploy the **full sdp-meta repo**

The root-level `app.yaml` tells Databricks Apps to run `lakehouse_app/start.sh`,
which builds and installs the sdp-meta wheel and then launches Flask.
Deploy from the **repo root** so `demo/`, `src/`, and `integration_tests/` are
all present inside the container alongside the app.

```bash
# Run from the sdp-meta repo root
cd /path/to/sdp-meta

# Sync the full repo to your workspace
databricks sync . /Workspace/Users/<you@databricks.com>/sdp-meta

# Deploy using the repo root as the source path
databricks apps deploy demo-sdp-meta \
  --source-code-path /Workspace/Users/<you@databricks.com>/sdp-meta
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
**Databricks Web UI → New → App → search for `demo-sdp-meta`**

---

## Run locally (macOS / Linux)

### 1. Clone and install

```bash
git clone https://github.com/databrickslabs/sdp-meta.git
cd sdp-meta

python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Flask + the small set of runtime deps the App needs
pip install -r lakehouse_app/requirements.txt

# Install sdp-meta in editable mode so the App and demos can import it
# without rebuilding a wheel on every restart.
pip install -e .
```

### 2. Authenticate to Databricks

`app.py` constructs `WorkspaceClient()` with no arguments and lets the SDK's
default credential chain resolve auth. Pick **one** of the options below; the
demo subprocesses inherit the same env vars automatically.

| Option | Setup | When to use |
|---|---|---|
| **A. `[DEFAULT]` profile** *(recommended)* | `databricks auth login --host <WORKSPACE_HOST>` (OAuth U2M) — caches into `~/.databrickscfg [DEFAULT]`. Nothing else to set. | You only ever target one workspace. |
| **B. Named profile** | `databricks configure --profile <name> --host <WORKSPACE_HOST> --token <PAT>` then `export DATABRICKS_CONFIG_PROFILE=<name>` | You switch between workspaces. |
| **C. PAT env vars** | `export DATABRICKS_HOST=<WORKSPACE_HOST>` and `export DATABRICKS_TOKEN=<PAT>` | Throwaway shells, CI. |

Resolution order is: PAT env vars → `DATABRICKS_CONFIG_PROFILE` → `[DEFAULT]`
in `~/.databrickscfg`. **No `--profile` flag is needed** — if nothing is set
the SDK falls back to `[DEFAULT]` automatically.

Verify before launching the App:

```bash
python -c "from databricks.sdk import WorkspaceClient; \
  w = WorkspaceClient(); \
  print('host=', w.config.host, 'auth=', w.config.auth_type, \
        'as=', w.current_user.me().user_name)"
```

If that prints your workspace host and your username, the App will work.

### 3. Start Flask from the repo root

```bash
# Tell app.py where the repo root is (so demo/, src/, integration_tests/ resolve)
export DLT_META_HOME="$PWD"

# Suppress browser tabs that the CLI would otherwise pop after onboard/deploy
export SDP_META_NO_BROWSER=1

# Optional: hot reload on code edits
export FLASK_DEBUG=true

# Optional: pick a non-default profile (matches the Apps "--profile" semantic)
# export DATABRICKS_CONFIG_PROFILE=<name>

flask --app lakehouse_app/app.py run --host 127.0.0.1 --port 8000
```

Open **http://127.0.0.1:8000**.

> Port 8000 mirrors the production `start.sh` default (which binds
> `${DATABRICKS_APP_PORT:-8000}`). Use any free port if 8000 is taken.

### 4. Identity caveat (local vs Apps)

| Context | Identity that issues UC / job / pipeline calls |
|---|---|
| Databricks Apps container | App service principal (`app-XXXXXX_<app-name>`) |
| Local Mac/Linux | **You** (the user whose PAT or OAuth tokens are on disk) |

This means:

- The **Test App access** button (`/check-uc-grants`) probes **your** grants
  on the catalog, not an App SP's. Demos run if you have `USE CATALOG` +
  `CREATE SCHEMA` on the target catalog (you usually do).
- To exercise the production "grant required" failure path locally, point
  the App at a catalog where your user lacks `CREATE SCHEMA`.

### 5. Common gotchas

- **`ModuleNotFoundError: databricks.labs.sdp_meta`** — you skipped
  `pip install -e .` in step 1.
- **Browser tab opens after every CLI action** — set
  `export SDP_META_NO_BROWSER=1` (already in step 3 above).
- **Port already in use** — `lsof -i :8000` to find the holder, or pass a
  different `--port`.
- **`demo/SDP_META_INTERACTIVE_DEMO.py` not found** — only happens if
  `scripts/deploy_app.sh` ran against your tree and renamed it to
  `.nbsource`. Restore with `git checkout demo/SDP_META_INTERACTIVE_DEMO.py`.

### 6. Mirror the App container's full boot path (optional)

To exercise the exact code path the production runtime uses (wheel build +
install + Flask launch), run `start.sh` directly:

```bash
bash lakehouse_app/start.sh
```

Slower (rebuilds the wheel every time), but it's the closest you can get to
production locally short of deploying.

---

## Using the app

### Pipeline Setup tab

Two-step workflow:

1. **Onboarding (Step 1)** — fills the SDP-META dataflow spec tables in Unity Catalog with your
   pipeline metadata. Configure schemas, table names, and paths, then click **Run Onboarding**.

2. **Deployment (Step 2)** — creates and launches the Lakeflow Declarative Pipeline from the
   registered spec. Set the pipeline name, target schema, and layer, then click **Deploy Pipeline**.

### Demos tab

Pre-built end-to-end examples that exercise different SDP-META features.  Enter your Unity
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
