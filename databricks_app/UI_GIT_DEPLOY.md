# Deploying the SDP-META App via the Databricks Apps UI + Git folder

Click-only deployment path. No `databricks` CLI, no `rsync` / `robocopy`,
no PowerShell — just the workspace UI and a published GitHub branch.

For the standard CLI-based deploy flow (`scripts/deploy_app.sh` /
`scripts/deploy_app.ps1`), see the **Deploy to Databricks** section of
[README.md](./README.md#deploy-to-databricks).

---

## How it works

`databricks_app/start.sh` has two boot modes:

- **Mode A** — the App's source-code path *is* the repo root (or contains
  sibling `src/` + `demo/`). `start.sh` builds the wheel and starts
  Flask directly. This is what `scripts/deploy_app.sh` produces.
- **Mode B** — the App's source-code path is `databricks_app/` only, with
  no `src/` / `demo/` next to it. `start.sh` detects this and clones the
  full `databricks/sdp-meta` repo into `/tmp/sdp-meta` at container
  start. Wheel build, demo discovery, and Flask all run from that fresh
  clone.

The Git-folder UI flow uses **Mode B**. `app.yaml` and `requirements.txt`
are already committed inside `databricks_app/`, so the Apps platform
finds them at the source-code-path root with zero injection.

---

## Steps

### 1. Clone the repo into a Databricks Git folder

In the workspace UI:

1. **Workspace → Users → `<your-email>` → Add → Git folder**
2. **Git repository URL:** `https://github.com/databrickslabs/sdp-meta.git`
3. **Git provider:** GitHub
4. **Git folder name:** keep the default (`sdp-meta`)
5. **Git branch:** `main` (or whichever branch you want to deploy)
6. Click **Create Git folder**.

You should now see `/Workspace/Users/<your-email>/sdp-meta/` containing
the full repo.

### 2. Create the App

1. **Compute → Apps → Create app**
2. **App name:** `demo-sdp-meta` (or any name you prefer)
3. **Template:** Custom
4. Click **Create**.
5. Wait 1–2 minutes for the App compute to provision (you'll see
   **Compute: running** when it's ready).

### 3. Point the App at `databricks_app/` and deploy

In the App detail page → **Deploy** (or **Source code** / **Settings**
depending on the workspace UI version):

- **Source code path:**
  `/Workspace/Users/<your-email>/sdp-meta/databricks_app`

  **Critical:** point at `databricks_app/` (the subfolder), **not** the
  repo root. This is what activates `start.sh`'s Mode B — the
  in-container clone of `sdp-meta` that exposes `setup.py`, `src/`,
  `demo/`, and `integration_tests/`.

Click **Deploy**. First boot takes 60–120 seconds (clones the repo,
builds the wheel, installs it, starts gunicorn).

### 4. Grant the App service principal Unity Catalog permissions

The App runs as `app-XXXXXX_<app-name>`. Find the exact SP name in
the App's **Settings → Service principal** field, then have a catalog
admin run:

```sql
GRANT USE CATALOG ON CATALOG <target_catalog>
    TO `app-XXXXXX_demo-sdp-meta`;
GRANT CREATE SCHEMA, USE SCHEMA ON CATALOG <target_catalog>
    TO `app-XXXXXX_demo-sdp-meta`;
GRANT CREATE TABLE ON SCHEMA <target_catalog>.<bronze_schema>
    TO `app-XXXXXX_demo-sdp-meta`;
GRANT CREATE TABLE ON SCHEMA <target_catalog>.<silver_schema>
    TO `app-XXXXXX_demo-sdp-meta`;
```

The Demos tab's **Test App access** button (`/check-uc-grants`) shows
exactly which grants are missing if these aren't enough.

### 5. Open the App

Click the URL at the top of the App detail page — looks like
`https://demo-sdp-meta-<hash>.<region>.databricksapps.com`.

---

## Updates and re-deploys

- **New commits on the branch?** Open the Git folder in the workspace →
  click **Pull** (or use the Git folder's auto-sync if enabled) → open
  the App → click **Deploy** again.
- The container caches `/tmp/sdp-meta` across restarts and runs
  `git pull --ff-only` on subsequent boots, so warm starts are fast.

---

## When this path *won't* work

| Constraint | Why it breaks |
|---|---|
| App compute can't reach `github.com` (air-gapped / strict egress firewall) | Mode B's `git clone https://github.com/databrickslabs/sdp-meta.git` fails at container boot. Use the CLI deploy script instead — it ships the full repo into the workspace ahead of time. |
| You want to deploy unmerged local changes | Git folders only see what's been pushed to the configured branch. Use the CLI deploy script (or push a feature branch and point the Git folder at it). |
| You need to pin to a fork or a private mirror | Mode B's `REPO_URL` is hard-coded to `databricks/sdp-meta`. Either edit `start.sh` on your branch, or use the CLI deploy script (which syncs your working tree regardless of remote). |
