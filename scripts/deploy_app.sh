#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# scripts/deploy_app.sh — deploy the dlt-meta databricks_app to Databricks Apps
#
# Why this script exists
# ──────────────────────
# The `databricks_app/app.py` Flask routes (/onboarding, /deploy, /rundemo)
# shell out to `src/cli.py` and `demo/launch_*_demo.py`. To make those work
# inside the running container, the Apps platform needs the FULL dlt-meta
# repo at the deployment source-code-path (Mode A in databricks_app/start.sh).
#
# Mode A requires `app.yaml` and `requirements.txt` at the source-code-path
# root. We deliberately keep those OUT of the local repo so the working tree
# matches upstream `databrickslabs/dlt-meta`. This script bridges the gap by
# staging the repo into a temp dir, dropping the two extra files there, and
# syncing the staging dir (not the working tree) to the workspace.
#
# Net effect:
#   - Local working tree stays identical to upstream (no root app.yaml).
#   - Workspace path has the full Mode A layout.
#   - Local edits to demo/, src/, databricks_app/ are reflected on every run.
#   - No GitHub clone at container startup → faster, works in air-gapped envs.
#
# Usage
# ─────
#   ./scripts/deploy_app.sh \
#       --profile <DATABRICKS_CLI_PROFILE> \
#       --path    /Workspace/Users/<you>/<app-folder>
#
#   # Or via env vars:
#   PROFILE=<profile> WORKSPACE_PATH=/Workspace/Users/<you>/<app-folder> \
#       ./scripts/deploy_app.sh
#
# --profile and --path are required; the script aborts otherwise. --app and
# --mode are optional (defaults below).
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Defaults (override with env vars or CLI flags) ───────────────────────────
# Empty defaults force callers to pass --profile / --path explicitly, so a
# fresh clone of the public repo never deploys against someone else's
# workspace. The script aborts below if either is missing.
PROFILE="${PROFILE:-}"
APP_NAME="${APP_NAME:-demo-sdp-meta}"
WORKSPACE_PATH="${WORKSPACE_PATH:-}"
DEPLOY_MODE="${DEPLOY_MODE:-SNAPSHOT}"

# ── CLI flag parsing ─────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --profile) PROFILE="$2"; shift 2 ;;
        --app)     APP_NAME="$2"; shift 2 ;;
        --path)    WORKSPACE_PATH="$2"; shift 2 ;;
        --mode)    DEPLOY_MODE="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,36p' "$0"  # print header comment as help
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            echo "Usage: $0 [--profile PROFILE] [--app NAME] [--path /Workspace/...] [--mode SNAPSHOT|AUTO_SYNC]" >&2
            exit 2
            ;;
    esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

# ── Required-argument guard ──────────────────────────────────────────────────
# Done after CLI parsing so flag-value > env-var > (no default). Keeps a
# fresh clone from accidentally targeting someone else's workspace.
missing=()
[[ -z "$PROFILE"        ]] && missing+=("--profile / \$PROFILE")
[[ -z "$WORKSPACE_PATH" ]] && missing+=("--path / \$WORKSPACE_PATH")
if (( ${#missing[@]} > 0 )); then
    echo "Error: missing required argument(s): ${missing[*]}" >&2
    echo "Run with -h for usage." >&2
    exit 2
fi

echo "──────────────────────────────────────────────────────────────────────"
echo " Repo root      : $REPO_ROOT"
echo " Profile        : $PROFILE"
echo " App name       : $APP_NAME"
echo " Workspace path : $WORKSPACE_PATH"
echo " Deploy mode    : $DEPLOY_MODE"
echo "──────────────────────────────────────────────────────────────────────"

# ── Sanity checks ────────────────────────────────────────────────────────────
command -v databricks >/dev/null || { echo "databricks CLI not found in PATH" >&2; exit 1; }
command -v rsync      >/dev/null || { echo "rsync not found in PATH" >&2; exit 1; }
[[ -f databricks_app/start.sh ]]        || { echo "databricks_app/start.sh not found" >&2; exit 1; }
[[ -f databricks_app/requirements.txt ]] || { echo "databricks_app/requirements.txt not found" >&2; exit 1; }
[[ -f databricks_app/app.py ]]          || { echo "databricks_app/app.py not found" >&2; exit 1; }
[[ -d src && -d demo ]]                || { echo "src/ and demo/ must exist at repo root" >&2; exit 1; }

# A root-level app.yaml in the working tree means someone manually copied the
# auto-generated staging file back. That's an anti-pattern: the staging copy
# in the tempdir is the source of truth, and a stale root copy can confuse
# users into thinking they need to maintain it. Reject early with a clear msg.
if [[ -f app.yaml ]]; then
    echo "Error: app.yaml exists at the repo root." >&2
    echo "       scripts/deploy_app.sh auto-generates app.yaml into a staging" >&2
    echo "       tempdir; never commit one at the root. Delete it and rerun." >&2
    exit 1
fi

# ── Stage repo into a temp directory ─────────────────────────────────────────
STAGING="$(mktemp -d -t dltmeta-deploy.XXXXXX)"
trap 'rm -rf "$STAGING"' EXIT

echo ">> Staging repo to $STAGING ..."
# Mirror the repo into staging while skipping things sync would reject anyway:
#   .git/         — version control metadata (huge, irrelevant to runtime)
#   .databricks/  — sync snapshots / local CLI state
#   __pycache__/  — compiled Python (regenerated in container)
#   *.pyc         — same
#   .venv/, venv/ — local virtualenvs
#   .DS_Store     — macOS finder turds
# .databricksignore is COPIED into staging so `databricks sync` will then
# also honour the project's own ignore list (docs/, examples/, tests/, etc.).
rsync -a \
    --exclude='.git/' \
    --exclude='.databricks/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='*.pyo' \
    --exclude='.venv/' \
    --exclude='venv/' \
    --exclude='.DS_Store' \
    --exclude='dist/' \
    --exclude='build/' \
    --exclude='*.egg-info/' \
    "$REPO_ROOT/" "$STAGING/"

# ── Inject Mode A entry-point files ──────────────────────────────────────────
echo ">> Writing Mode A app.yaml + requirements.txt at staging root ..."
cat > "$STAGING/app.yaml" <<'EOF'
# Auto-generated by scripts/deploy_app.sh — DO NOT COMMIT this file to the
# local repo. The Databricks Apps platform requires app.yaml at the source-
# code-path root; databricks_app/start.sh detects Mode A (full repo) and runs
# the Flask app from the repo root so demo/ and src/ resolve correctly.
command: [
  "bash",
  "databricks_app/start.sh"
]
EOF

cp "$REPO_ROOT/databricks_app/requirements.txt" "$STAGING/requirements.txt"

# ── Disguise the interactive demo notebook as a regular file ─────────────────
# `databricks sync` inspects file content and, when a ``.py`` file starts with
# ``# Databricks notebook source``, stores it in the workspace as a NOTEBOOK
# (extension stripped). The Apps platform then projects only FILE-typed
# entries into ``/app/python/source_code/`` — so a NOTEBOOK-typed
# ``demo/SDP_META_INTERACTIVE_DEMO`` never appears as
# ``demo/SDP_META_INTERACTIVE_DEMO.py`` inside the running container, and
# ``demo/launch_interactive_demo.py`` (which loads the notebook source via
# ``REPO_ROOT / "demo" / "SDP_META_INTERACTIVE_DEMO.py"``) crashes with
# "Demo notebook source not found".
#
# Renaming the staged copy to ``.nbsource`` (not ``.py``) bypasses
# auto-detection: the workspace stores it as a regular FILE, the container
# mounts it as a regular FILE, and ``databricks_app/start.sh`` restores the
# canonical ``.py`` filename in-place at startup. Bytes are unchanged, so
# the launcher's ``ws.workspace.upload(format=SOURCE, language=PYTHON)``
# call still ships the original magic-header content to the per-run notebook
# path inside the workspace.
NBSRC="$STAGING/demo/SDP_META_INTERACTIVE_DEMO.py"
if [[ -f "$NBSRC" ]]; then
    echo ">> Renaming demo/SDP_META_INTERACTIVE_DEMO.py -> .nbsource (sync-as-FILE workaround) ..."
    mv "$NBSRC" "${NBSRC}.nbsource"
fi

# ── Wipe the workspace path before re-syncing ────────────────────────────────
# Two reasons the wipe is required and not just nice-to-have:
#   1. Databricks workspace imports unpack `.dbc` archives into folders. A
#      subsequent `databricks sync --full` then fails with RESOURCE_ALREADY_
#      EXISTS on the unpacked notebook (the sync layer treats the unpacked
#      folder + the `.dbc` file as conflicting destinations).
#   2. `databricks sync --full` only deletes files it tracked in its own
#      snapshot. Anything synced from a previous, differently-rooted source
#      survives forever otherwise.
# Deleting and re-creating is fast (a few hundred small files) and gives us
# a deterministic Mode A layout each run.
echo ">> Wiping $WORKSPACE_PATH (clean redeploy) ..."
databricks workspace delete "$WORKSPACE_PATH" --recursive --profile "$PROFILE" 2>/dev/null || true

# ── Full sync staging -> workspace ───────────────────────────────────────────
# --full: make destination match source exactly (deletes stale files at dest).
# Sync respects the .databricksignore that lives inside the staging dir.
echo ">> Syncing staging -> $WORKSPACE_PATH (full, --profile $PROFILE) ..."
databricks sync "$STAGING" "$WORKSPACE_PATH" --full --profile "$PROFILE"

# ── Verify app.yaml landed in the workspace ──────────────────────────────────
# `databricks sync` honours .gitignore rules in the source tree. The staging
# tempdir is an rsync of the repo, so it inherits the repo's .gitignore — any
# rule there that accidentally matches `app.yaml` (e.g. a bare `/app.yaml`
# guard) silently strips the entrypoint config from the synced workspace and
# the App boots with no command, crashing on every start. Catch that here
# instead of leaving the operator to grep the Apps logz pane.
echo ">> Verifying app.yaml landed at $WORKSPACE_PATH ..."
if ! databricks workspace export "$WORKSPACE_PATH/app.yaml" --profile "$PROFILE" >/dev/null 2>&1; then
    echo "Error: app.yaml was not synced to $WORKSPACE_PATH/app.yaml." >&2
    echo "       Most likely cause: a .gitignore rule (or .databricksignore)" >&2
    echo "       matches 'app.yaml'. Inspect those files and remove the rule." >&2
    exit 1
fi

# ── Trigger app deployment ───────────────────────────────────────────────────
echo ">> Deploying app '$APP_NAME' (mode=$DEPLOY_MODE) ..."
databricks apps deploy "$APP_NAME" \
    --source-code-path "$WORKSPACE_PATH" \
    --profile "$PROFILE" \
    --mode "$DEPLOY_MODE"

echo ">> Done. App URL:"
databricks apps get "$APP_NAME" --profile "$PROFILE" --output json \
    | python3 -c "import json,sys; print(json.load(sys.stdin).get('url',''))"
