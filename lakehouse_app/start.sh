#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# DLT-META Lakehouse App — startup script
#
# Works in two deployment modes:
#
#  Mode A — Full repo deployed (recommended):
#    Workspace sync path: .../dlt-meta          (repo root)
#    App source-code-path: .../dlt-meta
#    Container layout:
#      /app/python/source_code/
#        setup.py, src/, demo/, integration_tests/, lakehouse_app/
#
#  Mode B — Only lakehouse_app/ deployed (current/legacy):
#    Workspace sync path: .../dlt-meta/lakehouse_app
#    App source-code-path: .../dlt-meta/lakehouse_app
#    Container layout:
#      /app/python/source_code/
#        app.py, start.sh, templates/, ...   (no demo/, no src/)
#    → start.sh clones the full repo to /tmp/dlt-meta and uses that.
#
# In both modes the script exports DLT_META_HOME so app.py knows where
# demo/ and src/ live, then starts Flask from that directory so relative
# paths inside demo scripts (./demo/conf/...) resolve correctly.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

REPO_URL="https://github.com/databrickslabs/dlt-meta.git"

# Directory that contains this script (= source root in Mode B,
# or lakehouse_app/ subdirectory in Mode A)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── Detect deployment mode ────────────────────────────────────────────────────
# In Mode A the parent of SCRIPT_DIR is the repo root (has demo/ and src/).
# In Mode B SCRIPT_DIR itself is the source root and has no demo/ or src/.
PARENT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -d "$PARENT_DIR/demo" ] && [ -d "$PARENT_DIR/src" ]; then
    # Mode A — full repo deployed, repo root is one level up
    REPO_ROOT="$PARENT_DIR"
    echo "[start.sh] Mode A: full repo found at $REPO_ROOT"

elif [ -d "$SCRIPT_DIR/demo" ] && [ -d "$SCRIPT_DIR/src" ]; then
    # Mode A variant — deployed directly from repo root (start.sh at root)
    REPO_ROOT="$SCRIPT_DIR"
    echo "[start.sh] Mode A (root): full repo found at $REPO_ROOT"

else
    # Mode B — only lakehouse_app/ deployed; clone the full repo
    REPO_ROOT="/tmp/dlt-meta"
    if [ -d "$REPO_ROOT/.git" ]; then
        echo "[start.sh] Mode B: using cached clone at $REPO_ROOT"
        cd "$REPO_ROOT" && git pull --quiet --ff-only || true
    else
        echo "[start.sh] Mode B: cloning $REPO_URL → $REPO_ROOT ..."
        git clone --depth=1 "$REPO_URL" "$REPO_ROOT"
        echo "[start.sh] Clone complete."
    fi
fi

echo "[start.sh] Repo root: $REPO_ROOT"

# Export so app.py/_repo_root() picks it up — highest-priority override
export DLT_META_HOME="$REPO_ROOT"

# ── Verify required directories ───────────────────────────────────────────────
for dir in demo src integration_tests; do
    if [ ! -d "$REPO_ROOT/$dir" ]; then
        echo "[start.sh] ERROR: '$dir/' not found under $REPO_ROOT" >&2
        exit 1
    fi
    echo "[start.sh] Found $REPO_ROOT/$dir/ ✓"
done

# ── Restore the interactive demo notebook source ─────────────────────────────
# scripts/deploy_app.sh renames demo/SDP_META_INTERACTIVE_DEMO.py to
# demo/SDP_META_INTERACTIVE_DEMO.py.nbsource at staging time so workspace
# sync stores it as a regular FILE (otherwise the magic header would cause
# Databricks to store it as a NOTEBOOK with the .py stripped, and the
# Apps platform would then never project it as a .py source file inside
# /app/python/source_code/).
#
# demo/launch_interactive_demo.py hard-codes the source path as
# ``REPO_ROOT / "demo" / "SDP_META_INTERACTIVE_DEMO.py"`` (we deliberately
# don't modify demo/ files), so we copy the staged ``.nbsource`` back into
# place here. Bytes are unchanged; the launcher's ``ws.workspace.upload()``
# call still ships the original magic-header content to its per-run target.
#
# Local-dev runs (where the working tree already has the canonical .py
# alongside) skip the copy: we only restore when the .py is missing.
NBSRC_STAGED="$REPO_ROOT/demo/SDP_META_INTERACTIVE_DEMO.py.nbsource"
NBSRC_CANONICAL="$REPO_ROOT/demo/SDP_META_INTERACTIVE_DEMO.py"
if [ -f "$NBSRC_STAGED" ] && [ ! -f "$NBSRC_CANONICAL" ]; then
    echo "[start.sh] Restoring demo/SDP_META_INTERACTIVE_DEMO.py from .nbsource staging copy ..."
    cp "$NBSRC_STAGED" "$NBSRC_CANONICAL"
fi
if [ -f "$NBSRC_CANONICAL" ]; then
    echo "[start.sh] Found $NBSRC_CANONICAL ✓"
else
    echo "[start.sh] WARN: $NBSRC_CANONICAL not present — Interactive Notebook demo will fail with 'Demo notebook source not found'." >&2
fi

# ── Install build tools ───────────────────────────────────────────────────────
echo "[start.sh] Installing build tools..."
pip install --quiet wheel setuptools

# ── Build the sdp-meta wheel ──────────────────────────────────────────────────
echo "[start.sh] Building sdp-meta wheel from $REPO_ROOT ..."
cd "$REPO_ROOT"
python setup.py bdist_wheel --quiet

WHL=$(ls "$REPO_ROOT/dist/databricks_labs_sdp_meta-"*.whl 2>/dev/null | sort -V | tail -1)
if [ -z "$WHL" ]; then
    echo "[start.sh] ERROR: wheel build produced no .whl file." >&2
    exit 1
fi
echo "[start.sh] Built: $WHL"

# ── Install the wheel ─────────────────────────────────────────────────────────
echo "[start.sh] Installing $WHL ..."
pip install --quiet --force-reinstall "$WHL"
echo "[start.sh] sdp-meta installed successfully."

# ── Start Flask from the repo root ───────────────────────────────────────────
# Running from $REPO_ROOT means relative paths in demo scripts
# (e.g. './demo/conf/json/...') resolve correctly.
#
# The Databricks Apps runtime injects DATABRICKS_APP_PORT and routes traffic
# to it; the process MUST bind that port. `flask run` defaults to 5000 and
# does NOT read DATABRICKS_APP_PORT (it reads FLASK_RUN_PORT, which the
# platform doesn't set), so we pass --port explicitly. Falls back to 8000
# for local runs where the env var is absent.
APP_PORT="${DATABRICKS_APP_PORT:-8000}"
echo "[start.sh] Starting Flask (cwd=$REPO_ROOT, port=$APP_PORT) ..."
cd "$REPO_ROOT"
exec flask --app "$SCRIPT_DIR/app.py" run --host 0.0.0.0 --port "$APP_PORT"
