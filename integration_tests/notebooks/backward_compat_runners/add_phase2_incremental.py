# Databricks notebook source
# Phase 2 incremental seed-data dropper.
#
# Runs at the start of the Phase 2 (v0.0.11 upgrade) workflow, BEFORE
# the bronze pipeline. Copies fresh JSON files from the per-run
# ``customers_phase2/`` and ``transactions_phase2/`` staging paths
# (already on UC volume — uploaded by the orchestrator) into the
# ORIGINAL source paths the dataflowspec points at
# (``customers/`` and ``transactions/``).
#
# Auto Loader is deterministic on file path: a NEW file in an
# already-watched directory is treated as new ingestion. So the very
# next pipeline run inside Phase 2 picks up exactly the rows we
# dropped here, on top of everything Phase 1 already wrote — proving
# v0.0.11 can resume incremental ingestion against tables and
# checkpoints first written by v0.0.10.
import os

uc_volume_path = dbutils.widgets.get("uc_volume_path").rstrip("/")
int_tests_dir = dbutils.widgets.get("int_tests_dir").strip("/")

PAIRS = (
    ("customers_phase2",  "customers"),
    ("transactions_phase2", "transactions"),
)

for staging, target in PAIRS:
    src_dir = f"{uc_volume_path}/{int_tests_dir}/resources/data/{staging}"
    dst_dir = f"{uc_volume_path}/{int_tests_dir}/resources/data/{target}"
    src_files = dbutils.fs.ls(src_dir)
    if not src_files:
        raise RuntimeError(f"Phase 2 staging path {src_dir} is empty")
    print(f"Copying {len(src_files)} file(s) from {src_dir} -> {dst_dir}")
    for f in src_files:
        # Force a unique destination filename so Auto Loader treats this
        # as a brand-new file even if (somehow) a same-named file already
        # landed in the watched directory.
        new_name = f"phase2_{os.path.basename(f.name)}"
        dst = f"{dst_dir}/{new_name}"
        dbutils.fs.cp(f.path, dst)
        print(f"  copied -> {dst}")

print("Phase 2 incremental seed copy complete.")
