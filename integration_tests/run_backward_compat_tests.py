"""Backward-compatibility integration test: generic SOURCE -> TARGET upgrade.

Goal
----
A customer's existing pipeline running on SOURCE_VERSION must keep working
unchanged when the wheel is swapped to TARGET_VERSION. No notebook edits,
no onboarding redo, no DLT checkpoint resets, no extra wheels.

Two profiles ship out of the box -- see ``version_profiles.py``:

  * ``LEGACY``   -- v0.0.1 through v0.0.10 (dlt_meta dist, ``from src.*``
                    runner imports, ``dlt_meta_whl`` config key).
  * ``CURRENT``  -- v0.1.0 and later (databricks_labs_sdp_meta dist,
                    ``from databricks.labs.sdp_meta.*`` runner imports,
                    ``sdp_meta_whl`` config key). The v0.1.0 main wheel
                    BUNDLES a legacy-namespace compat surface: a real
                    top-level ``src`` package (plus a ``dlt_meta``
                    package), both re-exporting
                    ``databricks.labs.sdp_meta.*``. So a LEGACY-source
                    customer who flips their ``dlt_meta_whl`` config from
                    a v0.0.10 wheel to a v0.1.0 wheel keeps working
                    without any other change -- their
                    ``from src.* import …`` resolves through the real
                    ``src`` package via normal import machinery.

Profile resolution is git-ref-prefix based. Pass ``--source_version`` and
``--target_version`` (any git tag, branch, or SHA the wheel builder can
check out). The orchestrator infers the profile for each ref; override
via ``--source_profile`` / ``--target_profile`` if a custom branch needs
explicit pinning.

Two-phase contract
------------------

Phase 1 (SOURCE wheel):
  1. Build SOURCE wheel from --source_version git ref.
  2. Onboard A1 (initial) using SOURCE-shape onboarding template.
  3. Run bronze A1 pipeline + silver pipeline.
  4. Onboard A2 (incremental).
  5. Run bronze A2 + silver again.
  6. Validate row counts and persist them for Phase 2.

[Local: build TARGET wheel, upload to the SAME UC volume, then
 ``pipelines.update()`` each pipeline's configuration to swap the value
 behind the SOURCE profile's pipeline-config key from the SOURCE wheel
 path to the TARGET wheel path. Pipeline IDs DO NOT change, so DLT
 checkpoints (which are pipeline-scoped) are preserved.]

Phase 2 (TARGET wheel):
  1. Drop a small new batch of customers + transactions JSON files
     into the original source paths (5 each).
  2. Re-run bronze pipeline. Auto Loader sees the new files via the
     existing checkpoint and ingests them on top of SOURCE's tables.
  3. Re-run silver pipeline.
  4. Validate: every Phase 1 row still present (compared to the
     persisted Phase 1 counts), bronze grew by >= 5 rows per source,
     silver did not regress, dataflowspec persisted by SOURCE reads
     cleanly through TARGET's dataclasses.

One config key, one wheel, one ``%pip install``
-----------------------------------------------
The runner notebook is the SOURCE profile's runner -- byte-for-byte the
file the customer would have on the source version. It reads exactly
ONE pipeline-config key (``dlt_meta_whl`` for LEGACY) and runs ONE
``%pip install $dlt_meta_whl`` line. For LEGACY -> CURRENT cross-
namespace upgrades the source runner's ``from src.* import …`` keeps
resolving because the v0.1.0 wheel bundles a real top-level ``src``
package: once ``%pip install`` lands the wheel, the runner's
``from src.dataflow_pipeline import …`` walks normal import machinery,
finds ``src/`` in site-packages, runs its ``__init__`` (which registers
the ``src.<sub>`` -> ``databricks.labs.sdp_meta.<sub>`` aliases), and
resolves. This does NOT depend on a ``.pth`` firing -- serverless
``%pip install`` does not re-trigger ``site.py``'s ``.pth`` scan, which
is exactly why resolution goes through a real package rather than a
startup hook.

Usage
-----

    # Default: v0.0.10 -> v0.1.0 (legacy -> current)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog>

    # Future: v0.1.0 -> v0.1.1 (current -> current)
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=v0.1.0 \\
        --target_version=v0.1.1

    # Custom branches with explicit profile pins
    python integration_tests/run_backward_compat_tests.py \\
        --uc_catalog_name=<catalog> \\
        --source_version=feature/legacy-bugfix --source_profile=legacy \\
        --target_version=feature/sdp-meta     --target_profile=current
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
import uuid
import warnings
import webbrowser
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional

# Add project root to Python path so ``databricks.labs.sdp_meta`` resolves
# (we only use it for the WorkspaceInstaller helper, not anything that's
# version-sensitive between phases).
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from databricks.sdk import WorkspaceClient  # noqa: E402
from databricks.sdk.service import compute, jobs  # noqa: E402
from databricks.sdk.service.catalog import SchemasAPI, VolumeInfo, VolumeType  # noqa: E402
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary  # noqa: E402
from databricks.sdk.service.workspace import ImportFormat, Language  # noqa: E402

from databricks.labs.sdp_meta.identifiers import validate_uc_identifier  # noqa: E402

from integration_tests.version_profiles import (  # noqa: E402
    DEFAULT_SOURCE_REF,
    DEFAULT_TARGET_REF,
    KNOWN_PROFILES,
    VersionProfile,
    is_cross_namespace_upgrade,
    resolve_profile,
)
from integration_tests.wheel_builder import GitRefWheelBuilder  # noqa: E402


@dataclass
class BCRunnerConf:
    """Per-run state for the backward-compat orchestrator.

    Mirrors ``SDPMetaRunnerConf`` from ``run_integration_tests.py`` but
    pared down to the surface this test actually uses (cloudfiles +
    bronze/silver), and adds the wheel-swap fields. Profile-driven --
    every version-line knob (distribution, runner notebook, onboarding
    template, pipeline-config key, compat shim) lives on
    ``source_profile`` / ``target_profile``, never on this dataclass.
    """

    run_id: str
    uc_catalog_name: str
    source_ref: str
    target_ref: str
    source_profile: VersionProfile
    target_profile: VersionProfile
    profile: Optional[str] = None  # databricks-cli profile (NOT the version profile)
    username: Optional[str] = None

    # Where wheels come from at install time.
    #
    #   "local"  -- build wheels from <source_ref> + <target_ref> with
    #               GitRefWheelBuilder, upload to UC volume, runners
    #               and wheel-tasks reference the UC volume paths
    #               (default; matches what real customers do --
    #               install a pre-built artifact).
    #   "git"    -- skip the local build entirely. Runners
    #               ``%pip install git+<git_repo_url>@<ref>`` directly
    #               and the Phase 1 onboarding wheel-task resolves
    #               the same git URL via JobEnvironment.dependencies.
    #               Faster local iteration; requires the cluster to
    #               have egress to ``git_repo_url``.
    install_mode: str = "local"
    git_repo_url: str = "https://github.com/databrickslabs/dlt-meta.git"
    # When True, skip the git-worktree checkout for the TARGET main
    # wheel and run ``setup.py bdist_wheel`` against the developer's
    # working tree instead. Use ONLY for iterating on uncommitted
    # target-side changes (bundled compat shim, post-rename CLI
    # aliases, etc.) before they're pushed to ``target_ref``. Source
    # ALWAYS comes from the pinned ref -- the source side is the
    # customer's already-released wheel and has nothing to do with
    # local edits.
    build_target_from_worktree: bool = False

    # Schema names (one per layer + one for dataflowspec). Per-run
    # suffix avoids collisions when two devs hit the same UC catalog.
    sdp_meta_schema: str = ""
    bronze_schema: str = ""
    silver_schema: str = ""

    # Volume + workspace paths.
    uc_volume_name: str = "dlt_meta_files"
    volume_info: Optional[VolumeInfo] = None
    uc_volume_path: str = ""
    runners_nb_path: str = ""
    int_tests_dir: str = "integration_tests"

    # Local + remote wheel paths.
    #
    # ``source_main_whl`` -- main wheel built from --source_version.
    # ``target_main_whl`` -- main wheel built from --target_version.
    #
    # No companion compat-shim wheel: the target wheel bundles its own
    # legacy-namespace compat surface (a real ``src`` package +
    # ``dlt_meta`` package) when needed, so a single wheel install is
    # always enough for both same-namespace and cross-namespace
    # upgrades.
    source_main_whl_local: str = ""
    target_main_whl_local: str = ""
    source_main_whl_remote: str = ""
    target_main_whl_remote: str = ""

    # Generated onboarding-file output paths (one per group) --
    # gitignored.
    a1_onboarding_file: str = ""
    a2_onboarding_file: str = ""

    # Pipeline IDs (created in Phase 1, reused in Phase 2).
    bronze_a1_pipeline_id: str = ""
    bronze_a2_pipeline_id: str = ""
    silver_pipeline_id: str = ""

    # Job IDs (one job per phase).
    phase1_job_id: Optional[int] = None
    phase2_job_id: Optional[int] = None

    # Phase output CSV paths inside the workspace + local copies.
    phase1_output_ws: str = ""
    phase2_output_ws: str = ""

    # Incremental seed shape. The two staging dirs we ship in the repo
    # contain exactly this many rows; the validator uses these as the
    # >= floor for bronze growth in Phase 2.
    phase2_customer_delta: int = 5
    phase2_transaction_delta: int = 5

    # Cached per-pipeline configuration dictionaries. Built once in
    # Phase 1 (under SOURCE) and again in Phase 2 (under TARGET).
    # Stored on the conf so logging + error messages can introspect.
    _phase1_pipeline_configs: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.sdp_meta_schema = self.sdp_meta_schema or f"sdp_meta_dataflowspecs_bc_{self.run_id}"
        self.bronze_schema = self.bronze_schema or f"sdp_meta_bronze_bc_{self.run_id}"
        self.silver_schema = self.silver_schema or f"sdp_meta_silver_bc_{self.run_id}"
        self.a1_onboarding_file = (
            f"integration_tests/conf/json/backward_compat_onboarding_{self.run_id}.json"
        )
        self.a2_onboarding_file = (
            f"integration_tests/conf/json/backward_compat_onboarding_A2_{self.run_id}.json"
        )

    @property
    def is_cross_namespace_upgrade(self) -> bool:
        """Whether SOURCE and TARGET use different runner-import namespaces.

        Used purely for logging and gate decisions; no separate compat
        shim wheel is built or installed -- the target wheel is
        responsible for bundling its own legacy-namespace compat
        surface when the namespace changes.
        """
        return is_cross_namespace_upgrade(
            source=self.source_profile, target=self.target_profile
        )


class BackwardCompatRunner:
    """Two-phase orchestrator: SOURCE -> TARGET wheel swap.

    Mirrors ``SDPMETARunner`` from ``run_integration_tests.py`` for the
    upload + workflow primitives, but adds the phase boundary and the
    pipeline reconfig step. Kept as a separate class so the existing
    test runner stays untouched.
    """

    # Per-phase wait ceiling for ``jobs.run_now().result(...)``. Phase 1
    # is the long pole: six pipeline runs in sequence (onboard A1 ->
    # bronze A1 -> silver -> onboard A2 -> bronze A2 -> silver) plus
    # the validate notebook task. Each serverless DLT pipeline run
    # incurs a cold-start charge (~1-2 min) on top of its actual
    # processing window. Override at runtime with --phase_timeout_min
    # if a slow workspace pushes Phase 1 past the wall.
    DEFAULT_PHASE_TIMEOUT_MIN = 30

    def __init__(self, args: dict, ws: WorkspaceClient) -> None:
        self.args = args
        self.ws = ws
        self.base_dir = "integration_tests"
        self.phase_timeout_min = int(
            args.get("phase_timeout_min") or self.DEFAULT_PHASE_TIMEOUT_MIN
        )

    # ----- helpers --------------------------------------------------------

    def _my_username(self) -> str:
        return self.ws.current_user.me().user_name

    def _build_runner_conf(self) -> BCRunnerConf:
        run_id = uuid.uuid4().hex[:12]
        username = self._my_username()

        source_ref = self.args.get("source_version") or DEFAULT_SOURCE_REF
        target_ref = self.args.get("target_version") or DEFAULT_TARGET_REF
        source_profile = resolve_profile(
            source_ref, profile_override=self.args.get("source_profile")
        )
        target_profile = resolve_profile(
            target_ref, profile_override=self.args.get("target_profile")
        )

        install_mode = (self.args.get("install_mode") or "local").lower()
        if install_mode not in ("local", "git"):
            raise ValueError(
                f"--install_mode must be 'local' or 'git'; got {install_mode!r}"
            )

        build_target_from_worktree = bool(
            self.args.get("build_target_from_worktree")
        )
        if build_target_from_worktree and install_mode != "local":
            raise ValueError(
                "--build_target_from_worktree only makes sense with "
                "--install_mode=local; got "
                f"install_mode={install_mode!r}."
            )

        conf = BCRunnerConf(
            run_id=run_id,
            uc_catalog_name=self.args["uc_catalog_name"],
            source_ref=source_ref,
            target_ref=target_ref,
            source_profile=source_profile,
            target_profile=target_profile,
            profile=self.args.get("profile"),
            username=username,
            install_mode=install_mode,
            git_repo_url=self.args.get("git_repo_url")
            or BCRunnerConf.__dataclass_fields__["git_repo_url"].default,
            build_target_from_worktree=build_target_from_worktree,
        )
        conf.runners_nb_path = (
            f"/Users/{username}/dlt_meta_int_tests/backward_compat/{run_id}"
        )
        conf.phase1_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase1.csv"
        )
        conf.phase2_output_ws = (
            f"{conf.runners_nb_path}/integration-test-output_phase2.csv"
        )

        if conf.is_cross_namespace_upgrade and not (
            target_ref.startswith("v0.0.")
            or target_ref.startswith("v0.1")
            or target_ref == "feature/sdp-meta"
        ):
            # Cross-namespace upgrades only work when the target wheel
            # bundles a legacy-namespace compat surface. v0.1.0 and later
            # CURRENT-profile builds do; arbitrary unrecognised refs
            # might not. Surface a warning so the user knows to verify
            # the target wheel actually ships the real ``src`` package
            # before running.
            warnings.warn(
                f"Cross-namespace upgrade requested ({source_profile.name!r} "
                f"-> {target_profile.name!r}) with target_ref={target_ref!r}. "
                "The target wheel MUST bundle a legacy-namespace compat "
                "surface (a real `src` package + `dlt_meta` package "
                "re-exporting `databricks.labs.sdp_meta.*`) for the source "
                "runner notebook's "
                "`from src.* import …` to keep resolving. Verify the "
                "wheel before running, or pin source/target profiles "
                "explicitly.",
                stacklevel=2,
            )

        return conf

    # ----- wheel build + upload ------------------------------------------

    def build_wheels(self, conf: BCRunnerConf) -> None:
        """Build SOURCE main + TARGET main wheels.

        Skipped entirely when ``install_mode == "git"`` -- in that case
        runners and wheel-tasks resolve git URLs at install time and
        nothing has to be uploaded.

        Only ONE wheel per side is built. For cross-namespace upgrades
        (LEGACY -> CURRENT) the target wheel BUNDLES a legacy-namespace
        compat surface (a real ``src`` package + ``dlt_meta`` package
        re-exporting ``databricks.labs.sdp_meta.*``, configured in the
        top-level ``setup.py``) so we don't need a separate companion
        wheel to deliver it.
        """
        if conf.install_mode == "git":
            print(
                f"=== install_mode=git: skipping local wheel build; "
                f"all installs will resolve from {conf.git_repo_url!r} ==="
            )
            return

        builder = GitRefWheelBuilder()
        try:
            # SOURCE wheel always comes from the pinned ref -- it
            # represents the customer's already-released wheel, which
            # has no relationship to local working-tree edits.
            print(
                f"=== Building SOURCE wheel ({conf.source_profile.name}) from "
                f"ref={conf.source_ref!r} ==="
            )
            conf.source_main_whl_local = str(builder.build(conf.source_ref))

            # TARGET wheel: optionally bypass git for unreleased
            # target-side changes still living in the developer's
            # working tree. See ``BCRunnerConf.build_target_from_worktree``.
            if conf.build_target_from_worktree:
                print(
                    f"=== Building TARGET wheel ({conf.target_profile.name}) from "
                    "LOCAL WORKING TREE (--build_target_from_worktree) ==="
                )
                conf.target_main_whl_local = str(builder.build_from_worktree())
            else:
                print(
                    f"=== Building TARGET wheel ({conf.target_profile.name}) from "
                    f"ref={conf.target_ref!r} ==="
                )
                conf.target_main_whl_local = str(builder.build(conf.target_ref))
        finally:
            builder.cleanup()

    def upload_wheel(self, conf: BCRunnerConf, local_path: str, remote_subdir: str) -> str:
        """Upload a single .whl to ``<volume>/wheels/<remote_subdir>/<name>``.

        Returns the absolute UC volume path of the uploaded wheel,
        suitable for ``%pip install`` from a Databricks notebook.
        """
        wheel_name = os.path.basename(local_path)
        remote = f"{conf.uc_volume_path}wheels/{remote_subdir}/{wheel_name}"
        with open(local_path, "rb") as fh:
            self.ws.files.upload(file_path=remote, contents=fh, overwrite=True)
        print(f"  uploaded -> {remote}")
        return remote

    # ----- install-spec resolver ----------------------------------------

    def _git_install_spec(
        self,
        conf: BCRunnerConf,
        ref: str,
        *,
        subdir: Optional[str] = None,
    ) -> str:
        """Build a pip-compatible ``git+<url>@<ref>[#subdirectory=...]`` URL.

        Both ``%pip install`` and ``JobEnvironment.dependencies`` accept
        this form (assuming the cluster has egress to the repo host).
        """
        spec = f"git+{conf.git_repo_url}@{ref}"
        if subdir:
            spec = f"{spec}#subdirectory={subdir}"
        return spec

    def install_spec_source_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the SOURCE main wheel."""
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.source_ref)
        return conf.source_main_whl_remote

    def install_spec_target_main(self, conf: BCRunnerConf) -> str:
        """Pip-installable spec for the TARGET main wheel."""
        if conf.install_mode == "git":
            return self._git_install_spec(conf, conf.target_ref)
        return conf.target_main_whl_remote

    # ----- UC + onboarding generation ------------------------------------

    def initialize_uc_resources(self, conf: BCRunnerConf) -> None:
        api = SchemasAPI(self.ws.api_client)
        for s, comment in (
            (conf.sdp_meta_schema, "sdp_meta dataflowspec schema (backward-compat run)"),
            (conf.bronze_schema, "bronze schema (backward-compat run)"),
            (conf.silver_schema, "silver schema (backward-compat run)"),
        ):
            api.create(catalog_name=conf.uc_catalog_name, name=s, comment=comment)
        vol = self.ws.volumes.create(
            catalog_name=conf.uc_catalog_name,
            schema_name=conf.sdp_meta_schema,
            name=conf.uc_volume_name,
            volume_type=VolumeType.MANAGED,
        )
        conf.volume_info = vol
        conf.uc_volume_path = (
            f"/Volumes/{vol.catalog_name}/{vol.schema_name}/{vol.name}/"
        )
        print(f"UC volume ready: {conf.uc_volume_path}")

    def generate_onboarding_files(self, conf: BCRunnerConf) -> None:
        """Render onboarding JSON from the SOURCE profile's templates.

        Each profile owns its onboarding shape (LEGACY templates strip
        v0.1.0-only fields like ``rowFilter``; CURRENT templates use
        the full v0.1.0+ shape). We render from the source profile's
        templates so Phase 1 onboards exactly what the customer would
        have onboarded on the source version.
        """
        subs = {
            "{uc_volume_path}": conf.uc_volume_path,
            "{uc_catalog_name}": conf.uc_catalog_name,
            "{bronze_schema}": conf.bronze_schema,
            "{silver_schema}": conf.silver_schema,
        }
        for tmpl, out in (
            (conf.source_profile.onboarding_a1_template, conf.a1_onboarding_file),
            (conf.source_profile.onboarding_a2_template, conf.a2_onboarding_file),
        ):
            with open(tmpl, "r") as fh:
                text = fh.read()
            for k, v in subs.items():
                text = text.replace(k, v or "")
            payload = json.loads(text)
            with open(out, "w") as fh:
                json.dump(payload, fh, indent=4)
            print(
                f"  rendered onboarding from {tmpl} ({conf.source_profile.name}) "
                f"-> {out}"
            )

    # ----- workspace upload ---------------------------------------------

    def upload_files(self, conf: BCRunnerConf) -> None:
        # Resources (data + ddl) -- the same trees the existing
        # cloudfiles integration test consumes, plus our two
        # ``customers_phase2/`` and ``transactions_phase2/`` staging
        # dirs.
        for root, _dirs, files in os.walk(f"{conf.int_tests_dir}/resources"):
            for fname in files:
                with open(os.path.join(root, fname), "rb") as content:
                    self.ws.files.upload(
                        file_path=f"{conf.uc_volume_path}{root}/{fname}",
                        contents=content,
                        overwrite=True,
                    )

        # Conf (silver transformations + DQE rules + the rendered
        # onboarding files we just generated). Only .json -- the
        # backward-compat test is JSON-only by design (YAML support
        # is a v0.1.0-only addition).
        for root, _dirs, files in os.walk(f"{conf.int_tests_dir}/conf/json"):
            for fname in files:
                if not fname.endswith(".json"):
                    continue
                with open(os.path.join(root, fname), "rb") as content:
                    self.ws.files.upload(
                        file_path=f"{conf.uc_volume_path}{root}/{fname}",
                        contents=content,
                        overwrite=True,
                    )

        # Runner notebooks (under <runners_nb_path>/runners/ -- mirrors
        # the existing test's contract).
        self.ws.workspace.mkdirs(f"{conf.runners_nb_path}/runners")
        local_runners = f"{conf.int_tests_dir}/notebooks/backward_compat_runners"
        for nb in os.listdir(local_runners):
            with open(os.path.join(local_runners, nb), "rb") as fh:
                self.ws.workspace.upload(
                    path=f"{conf.runners_nb_path}/runners/{nb}",
                    format=ImportFormat.SOURCE,
                    language=Language.PYTHON,
                    content=fh.read(),
                )

        if conf.install_mode == "git":
            print(
                "  install_mode=git: skipping wheel uploads "
                "(cluster will resolve git URLs at install time)."
            )
        else:
            # Wheels go under wheels/<source|target>/ so the same upload
            # routine works for arbitrary version pairs without
            # colliding if source and target build distinct wheels with
            # the same filename (e.g. main vs feature/sdp-meta both
            # producing
            # ``databricks_labs_sdp_meta-0.1.0-py3-none-any.whl``).
            conf.source_main_whl_remote = self.upload_wheel(
                conf, conf.source_main_whl_local, "source"
            )
            conf.target_main_whl_remote = self.upload_wheel(
                conf, conf.target_main_whl_local, "target"
            )

    # ----- pipeline create / update --------------------------------------

    def _runner_notebook_filename(self, conf: BCRunnerConf) -> str:
        """File name (basename) of the SOURCE profile's runner notebook.

        Used by both ``create_pipeline`` (sets the library path) and
        ``upload_files`` (decides which file to upload from the local
        ``backward_compat_runners/`` directory).
        """
        return os.path.basename(conf.source_profile.runner_notebook_local_path)

    def _build_phase1_pipeline_config(
        self,
        conf: BCRunnerConf,
        layer: str,
        group: str,
    ) -> Dict[str, str]:
        """Pipeline configuration for Phase 1 (SOURCE wheel).

        One key, one wheel: the SOURCE profile's ``pipeline_config_whl_key``
        maps to the SOURCE main wheel path. The SOURCE runner notebook
        reads that key and runs a single ``%pip install $key`` line.

        ``pipelines.maxFlowRetryAttempts=0`` makes DLT fail an update
        on the FIRST flow failure rather than retrying the failing
        flow up to its default attempts. For an integration test
        we want fast, deterministic failure -- a true bug doesn't
        get less true with three more attempts, and waiting through
        the retry budget doubles or triples the diagnose-fix-rerun
        cycle on broken specs.
        """
        return {
            "layer": layer,
            conf.source_profile.pipeline_config_whl_key: (
                self.install_spec_source_main(conf)
            ),
            f"{layer}.group": group,
            f"{layer}.dataflowspecTable": (
                f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}.{layer}_dataflowspec"
            ),
            "pipelines.externalSink.enabled": "true",
            "pipelines.maxFlowRetryAttempts": "0",
        }

    def _build_phase2_pipeline_config(
        self,
        conf: BCRunnerConf,
        layer: str,
        group: str,
    ) -> Dict[str, str]:
        """Pipeline configuration for Phase 2 (TARGET wheel).

        Same SOURCE-profile pipeline-config key as Phase 1 -- the
        runner notebook is the customer's source-version runner and
        only knows the key IT was wired with. We swap the VALUE behind
        that key from the SOURCE wheel path to the TARGET wheel path
        so the same notebook installs the new wheel via the same
        ``%pip install`` line. No second key, no second wheel.

        For LEGACY -> CURRENT cross-namespace upgrades, the TARGET
        wheel is responsible for bundling its own legacy-namespace
        compat surface (a real ``src`` package + ``dlt_meta`` package)
        so the SOURCE runner's ``from src.* import …`` keeps resolving
        through normal import machinery. The orchestrator does not
        deliver a separate companion wheel.
        """
        return {
            "layer": layer,
            conf.source_profile.pipeline_config_whl_key: (
                self.install_spec_target_main(conf)
            ),
            f"{layer}.group": group,
            f"{layer}.dataflowspecTable": (
                f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}.{layer}_dataflowspec"
            ),
            "pipelines.externalSink.enabled": "true",
            # See _build_phase1_pipeline_config for why we disable
            # flow-level retries in this test.
            "pipelines.maxFlowRetryAttempts": "0",
        }

    def create_pipeline(
        self,
        conf: BCRunnerConf,
        name: str,
        layer: str,
        group: str,
        target_schema: str,
    ) -> str:
        """Create a DLT pipeline pinned to the SOURCE wheel.

        Phase 2 swaps the wheel via ``swap_pipelines_to_target`` rather
        than recreating the pipeline; that preserves the pipeline ID
        AND the per-pipeline DLT checkpoints, which is the customer-
        upgrade scenario we're modelling.
        """
        configuration = self._build_phase1_pipeline_config(conf, layer, group)
        conf._phase1_pipeline_configs[name] = configuration
        runner_path = (
            f"{conf.runners_nb_path}/runners/{self._runner_notebook_filename(conf)}"
        )
        created = self.ws.pipelines.create(
            catalog=conf.uc_catalog_name,
            name=name,
            serverless=True,
            configuration=configuration,
            libraries=[
                PipelineLibrary(
                    notebook=NotebookLibrary(path=runner_path)
                )
            ],
            schema=target_schema,
        )
        if created is None or not created.pipeline_id:
            raise RuntimeError(f"Pipeline {name!r} creation failed")
        print(f"  created pipeline {name!r} -> {created.pipeline_id}")
        return created.pipeline_id

    def create_all_pipelines(self, conf: BCRunnerConf) -> None:
        conf.bronze_a1_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-bronze-A1-{conf.run_id}",
            "bronze",
            "A1",
            conf.bronze_schema,
        )
        conf.bronze_a2_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-bronze-A2-{conf.run_id}",
            "bronze",
            "A2",
            conf.bronze_schema,
        )
        conf.silver_pipeline_id = self.create_pipeline(
            conf,
            f"backward-compat-silver-{conf.run_id}",
            "silver",
            "A1",
            conf.silver_schema,
        )

    def swap_pipelines_to_target(self, conf: BCRunnerConf) -> None:
        """Update every pipeline's configuration to point at TARGET wheel.

        Pipeline IDs and DLT checkpoints are preserved; only the value
        behind the SOURCE-defined pipeline-config key changes (from
        SOURCE wheel path to TARGET wheel path). The runner notebook,
        the libraries list, the schema, and every other pipeline
        attribute stay byte-identical.

        Note on whl-typed PipelineLibrary entries: serverless DLT
        rejects them with ``InvalidParameterValue: Whl libraries are
        not supported``, so the legacy-namespace compat surface cannot
        be delivered as a separate pipeline library. Bundling it
        directly into the main wheel sidesteps that: the wheel ships a
        real top-level ``src`` package, so after ``%pip install`` the
        runner's ``from src.* import …`` resolves through normal import
        machinery -- no separate library, and no reliance on a ``.pth``
        startup hook (which serverless ``%pip install`` would not fire
        anyway).
        """
        targets = (
            (conf.bronze_a1_pipeline_id, "bronze", "A1", conf.bronze_schema),
            (conf.bronze_a2_pipeline_id, "bronze", "A2", conf.bronze_schema),
            (conf.silver_pipeline_id, "silver", "A1", conf.silver_schema),
        )
        for pid, layer, group, _target_schema in targets:
            config = self._build_phase2_pipeline_config(conf, layer, group)
            existing = self.ws.pipelines.get(pid)
            self.ws.pipelines.update(
                pipeline_id=pid,
                catalog=existing.spec.catalog,
                name=existing.spec.name,
                serverless=True,
                configuration=config,
                libraries=existing.spec.libraries,
                schema=existing.spec.schema,
            )
            print(
                f"  swapped pipeline {pid} -> "
                f"{conf.target_profile.name} wheel spec"
            )

    # ----- workflow building --------------------------------------------

    def build_phase1_job(self, conf: BCRunnerConf):
        """Phase 1 workflow: onboard A1 -> bronze A1 -> onboard A2 ->
        bronze A2 -> silver -> validate_phase1.

        Mirrors the existing cloudfiles A1+A2 happy path exactly
        (see ``IntegrationTestRunner.create_cloudfiles_workflow_spec``
        in ``run_integration_tests.py``), with EVERY pipeline pinned
        to the SOURCE wheel and the validation notebook set to the
        backward-compat ``validate_phase1.py`` (which persists
        per-table counts to a UC volume scratch file for Phase 2 to
        read back).

        Note: silver runs ONCE, AFTER bronze_A2. We do not run silver
        between bronze_A1 and bronze_A2 because the
        ``customers_silver_flow`` append flow reads from a
        ``customers_delta`` bronze table that is produced by the A2
        onboarding spec, not by A1. Inserting a silver step after A1
        would fail with ``TABLE_OR_VIEW_NOT_FOUND`` on
        ``customers_delta`` -- the canonical cloudfiles workflow has
        the same constraint.
        """
        # Onboarding wheel-tasks must resolve against the SOURCE wheel.
        # The wheel distribution name comes from the source profile
        # (e.g. LEGACY -> "dlt_meta", CURRENT -> "databricks_labs_sdp_meta").
        # The dependency itself is whatever ``install_spec_source_main``
        # returns -- a UC volume path in local mode, a git URL in git mode.
        env_key = "bc_phase1_env"
        environments = [
            jobs.JobEnvironment(
                environment_key=env_key,
                spec=compute.Environment(
                    client="1",
                    dependencies=[self.install_spec_source_main(conf)],
                ),
            )
        ]
        common_named = {
            "database": f"{conf.uc_catalog_name}.{conf.sdp_meta_schema}",
            "bronze_dataflowspec_table": "bronze_dataflowspec",
            "silver_dataflowspec_table": "silver_dataflowspec",
            "import_author": "backward_compat",
            "version": "v1",
            "env": "it",
            "uc_enabled": "True",
        }
        tasks = [
            jobs.Task(
                task_key="phase1_onboard_A1",
                description=(
                    f"Phase 1 ({conf.source_profile.name} / {conf.source_ref}): "
                    "onboard A1 (initial)"
                ),
                environment_key=env_key,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name=conf.source_profile.distribution,
                    entry_point="run",
                    named_parameters={
                        **common_named,
                        "onboard_layer": "bronze_silver",
                        "onboarding_file_path": (
                            f"{conf.uc_volume_path}{conf.a1_onboarding_file}"
                        ),
                        "overwrite": "True",
                    },
                ),
            ),
            jobs.Task(
                task_key="phase1_bronze_A1",
                depends_on=[jobs.TaskDependency(task_key="phase1_onboard_A1")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a1_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_onboard_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_bronze_A1")],
                description=(
                    f"Phase 1 ({conf.source_profile.name} / {conf.source_ref}): "
                    "onboard A2 (incremental)"
                ),
                environment_key=env_key,
                python_wheel_task=jobs.PythonWheelTask(
                    package_name=conf.source_profile.distribution,
                    entry_point="run",
                    named_parameters={
                        **common_named,
                        "onboard_layer": "bronze",
                        "onboarding_file_path": (
                            f"{conf.uc_volume_path}{conf.a2_onboarding_file}"
                        ),
                        "overwrite": "False",
                    },
                ),
            ),
            jobs.Task(
                task_key="phase1_bronze_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_onboard_A2")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a2_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_silver_after_A2",
                depends_on=[jobs.TaskDependency(task_key="phase1_bronze_A2")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.silver_pipeline_id),
            ),
            jobs.Task(
                task_key="phase1_validate",
                depends_on=[jobs.TaskDependency(task_key="phase1_silver_after_A2")],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/validate_phase1.py",
                    base_parameters={
                        "uc_catalog_name": conf.uc_catalog_name,
                        "bronze_schema": conf.bronze_schema,
                        "silver_schema": conf.silver_schema,
                        "uc_volume_path": conf.uc_volume_path,
                        "output_file_path": f"/Workspace{conf.phase1_output_ws}",
                        "run_id": conf.run_id,
                    },
                ),
            ),
        ]
        return self.ws.jobs.create(
            name=f"backward-compat-phase1-{conf.run_id}",
            environments=environments,
            tasks=tasks,
        )

    def build_phase2_job(self, conf: BCRunnerConf):
        """Phase 2 workflow: drop incremental seed -> bronze A1 -> silver ->
        validate_phase2.

        No onboarding. No A2 redo. The dataflowspec persisted by Phase 1
        IS the spec Phase 2 runs. The only thing that changed between
        phases is the wheel attached to each pipeline -- swapped via
        ``pipelines.update()`` BEFORE this job is built.
        """
        tasks = [
            jobs.Task(
                task_key="phase2_add_incremental",
                description=(
                    f"Phase 2 ({conf.target_profile.name} / {conf.target_ref}): "
                    "drop incremental seed"
                ),
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/add_phase2_incremental.py",
                    base_parameters={
                        "uc_volume_path": conf.uc_volume_path,
                        "int_tests_dir": conf.int_tests_dir,
                    },
                ),
            ),
            jobs.Task(
                task_key="phase2_bronze",
                depends_on=[jobs.TaskDependency(task_key="phase2_add_incremental")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.bronze_a1_pipeline_id),
            ),
            jobs.Task(
                task_key="phase2_silver",
                depends_on=[jobs.TaskDependency(task_key="phase2_bronze")],
                pipeline_task=jobs.PipelineTask(pipeline_id=conf.silver_pipeline_id),
            ),
            jobs.Task(
                task_key="phase2_validate",
                depends_on=[jobs.TaskDependency(task_key="phase2_silver")],
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{conf.runners_nb_path}/runners/validate_phase2.py",
                    base_parameters={
                        "uc_catalog_name": conf.uc_catalog_name,
                        "bronze_schema": conf.bronze_schema,
                        "silver_schema": conf.silver_schema,
                        "sdp_meta_schema": conf.sdp_meta_schema,
                        "uc_volume_path": conf.uc_volume_path,
                        "output_file_path": f"/Workspace{conf.phase2_output_ws}",
                        "run_id": conf.run_id,
                        "phase2_customer_delta": str(conf.phase2_customer_delta),
                        "phase2_transaction_delta": str(conf.phase2_transaction_delta),
                        # validate_phase2 is a regular notebook_task (not
                        # a DLT runner), so it must %pip install the
                        # TARGET wheel itself before doing
                        # ``from src.dataflow_spec import …``.
                        "target_main_whl": conf.target_main_whl_remote,
                    },
                ),
            ),
        ]
        return self.ws.jobs.create(
            name=f"backward-compat-phase2-{conf.run_id}",
            tasks=tasks,
        )

    # ----- launch + result download --------------------------------------

    def launch_job(self, job_id: int, label: str):
        print(
            f"=== Launching {label} job_id={job_id} "
            f"(timeout={self.phase_timeout_min}min) ==="
        )
        webbrowser.open(
            f"{self.ws.config.host}/jobs/{job_id}?o={self.ws.get_workspace_id()}"
        )
        run = self.ws.jobs.run_now(job_id=job_id).result(
            timeout=timedelta(minutes=self.phase_timeout_min)
        )
        print(f"=== {label} run finished ===")
        return run

    def download_phase_output(self, ws_path: str, local_name: str) -> None:
        try:
            payload = self.ws.workspace.download(ws_path)
            with open(local_name, "wb") as out:
                out.write(payload.read())
            print(f"  downloaded -> {local_name}")
        except Exception as exc:
            print(f"  warn: failed to download {ws_path}: {exc}")

    # ----- cleanup -------------------------------------------------------

    def cleanup(self, conf: BCRunnerConf) -> None:
        print("=== Cleaning up backward-compat run ===")
        for pid in (
            conf.bronze_a1_pipeline_id,
            conf.bronze_a2_pipeline_id,
            conf.silver_pipeline_id,
        ):
            if pid:
                try:
                    self.ws.pipelines.delete(pid)
                    print(f"  deleted pipeline {pid}")
                except Exception as exc:
                    print(f"  warn: pipeline {pid} delete failed: {exc}")
        for jid in (conf.phase1_job_id, conf.phase2_job_id):
            if jid:
                try:
                    self.ws.jobs.delete(jid)
                    print(f"  deleted job {jid}")
                except Exception as exc:
                    print(f"  warn: job {jid} delete failed: {exc}")
        if conf.uc_catalog_name:
            test_schemas = {conf.sdp_meta_schema, conf.bronze_schema, conf.silver_schema}
            for schema in self.ws.schemas.list(catalog_name=conf.uc_catalog_name):
                if schema.name in test_schemas:
                    print(f"  deleting schema {schema.full_name}")
                    for vol in self.ws.volumes.list(
                        catalog_name=conf.uc_catalog_name, schema_name=schema.name
                    ):
                        try:
                            self.ws.volumes.delete(vol.full_name)
                        except Exception as exc:
                            print(f"    warn: volume delete failed: {exc}")
                    for table in self.ws.tables.list(
                        catalog_name=conf.uc_catalog_name, schema_name=schema.name
                    ):
                        try:
                            self.ws.tables.delete(table.full_name)
                        except Exception as exc:
                            print(f"    warn: table delete failed: {exc}")
                    try:
                        self.ws.schemas.delete(schema.full_name)
                    except Exception as exc:
                        print(f"  warn: schema delete failed: {exc}")
        print("=== Cleanup complete ===")

    # ----- top-level orchestration ---------------------------------------

    def run(self, *, cleanup: bool = False) -> int:
        """Drive the full two-phase test.

        ``cleanup=True`` runs ``cleanup()`` in a ``finally`` block; with
        ``cleanup=False`` (the default) we leave UC state in place so a
        failed run is debuggable. Re-run cleanup later with
        ``integration_tests/cleanup_script.py`` or rerun this script
        with ``--cleanup``.
        """
        conf = self._build_runner_conf()
        worktree_note = (
            " [TARGET wheels: local working tree]"
            if conf.build_target_from_worktree
            else ""
        )
        print(
            f"=== Backward-compat run_id={conf.run_id} "
            f"source={conf.source_ref!r} ({conf.source_profile.name}) -> "
            f"target={conf.target_ref!r} ({conf.target_profile.name}) "
            f"install_mode={conf.install_mode!r}{worktree_note} ==="
        )
        if conf.is_cross_namespace_upgrade:
            print(
                f"  cross-namespace upgrade "
                f"({conf.source_profile.runner_imports_namespace!r} -> "
                f"{conf.target_profile.runner_imports_namespace!r}): "
                "the TARGET wheel is expected to bundle a legacy-namespace "
                "compat surface (a real `src` package + `dlt_meta` package "
                "re-exporting `databricks.labs.sdp_meta.*`) so the source "
                "runner notebook's `from src.* import …` keeps resolving "
                "via normal import machinery. Phase 2 installs ONE wheel, "
                "runs ONE %pip install."
            )
        rc = 0
        try:
            self.build_wheels(conf)
            self.initialize_uc_resources(conf)
            self.generate_onboarding_files(conf)
            self.upload_files(conf)
            self.create_all_pipelines(conf)

            phase1_label = (
                f"Phase 1 ({conf.source_profile.name} / {conf.source_ref})"
            )
            phase1_job = self.build_phase1_job(conf)
            conf.phase1_job_id = phase1_job.job_id
            self.launch_job(phase1_job.job_id, label=phase1_label)
            self.download_phase_output(
                conf.phase1_output_ws,
                f"backward_compat_phase1_{conf.run_id}.csv",
            )

            print(
                f"=== Swapping pipelines: {conf.source_profile.name} "
                f"({conf.source_ref}) -> {conf.target_profile.name} "
                f"({conf.target_ref}) ==="
            )
            self.swap_pipelines_to_target(conf)

            phase2_label = (
                f"Phase 2 ({conf.target_profile.name} / {conf.target_ref})"
            )
            phase2_job = self.build_phase2_job(conf)
            conf.phase2_job_id = phase2_job.job_id
            self.launch_job(phase2_job.job_id, label=phase2_label)
            self.download_phase_output(
                conf.phase2_output_ws,
                f"backward_compat_phase2_{conf.run_id}.csv",
            )
        except Exception:
            traceback.print_exc()
            rc = 1
        finally:
            if cleanup:
                self.cleanup(conf)
        return rc


def get_workspace_client(profile: Optional[str]) -> WorkspaceClient:
    if os.environ.get("DATABRICKS_APP_PORT"):
        return WorkspaceClient()
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient(
        host=input("Databricks Workspace URL: "), token=input("Token: ")
    )


def parse_cli() -> dict:
    profile_choices = sorted(KNOWN_PROFILES)
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--uc_catalog_name",
        required=True,
        help="Unity Catalog name to create per-run schemas/volumes under.",
    )
    p.add_argument(
        "--profile",
        default=None,
        help="databricks-cli profile name (defaults to interactive prompt).",
    )
    p.add_argument(
        "--source_version",
        default=DEFAULT_SOURCE_REF,
        help=(
            "Git ref of the SOURCE version to upgrade FROM "
            f"(default: {DEFAULT_SOURCE_REF}). Tag, branch, or SHA."
        ),
    )
    p.add_argument(
        "--target_version",
        default=DEFAULT_TARGET_REF,
        help=(
            "Git ref of the TARGET version to upgrade TO "
            f"(default: {DEFAULT_TARGET_REF}). Tag, branch, or SHA."
        ),
    )
    p.add_argument(
        "--source_profile",
        default=None,
        choices=profile_choices,
        help=(
            "Override profile resolution for --source_version. Useful "
            "when --source_version is a custom branch the registry "
            "doesn't recognize."
        ),
    )
    p.add_argument(
        "--target_profile",
        default=None,
        choices=profile_choices,
        help=(
            "Override profile resolution for --target_version. Useful "
            "when --target_version is a custom branch the registry "
            "doesn't recognize."
        ),
    )
    p.add_argument(
        "--install_mode",
        default="local",
        choices=["local", "git"],
        help=(
            "How wheels reach the cluster. "
            "'local' (default): build with GitRefWheelBuilder + upload "
            "to UC volume (matches what real customers do -- install a "
            "pre-built artifact, no GitHub egress required). "
            "'git': skip the local build; runners and wheel-tasks "
            "resolve via 'pip install git+<repo>@<ref>'. Faster local "
            "iteration; the cluster MUST have egress to --git_repo_url."
        ),
    )
    p.add_argument(
        "--git_repo_url",
        default=BCRunnerConf.__dataclass_fields__["git_repo_url"].default,
        help=(
            "Git repository URL used when --install_mode=git "
            "(default: %(default)s). Ignored when --install_mode=local."
        ),
    )
    p.add_argument(
        "--build_target_from_worktree",
        action="store_true",
        help=(
            "Build the TARGET main wheel from the developer's local "
            "working tree instead of from --target_version. The "
            "SOURCE wheel still comes from --source_version (as a "
            "released customer would have). Use ONLY for iterating on "
            "uncommitted target-side changes (bundled compat shim, "
            "post-rename CLI aliases, etc.) before they're pushed. "
            "Requires --install_mode=local; the produced wheel is "
            "uploaded to UC volume the same way ref-built wheels are."
        ),
    )
    p.add_argument(
        "--phase_timeout_min",
        type=int,
        default=BackwardCompatRunner.DEFAULT_PHASE_TIMEOUT_MIN,
        help=(
            "Per-phase wall-clock timeout in minutes for "
            "jobs.run_now().result() (default: %(default)s). Phase 1 "
            "runs six DLT pipelines sequentially plus a validate "
            "notebook; bump this if your workspace's serverless cold "
            "starts push the run past the default."
        ),
    )
    p.add_argument(
        "--cleanup",
        action="store_true",
        help=(
            "Delete pipelines/jobs/schemas/volumes after the run "
            "completes (success or fail)."
        ),
    )
    args = vars(p.parse_args())
    validate_uc_identifier(args["uc_catalog_name"], kind="--uc_catalog_name")
    return args


def main() -> int:
    args = parse_cli()
    ws = get_workspace_client(args.get("profile"))
    runner = BackwardCompatRunner(args, ws)
    return runner.run(cleanup=args["cleanup"])


if __name__ == "__main__":
    raise SystemExit(main())
