"""Version profiles for the backward-compatibility integration test.

Background
----------

The backward-compat test simulates a customer pipeline upgrade by:

  1. Building a wheel from the SOURCE git ref (the version the customer is
     running today).
  2. Onboarding + running their pipeline on that wheel (Phase 1).
  3. Building a wheel from the TARGET git ref (the version they want to
     upgrade to).
  4. Swapping the wheel attached to every pipeline to the TARGET wheel
     WITHOUT recreating the pipeline or editing the customer's runner
     notebook (Phase 2).
  5. Asserting data preservation, incremental growth, and dataclass
     compatibility.

Different version-lines of sdp-meta have different "shapes" -- distribution
name, runner-notebook import style, onboarding-spec fields, pipeline-config
keys, companion wheels, etc. A ``VersionProfile`` captures everything the
orchestrator needs to know about ONE version-line so the test stays
generic across upgrade pairs.

Today two profiles ship out of the box:

  * ``LEGACY``   -- v0.0.1 through v0.0.10. Dist name ``dlt_meta``, runner
                    imports ``from src.*``, pipeline config key
                    ``dlt_meta_whl``. One key, one ``%pip install``,
                    byte-for-byte v0.0.10 customer notebook.
  * ``CURRENT``  -- v0.1.0 and later (and the ``feature/sdp-meta`` branch
                    that becomes v0.1.0 at release). Dist name
                    ``databricks_labs_sdp_meta``, runner imports
                    ``from databricks.labs.sdp_meta.*``, pipeline config
                    key ``sdp_meta_whl``. The v0.1.0 main wheel BUNDLES
                    a legacy-namespace compat surface (a real ``src``
                    package + ``dlt_meta`` package re-exporting
                    ``databricks.labs.sdp_meta.*``), so a LEGACY-source
                    customer who flips their ``dlt_meta_whl`` config from
                    a v0.0.10 wheel to a v0.1.0 wheel keeps working
                    without changing any other line in their pipeline
                    config or runner notebook. Bundling avoids two-wheel/
                    two-install contracts that don't compose reliably on
                    serverless DLT.

When a future v0.2.0 changes the contract again, register a new
``VersionProfile`` and add its ref-prefix to ``KNOWN_PROFILES``. Unknown
refs (e.g. arbitrary feature branches or SHAs) fall back to ``CURRENT``
and emit a warning -- callers can override the resolver via the
``--source_profile`` / ``--target_profile`` CLI flags.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass(frozen=True)
class VersionProfile:
    """Per-version-line knobs the orchestrator uses to build/install/run.

    Each field encodes ONE thing that varies across version-lines:

    Attributes:
      name: Short, stable identifier used in CLI flags and logs
        (e.g. ``"legacy"``, ``"current"``).
      description: Human-readable summary surfaced in ``--help`` and
        startup logs.
      ref_prefixes: Tuple of git-ref prefixes this profile owns
        (e.g. ``("v0.0.1", "v0.0.2", ..., "v0.0.10", "main")`` for
        LEGACY). Matched as prefixes so a SHA on a release branch still
        resolves correctly.
      distribution: pip distribution name produced by this version's
        ``setup.py`` (e.g. ``"dlt_meta"`` or ``"databricks_labs_sdp_meta"``).
        Used by the Phase 1 ``PythonWheelTask`` to address the onboarding
        entry point.
      pipeline_config_whl_key: The DLT-pipeline ``configuration`` key the
        runner notebook reads to ``%pip install`` the wheel
        (e.g. ``"dlt_meta_whl"`` or ``"sdp_meta_whl"``). The key is
        SOURCE-driven: in Phase 2 the orchestrator must use the SAME key
        so the customer's runner notebook keeps working.
      runner_notebook_local_path: Path under the repo root to the
        runner-notebook source file uploaded into the pipeline. Each
        profile ships its own runner so the import style + config
        key match.
      onboarding_a1_template: Path to the A1 (initial) onboarding
        template. Each profile uses its own shape so v0.1.0-only fields
        (``rowFilter`` etc.) only appear when the source is CURRENT.
      onboarding_a2_template: Path to the A2 (incremental) onboarding
        template. Same shape constraints as A1.
      runner_imports_namespace: The Python namespace this profile's
        runner notebook imports from (e.g. ``"src"`` for LEGACY,
        ``"databricks.labs.sdp_meta"`` for CURRENT). The orchestrator
        only uses this to detect cross-namespace upgrades, which is
        worth knowing even though the legacy-namespace compat is now
        bundled into the target wheel itself (see CURRENT below) --
        cross-namespace flag still drives logging and gate decisions.

    Note on legacy-namespace compatibility for cross-namespace upgrades
    (LEGACY -> CURRENT):
        Earlier iterations of this test built a SEPARATE compat-shim
        wheel from ``compat/`` and installed it as a second wheel
        under a second pipeline-config key. That approach hit two
        platform constraints:

        1. Serverless DLT rejects ``PipelineLibrary(whl=...)``
           (``InvalidParameterValue: Whl libraries are not supported``)
           so we couldn't deliver the shim via DLT's library lifecycle.
        2. ``%pip install $a $b`` with two notebook-config substitutions
           composed in one line worked in static checks but did not
           reliably re-process the shim's ``.pth`` after install in
           serverless DLT, so ``src.*`` aliases were missing on
           interpreter startup and the runner notebook's
           ``from src.dataflow_pipeline import …`` raised
           ``ModuleNotFoundError``.

        The current design BUNDLES a real top-level ``src`` package
        (plus a ``dlt_meta`` package) directly into the v0.1.0 main
        wheel (see top-level ``setup.py``). One ``%pip install`` of one
        wheel is enough; the runner's ``from src.dataflow_pipeline
        import …`` then resolves through normal import machinery --
        Python finds ``src/`` in site-packages, runs its ``__init__``
        (which registers the ``src.<sub>`` alias map), and resolves the
        symbol. This deliberately does NOT depend on a ``.pth`` firing
        at startup (constraint #2 above is exactly why), so it is robust
        on serverless DLT where the ``.pth`` scan never re-runs. There
        is no second key, no second wheel, no second install.
    """

    name: str
    description: str
    ref_prefixes: Tuple[str, ...]
    distribution: str
    pipeline_config_whl_key: str
    runner_notebook_local_path: str
    onboarding_a1_template: str
    onboarding_a2_template: str
    runner_imports_namespace: str
    # Free-form extra package names this profile must install alongside
    # its main wheel during Phase 1 (rare; reserved for future profiles
    # that may need an unconditional helper package).
    extra_install_subdirs: Tuple[str, ...] = field(default_factory=tuple)


LEGACY = VersionProfile(
    name="legacy",
    description="v0.0.1 through v0.0.10 (dlt_meta distribution, src.* imports)",
    ref_prefixes=(
        "v0.0.1",
        "v0.0.2",
        "v0.0.3",
        "v0.0.4",
        "v0.0.5",
        "v0.0.6",
        "v0.0.7",
        "v0.0.8",
        "v0.0.9",
        "v0.0.10",
        "main",  # main historically tracked the LEGACY shape pre-v0.1.0.
    ),
    distribution="dlt_meta",
    pipeline_config_whl_key="dlt_meta_whl",
    runner_notebook_local_path=(
        "integration_tests/notebooks/backward_compat_runners/init_dlt_meta_pipeline.py"
    ),
    onboarding_a1_template=(
        "integration_tests/conf/json/backward_compat-cloudfiles-onboarding.template"
    ),
    onboarding_a2_template=(
        "integration_tests/conf/json/backward_compat-cloudfiles-onboarding_A2.template"
    ),
    runner_imports_namespace="src",
)


CURRENT = VersionProfile(
    name="current",
    description=(
        "v0.1.0 and later (databricks-labs-sdp-meta distribution, "
        "databricks.labs.sdp_meta imports)"
    ),
    ref_prefixes=(
        "v0.1",     # matches v0.1.0, v0.1.1, ... v0.1.x
        "v0.2",     # placeholder for the next minor release; treat any
                    # later v0.2.x tag as CURRENT until a profile says
                    # otherwise. (When v0.2.0 changes the contract,
                    # register a new VersionProfile and remove this.)
        "feature/sdp-meta",
    ),
    distribution="databricks_labs_sdp_meta",
    pipeline_config_whl_key="sdp_meta_whl",
    runner_notebook_local_path=(
        "integration_tests/notebooks/backward_compat_runners/init_sdp_meta_pipeline.py"
    ),
    onboarding_a1_template="integration_tests/conf/json/cloudfiles-onboarding.template",
    onboarding_a2_template="integration_tests/conf/json/cloudfiles-onboarding_A2.template",
    runner_imports_namespace="databricks.labs.sdp_meta",
)


KNOWN_PROFILES = {
    LEGACY.name: LEGACY,
    CURRENT.name: CURRENT,
}


# Public default refs. Surfaced separately so the orchestrator's
# argparse defaults stay readable.
DEFAULT_SOURCE_REF = "v0.0.10"
DEFAULT_TARGET_REF = "feature/sdp-meta"


def resolve_profile(ref: str, *, profile_override: Optional[str] = None) -> VersionProfile:
    """Resolve a git ref to a :class:`VersionProfile`.

    Resolution order:
      1. If ``profile_override`` is supplied, look it up by name in
         :data:`KNOWN_PROFILES` and return that profile (raises ValueError
         if unknown). This is the explicit-override path used by the
         CLI's ``--source_profile`` / ``--target_profile`` flags.
      2. Otherwise, find the first profile whose ``ref_prefixes``
         contains a prefix that ``ref`` starts with (longest first, so
         ``"v0.0.10"`` wins over ``"v0.0.1"``).
      3. If no profile matches, return :data:`CURRENT` and log a
         warning. Custom branches and SHAs typically reflect work on
         top of the latest contract, so CURRENT is the safer default
         than LEGACY.

    Args:
      ref: A git ref the wheel builder will check out (tag, branch,
        or SHA).
      profile_override: Optional explicit profile name to bypass
        prefix-based resolution.

    Returns:
      The matched :class:`VersionProfile`.

    Raises:
      ValueError: If ``profile_override`` is supplied but unknown.
    """
    if profile_override is not None:
        try:
            return KNOWN_PROFILES[profile_override]
        except KeyError as exc:
            valid = ", ".join(sorted(KNOWN_PROFILES))
            raise ValueError(
                f"Unknown profile name: {profile_override!r}. "
                f"Valid options: {valid}."
            ) from exc

    candidates = []
    for profile in KNOWN_PROFILES.values():
        for prefix in profile.ref_prefixes:
            if ref == prefix or ref.startswith(f"{prefix}."):
                # Tag-style match: exact or dotted-suffix
                # (so ``v0.0.10`` matches itself and ``v0.0.10.post1``).
                candidates.append((len(prefix), profile))
                break
            if ref == prefix or ref.startswith(f"{prefix}/"):
                # Branch-style match (``feature/sdp-meta`` etc.).
                candidates.append((len(prefix), profile))
                break
    if candidates:
        # Longest prefix wins so ``v0.0.10`` resolves to LEGACY even
        # though both ``v0.0.1`` and ``v0.0.10`` are listed.
        candidates.sort(key=lambda c: c[0], reverse=True)
        return candidates[0][1]

    print(
        f"warn: ref={ref!r} did not match any registered version "
        f"profile; defaulting to {CURRENT.name!r}. Override with "
        "--source_profile / --target_profile if this is wrong."
    )
    return CURRENT


def is_cross_namespace_upgrade(
    *, source: VersionProfile, target: VersionProfile
) -> bool:
    """Return True if source and target use different runner-import namespaces.

    Cross-namespace upgrades (e.g. LEGACY's ``src.*`` -> CURRENT's
    ``databricks.labs.sdp_meta.*``) rely on the target wheel to bundle
    a legacy-namespace compat shim so the source's runner notebook
    keeps resolving its imports unchanged. There is no separate compat
    shim wheel to build or install: the target wheel handles its own
    compatibility surface.

    The orchestrator uses this flag for logging and for guarding against
    obviously-wrong upgrade pairs (e.g. CURRENT -> LEGACY downgrade,
    where the LEGACY wheel cannot satisfy a CURRENT runner's imports).
    """
    return source.runner_imports_namespace != target.runner_imports_namespace
