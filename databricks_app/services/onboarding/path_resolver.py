"""Resolve an onboarding-spec path to a readable local file.

Three input shapes are accepted by every onboarding entrypoint:

  1. ``/Volumes/...``        \u2014 UC Volume path. Downloaded via SDK ``files.download``
                              to ``/tmp/sdp_onboarding_*`` and the tempfile path
                              is returned along with a cleanup handle.
  2. ``dbfs:/...`` / ``/dbfs/...`` \u2014 DBFS path. Downloaded via SDK ``dbfs.read``.
  3. Anything else            \u2014 Repo-relative or absolute local path.

Plus a post-resolution syntax check (``_preflight_parse_onboarding``)
that fails fast at the App boundary before any UC side-effects fire.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile

logger = logging.getLogger(__name__)


# Extensions the CLI's ``update_ws_onboarding_paths`` understands.
# ``.template`` is the historical JSON template suffix; ``.json`` /
# ``.yml`` / ``.yaml`` match every onboarding file shipped in ``demo/``.
_VALID_ONBOARDING_EXTS = (".json", ".yml", ".yaml", ".template")


# ── Trusted server-generated paths (S-2 allow-list) ─────────────────
# The S-2 traversal guard below rejects any absolute path that resolves
# outside the repo root. That's correct for caller-supplied paths
# (user typed an absolute path into the form), but the App also has
# server-generated paths that legitimately live in ``<tempdir>`` and
# are NEVER caller-controlled \u2014 most notably the merged bundled-demo
# specs produced by ``services.onboarding.bundled_specs`` when a demo
# declares ``merge_with``. Without this allow-list, the user picks
# "Cloud Files Autoloader (YAML)" from the bundled-demo dropdown,
# the form gets pre-filled with ``<tempdir>/sdp_meta_app_bundled_merged/
# cloudfiles-onboarding.merged.template.yml``, and the next ``Preview``
# click 400s with "Onboarding path escapes the repo root" \u2014 a true
# false-positive of the security check.
#
# Trust criteria (must be ALL of):
#   - Path is materialised by App code, never by the browser.
#   - Path lives under a fixed prefix the App owns end-to-end.
#   - Prefix is computed at module import via ``tempfile.gettempdir()``
#     so the writer and the verifier agree on the value by construction
#     (single source of truth \u2014 ``bundled_specs.py`` imports
#     ``BUNDLED_SPEC_MERGED_DIR`` from this module).
#
# To add a new trusted source: append it to ``_TRUSTED_GENERATED_PREFIXES``
# AND document the writer here. Anything not on this list still goes
# through the repo-root boundary check.

BUNDLED_SPEC_MERGED_DIR = os.path.join(
    tempfile.gettempdir(), "sdp_meta_app_bundled_merged"
)

_TRUSTED_GENERATED_PREFIXES = (
    BUNDLED_SPEC_MERGED_DIR,
)


def _is_under_trusted_prefix(local_real: str) -> bool:
    """``True`` if ``local_real`` (already ``realpath``'d) resolves
    under any prefix in ``_TRUSTED_GENERATED_PREFIXES``.

    Mirrors the boundary-check semantics used for the repo root:
    accepts both the exact prefix and any subpath, with the trailing
    ``os.sep`` guard so ``/tmp/sdp_meta_app_bundled_merged_evil/x.yml``
    is correctly rejected even when the trusted prefix is
    ``/tmp/sdp_meta_app_bundled_merged``.
    """
    for raw_prefix in _TRUSTED_GENERATED_PREFIXES:
        prefix_real = os.path.realpath(raw_prefix)
        if (
            local_real == prefix_real
            or local_real.startswith(prefix_real + os.sep)
        ):
            return True
    return False


class _OnboardingFileError(ValueError):
    """Raised by ``_resolve_local_onboarding_path`` / ``_preflight_parse_onboarding``
    with a user-facing message. Callers translate this to a 400 response."""


def _resolve_local_onboarding_path(raw_path: str, repo_root: str):
    """Materialise the onboarding file on local disk and return
    ``(local_path, tempfile_to_cleanup_or_None)``.

    The returned tempfile (if any) is the caller's responsibility to
    ``os.unlink`` once they're done \u2014 typically in a ``finally`` block.
    """
    if not raw_path:
        raise _OnboardingFileError("Onboarding file path is required.")

    if raw_path.startswith(("/Volumes/", "dbfs:/", "/dbfs/")):
        import tempfile
        try:
            from databricks.sdk import WorkspaceClient
            ws = WorkspaceClient()
            ext = os.path.splitext(raw_path)[1] or ".json"
            tmp = tempfile.NamedTemporaryFile(
                delete=False, suffix=ext, dir="/tmp", prefix="sdp_onboarding_"
            )
            try:
                if raw_path.startswith("/Volumes/"):
                    resp = ws.files.download(raw_path)
                    tmp.write(resp.contents.read())
                else:
                    dbfs_path = raw_path
                    if dbfs_path.startswith("/dbfs/"):
                        dbfs_path = "dbfs:/" + dbfs_path[len("/dbfs/"):]
                    data = ws.dbfs.read(path=dbfs_path)
                    import base64
                    tmp.write(base64.b64decode(data.data))
            finally:
                tmp.close()
            logger.info("Downloaded remote onboarding file to %s", tmp.name)
            return tmp.name, tmp.name
        except Exception as exc:
            logger.exception("Failed to download onboarding file from %s", raw_path)
            raise _OnboardingFileError(
                f"Could not download onboarding file from {raw_path}: {exc}"
            ) from exc

    # Local / relative path \u2014 anchor at repo root and verify the
    # resolved path stays inside it.
    #
    # The security hole this guards against: without canonicalisation,
    # a caller-supplied ``../../etc/passwd`` (relative) or
    # ``/etc/passwd`` (absolute) would resolve to an arbitrary file on
    # the App container's filesystem, and the existing ``isfile()``
    # gate only confirms existence \u2014 the contents are then returned
    # verbatim to the browser via the ``rendered`` field on the
    # preview endpoint.
    #
    # We accept BOTH absolute and relative inputs and apply the same
    # boundary check to both: ``os.path.realpath`` collapses any ``..``
    # / symlink hops, and we then assert the final path is within
    # ``repo_root_real``. The trailing ``os.sep`` is required so
    # ``/repo_root_evil`` is rejected even when ``repo_root`` is
    # ``/repo_root`` \u2014 plain ``startswith`` is a footgun otherwise.
    if os.path.isabs(raw_path):
        candidate = raw_path
    else:
        candidate = os.path.join(repo_root, raw_path)
    local_path = os.path.realpath(candidate)
    repo_root_real = os.path.realpath(repo_root)
    inside_repo = (
        local_path == repo_root_real
        or local_path.startswith(repo_root_real + os.sep)
    )
    # The second branch (``_is_under_trusted_prefix``) is the
    # S-2 allow-list for server-generated paths \u2014 see the prologue
    # comment on ``_TRUSTED_GENERATED_PREFIXES`` above. Most callers
    # land in ``inside_repo``; the trusted branch exists specifically
    # so the bundled-demo merge flow doesn't get caught by its own
    # security check.
    if not (inside_repo or _is_under_trusted_prefix(local_path)):
        raise _OnboardingFileError(
            f"Onboarding path escapes the repo root: {raw_path}\n"
            f"Provide a path that resolves inside {repo_root_real!r}, "
            f"or a UC Volume path (/Volumes/catalog/schema/volume/file.yml)."
        )
    if not os.path.isfile(local_path):
        raise _OnboardingFileError(
            f"Onboarding file not found: {local_path}\n"
            f"Provide a path relative to the repo root "
            f"(e.g. demo/conf/yml/onboarding.template.yml), "
            f"or a UC Volume path (/Volumes/catalog/schema/volume/file.yml)."
        )
    return local_path, None


def _preflight_parse_onboarding(local_path: str):
    """Read ``local_path``, parse it as YAML or JSON (by extension), and
    return ``(parsed_obj, source_ext_lower)``.

    Catches the common preventable failures *before* the CLI subprocess
    fires and *before* any UC side-effects: malformed YAML / JSON, empty
    files, top-level scalars, unreadable bytes. The CLI does the same
    parse later as part of the substitution round-trip, so this is purely
    a fast-fail at the App boundary \u2014 no semantic checking of dataflow-spec
    fields (that's the cluster's job).
    """
    src_ext = os.path.splitext(local_path)[1].lower()
    if src_ext not in _VALID_ONBOARDING_EXTS:
        # Not fatal \u2014 ``update_ws_onboarding_paths`` falls back to JSON
        # for anything non-YAML \u2014 but the user almost certainly made a typo.
        logger.warning(
            "Onboarding file extension %r is not in the expected set %s; "
            "parsing as JSON.", src_ext, _VALID_ONBOARDING_EXTS,
        )

    try:
        with open(local_path, "r", encoding="utf-8") as fh:
            raw = fh.read()
    except OSError as exc:
        raise _OnboardingFileError(f"Could not read {local_path}: {exc}") from exc

    try:
        if src_ext in (".yml", ".yaml"):
            import yaml
            parsed = yaml.safe_load(raw)
        else:
            parsed = json.loads(raw)
    except Exception as exc:
        # Catch both yaml.YAMLError and json.JSONDecodeError under one
        # name without importing yaml unconditionally at module scope.
        raise _OnboardingFileError(
            f"Could not parse {local_path}: {exc}"
        ) from exc

    if parsed is None or parsed == "":
        raise _OnboardingFileError(f"Onboarding file is empty: {local_path}")
    if not isinstance(parsed, (list, dict)):
        raise _OnboardingFileError(
            f"Onboarding file must be a list or object at the top level, "
            f"got {type(parsed).__name__}: {local_path}"
        )
    return parsed, src_ext
