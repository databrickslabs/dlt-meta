"""Tests for ``databricks_app/services/onboarding/path_resolver.py``.

Pin the path-traversal hardening (S-2). The local-path branch:

  * rejects ``..`` traversal that escapes the repo root
  * rejects absolute paths pointing outside the repo
  * still accepts legitimate repo-relative paths to existing files
  * still accepts absolute paths that resolve INSIDE the repo
    (so the existing onboarding flow that hands the resolver a
    pre-joined absolute path keeps working)

The UC Volume / DBFS branch is exercised indirectly via the prefix
check; we don't network-mock the SDK here.
"""

from __future__ import annotations

import os
import sys
import tempfile
import unittest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_APP_DIR = os.path.join(_REPO_ROOT, "databricks_app")
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

from services.onboarding.path_resolver import (  # noqa: E402
    BUNDLED_SPEC_MERGED_DIR,
    _OnboardingFileError,
    _resolve_local_onboarding_path,
)


class ResolveLocalOnboardingPathTraversalTests(unittest.TestCase):
    """``..`` and absolute paths must not be able to read outside the
    repo root via the App's onboarding entrypoints.
    """

    def setUp(self):
        # Build a self-contained fake repo so the test doesn't depend
        # on the layout of the real repo (and so we can scope the
        # boundary check to a known root).
        self._repo = tempfile.mkdtemp(prefix="sdp_meta_test_repo_")
        self._outside = tempfile.mkdtemp(prefix="sdp_meta_test_outside_")
        os.makedirs(os.path.join(self._repo, "demo", "conf", "yml"))
        legit = os.path.join(self._repo, "demo", "conf", "yml", "onboarding.yml")
        with open(legit, "w") as f:
            f.write("name: legit\n")
        evil = os.path.join(self._outside, "evil.yml")
        with open(evil, "w") as f:
            f.write("name: evil\n")
        self._legit_relpath = "demo/conf/yml/onboarding.yml"
        self._evil_abspath = evil

    def tearDown(self):
        for root in (self._repo, self._outside):
            for dirpath, _, fnames in os.walk(root, topdown=False):
                for fn in fnames:
                    try:
                        os.unlink(os.path.join(dirpath, fn))
                    except OSError:
                        pass
                try:
                    os.rmdir(dirpath)
                except OSError:
                    pass

    def test_repo_relative_path_resolves(self):
        local, tmp = _resolve_local_onboarding_path(
            self._legit_relpath, self._repo
        )
        self.assertTrue(local.endswith("onboarding.yml"))
        self.assertIsNone(tmp)

    def test_parent_traversal_to_sibling_dir_rejected(self):
        """``demo/../../<outside>`` must be flagged."""
        evil_rel = os.path.relpath(self._evil_abspath, self._repo)
        # Make sure the input is actually a ``..``-prefixed escape; if
        # tempfile happened to put both dirs in the same parent the
        # relpath would just be ``../outside/evil.yml`` \u2014 still an
        # escape but worth asserting we hit it.
        self.assertTrue(evil_rel.startswith(".."))
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path(evil_rel, self._repo)

    def test_explicit_double_dot_traversal_rejected(self):
        """A hand-crafted traversal payload \u2014 the classic exploit
        shape from the review."""
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path(
                "../../etc/passwd", self._repo
            )

    def test_absolute_path_outside_repo_rejected(self):
        """An absolute path that resolves outside the repo \u2014 the
        original S-2 exploit shape \u2014 must be blocked by the
        realpath boundary check."""
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path("/etc/passwd", self._repo)

    def test_absolute_path_outside_repo_with_traversal_rejected(self):
        """``/etc/../etc/passwd`` style payloads must canonicalise to
        ``/etc/passwd`` and be rejected on the boundary check, not
        accidentally resolve through to a successful read."""
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path(
                "/etc/../etc/passwd", self._repo
            )

    def test_absolute_path_inside_repo_accepted(self):
        """Absolute paths that legitimately land inside the repo are
        accepted \u2014 some upstream callers pre-join the repo root and
        pass an absolute path through. The realpath check guarantees
        this is still safe."""
        abs_legit = os.path.join(
            self._repo, "demo", "conf", "yml", "onboarding.yml"
        )
        local, tmp = _resolve_local_onboarding_path(abs_legit, self._repo)
        # realpath() may collapse symlinks in the tempdir prefix on
        # macOS (/tmp \u2192 /private/tmp), so compare via realpath.
        self.assertEqual(local, os.path.realpath(abs_legit))
        self.assertIsNone(tmp)

    def test_missing_repo_relative_file_gives_not_found(self):
        """An in-repo path that simply doesn't exist must produce the
        existing ``not found`` error, NOT the traversal error \u2014 the
        user did the right thing, the file just isn't there."""
        with self.assertRaisesRegex(
            _OnboardingFileError, r"Onboarding file not found"
        ):
            _resolve_local_onboarding_path(
                "demo/missing.yml", self._repo
            )

    def test_uc_volume_branch_unaffected(self):
        """The UC Volume prefix takes a different branch \u2014 the
        traversal check applies only to local paths. Without an SDK
        mock we just confirm the prefix is recognised and we land on
        the download path, surfaced as a download error message."""
        with self.assertRaisesRegex(
            _OnboardingFileError, r"Could not download"
        ):
            _resolve_local_onboarding_path(
                "/Volumes/c/s/v/foo.yml", self._repo
            )

    def test_empty_path_rejected(self):
        with self.assertRaisesRegex(_OnboardingFileError, r"required"):
            _resolve_local_onboarding_path("", self._repo)


class ResolveLocalOnboardingPathTrustedPrefixTests(unittest.TestCase):
    """The S-2 boundary check rejects absolute paths outside the repo
    root \u2014 except for an explicit allow-list of server-generated paths
    (``_TRUSTED_GENERATED_PREFIXES``). This pins the allow-list contract:

      * a file under ``BUNDLED_SPEC_MERGED_DIR`` (the merged-bundled-demo
        output dir, server-materialised, never browser-supplied) is
        accepted even though it lives outside the repo
      * a sibling temp dir with a confusingly-similar prefix is NOT
        accepted (defends against ``..._evil`` startswith bypass)
      * a generic ``<tempdir>/random.yml`` is still rejected, so the
        allow-list is narrow and explicit
    """

    def setUp(self):
        self._repo = tempfile.mkdtemp(prefix="sdp_meta_test_repo_")
        os.makedirs(BUNDLED_SPEC_MERGED_DIR, exist_ok=True)

    def tearDown(self):
        for dirpath, _, fnames in os.walk(self._repo, topdown=False):
            for fn in fnames:
                try:
                    os.unlink(os.path.join(dirpath, fn))
                except OSError:
                    pass
            try:
                os.rmdir(dirpath)
            except OSError:
                pass

    def _write_trusted(self, name: str, body: str = "- a: 1\n") -> str:
        path = os.path.join(BUNDLED_SPEC_MERGED_DIR, name)
        with open(path, "w") as f:
            f.write(body)
        self.addCleanup(lambda p=path: os.path.exists(p) and os.unlink(p))
        return path

    def test_file_under_trusted_merged_dir_accepted(self):
        # Reproduces the production failure: bundled-demo picker pre-fills
        # ``<tempdir>/sdp_meta_app_bundled_merged/cloudfiles-onboarding.
        # merged.template.yml`` and Preview must accept it.
        trusted_file = self._write_trusted(
            "cloudfiles-onboarding.merged.template.yml"
        )
        local, tmp = _resolve_local_onboarding_path(trusted_file, self._repo)
        self.assertEqual(local, os.path.realpath(trusted_file))
        self.assertIsNone(tmp)

    def test_sibling_dir_with_similar_prefix_still_rejected(self):
        # ``<tempdir>/sdp_meta_app_bundled_merged_evil/x.yml`` must not
        # be accepted just because its prefix starts the same way \u2014
        # the trailing ``os.sep`` guard in the prefix check protects
        # against this startswith-bypass.
        evil_dir = BUNDLED_SPEC_MERGED_DIR + "_evil"
        os.makedirs(evil_dir, exist_ok=True)
        self.addCleanup(lambda: os.path.isdir(evil_dir) and os.rmdir(evil_dir))
        evil_file = os.path.join(evil_dir, "x.yml")
        with open(evil_file, "w") as f:
            f.write("- a: 1\n")
        self.addCleanup(
            lambda: os.path.exists(evil_file) and os.unlink(evil_file)
        )
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path(evil_file, self._repo)

    def test_random_tempfile_outside_allowlist_still_rejected(self):
        # The allow-list is narrow: a hand-crafted ``<tempdir>/random.yml``
        # path (not under the bundled-merged dir) is NOT a server-managed
        # path and must still be rejected, so the S-2 guarantee survives.
        with tempfile.NamedTemporaryFile(
            suffix=".yml", delete=False, prefix="sdp_meta_attack_"
        ) as tf:
            tf.write(b"- a: 1\n")
            attack = tf.name
        self.addCleanup(
            lambda: os.path.exists(attack) and os.unlink(attack)
        )
        with self.assertRaisesRegex(
            _OnboardingFileError, r"escapes the repo root"
        ):
            _resolve_local_onboarding_path(attack, self._repo)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
