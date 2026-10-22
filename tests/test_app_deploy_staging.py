"""Tests that the App deploy scripts stage every directory the wheel build needs.

``scripts/deploy_app.sh`` and ``scripts/deploy_app.ps1`` copy an allow-list of
repo directories to the App source-code-path. On App start,
``databricks_app/start.sh`` runs ``python setup.py bdist_wheel`` against that
copy. If a directory that setup.py reads is missing from the allow-list, the
wheel build fails (``examples/``) or silently drops packages (``compat/``),
and the App crashes after a "successful" deploy.

Both scripts also stage ``.databricksignore``, which ``databricks sync`` is
expected to honour. A required directory listed there would be staged but not
uploaded, so the test also checks that the staged ignore file does not exclude
any of them.

The required directories are derived from setup.py (``package_dir`` roots plus
the ``examples/`` tree that the build_py hook copies), so a new packaged
directory fails this test until both scripts stage it too.
"""
import ast
import fnmatch
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SETUP_PY = REPO_ROOT / "setup.py"
DEPLOY_SH = REPO_ROOT / "scripts" / "deploy_app.sh"
DEPLOY_PS1 = REPO_ROOT / "scripts" / "deploy_app.ps1"
DATABRICKSIGNORE = REPO_ROOT / ".databricksignore"

BASH_DIRS_RE = re.compile(r"^APP_RUNTIME_DIRS=\((.*?)^\)", re.MULTILINE | re.DOTALL)
PS1_DIRS_RE = re.compile(r"\$RuntimeDirs\s*=\s*@\((.*?)^\s*\)\s*$", re.MULTILINE | re.DOTALL)


def _setup_package_dir() -> dict:
    """Return the literal ``package_dir`` mapping of the ``setup()`` call."""
    tree = ast.parse(SETUP_PY.read_text(encoding="utf-8"), str(SETUP_PY))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
            for keyword in node.keywords:
                if keyword.arg == "package_dir":
                    return ast.literal_eval(keyword.value)
    raise AssertionError("setup.py has no literal package_dir in its setup() call")


def _dirs_needed_by_wheel_build() -> set:
    """Top-level repo directories that ``python setup.py bdist_wheel`` reads."""
    needed = {Path(path).parts[0] for path in _setup_package_dir().values()}
    if '"examples"' in SETUP_PY.read_text(encoding="utf-8"):
        needed.add("examples")
    return needed


def _bash_staged_dirs() -> set:
    match = BASH_DIRS_RE.search(DEPLOY_SH.read_text(encoding="utf-8"))
    if not match:
        raise AssertionError("APP_RUNTIME_DIRS=( ... ) not found in deploy_app.sh")
    lines = (line.split("#", 1)[0].strip() for line in match.group(1).splitlines())
    return {line.split()[0] for line in lines if line}


def _ps1_staged_dirs() -> set:
    match = PS1_DIRS_RE.search(DEPLOY_PS1.read_text(encoding="utf-8"))
    if not match:
        raise AssertionError("$RuntimeDirs = @( ... ) not found in deploy_app.ps1")
    body = "\n".join(line.split("#", 1)[0] for line in match.group(1).splitlines())
    return set(re.findall(r"'([^']+)'", body))


def _ignore_patterns() -> list:
    """Non-comment, non-negated patterns of the repo-root ``.databricksignore``."""
    lines = (line.strip() for line in DATABRICKSIGNORE.read_text(encoding="utf-8").splitlines())
    return [line for line in lines if line and not line.startswith(("#", "!"))]


def _ignore_excludes_top_level_dir(pattern: str, directory: str) -> bool:
    """True if a .gitignore-style ``pattern`` excludes the repo-root ``directory``.

    Only patterns that can match a top-level directory name are relevant:
    ``name/``, ``/name``, ``name``, ``**/name`` and globs such as ``ex*/``.
    Patterns with an inner slash (``src/foo/``) only match deeper paths.
    """
    pattern = pattern.rstrip("/")
    if pattern.startswith("**/"):
        pattern = pattern[3:]
    pattern = pattern.lstrip("/")
    if "/" in pattern:
        return False
    return fnmatch.fnmatchcase(directory, pattern)


class AppDeployStagingTests(unittest.TestCase):

    def test_wheel_build_dirs_are_derived(self):
        needed = _dirs_needed_by_wheel_build()
        self.assertIn("src", needed)
        self.assertIn("examples", needed)

    def test_bash_script_stages_every_dir_the_wheel_build_needs(self):
        missing = _dirs_needed_by_wheel_build() - _bash_staged_dirs()
        self.assertFalse(missing, f"deploy_app.sh APP_RUNTIME_DIRS is missing {sorted(missing)}")

    def test_powershell_script_stages_every_dir_the_wheel_build_needs(self):
        missing = _dirs_needed_by_wheel_build() - _ps1_staged_dirs()
        self.assertFalse(missing, f"deploy_app.ps1 $RuntimeDirs is missing {sorted(missing)}")

    def test_both_scripts_stage_the_same_dirs(self):
        self.assertEqual(_bash_staged_dirs(), _ps1_staged_dirs())

    def test_both_scripts_stage_databricksignore(self):
        self.assertIn(".databricksignore", DEPLOY_SH.read_text(encoding="utf-8"))
        self.assertIn("'.databricksignore'", DEPLOY_PS1.read_text(encoding="utf-8"))

    def test_databricksignore_does_not_exclude_wheel_build_dirs(self):
        excluded = {
            f"{directory} (by {pattern!r})"
            for directory in _dirs_needed_by_wheel_build()
            for pattern in _ignore_patterns()
            if _ignore_excludes_top_level_dir(pattern, directory)
        }
        self.assertFalse(
            excluded,
            f".databricksignore excludes directories the wheel build needs: {sorted(excluded)}",
        )

    def test_ignore_matcher_recognises_top_level_dir_patterns(self):
        for pattern in ("examples/", "/examples/", "examples", "**/examples/", "ex*/"):
            self.assertTrue(_ignore_excludes_top_level_dir(pattern, "examples"), pattern)
        for pattern in ("docs/", "src/databricks/", "**/*.pyc", "examples/json/"):
            self.assertFalse(_ignore_excludes_top_level_dir(pattern, "examples"), pattern)


if __name__ == "__main__":
    unittest.main()
