"""Tests that the two distributions advertise the same Python support.

The supported interpreter range is written in three places: ``python_requires``
and the version classifiers in setup.py, plus the same pair in compat/setup.py.
pip only enforces ``python_requires``, so a stale classifier list or a compat
wrapper that drifts from the package it forwards to would ship unnoticed.

The setup.py files are read with ``ast`` rather than imported -- executing them
at test time would need setuptools' build context and would run the README
rewriting in the primary setup.py for no benefit.
"""
import ast
import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PRIMARY_SETUP = REPO_ROOT / "setup.py"
COMPAT_SETUP = REPO_ROOT / "compat" / "setup.py"

# ">=3.8, <3.13" -- an inclusive floor and an exclusive ceiling, both 3.x.
PYTHON_REQUIRES_RE = re.compile(r"^>=3\.(\d+),\s*<3\.(\d+)$")
VERSION_CLASSIFIER_RE = re.compile(r"^Programming Language :: Python :: 3\.(\d+)$")


def _setup_kwargs(setup_py: Path) -> dict:
    """Return the literal keyword arguments of the ``setup()`` call."""
    tree = ast.parse(setup_py.read_text(encoding="utf-8"), str(setup_py))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "setup":
            kwargs = {}
            for keyword in node.keywords:
                if keyword.arg is None:
                    continue
                try:
                    kwargs[keyword.arg] = ast.literal_eval(keyword.value)
                except ValueError:
                    # Computed values such as long_description; not under test.
                    continue
            return kwargs
    raise AssertionError(f"no setup() call found in {setup_py}")


class PythonSupportMetadataTests(unittest.TestCase):

    def setUp(self):
        self.primary = _setup_kwargs(PRIMARY_SETUP)
        self.compat = _setup_kwargs(COMPAT_SETUP)

    def _declared_range(self, kwargs: dict, label: str) -> range:
        """Minor versions covered by ``python_requires``, e.g. range(8, 13)."""
        python_requires = kwargs.get("python_requires")
        self.assertIsNotNone(python_requires, f"{label} declares no python_requires")
        match = PYTHON_REQUIRES_RE.match(python_requires)
        self.assertIsNotNone(
            match,
            f"{label} python_requires={python_requires!r} is not of the form "
            "'>=3.X, <3.Y'; update PYTHON_REQUIRES_RE if this is intentional",
        )
        floor, ceiling = int(match.group(1)), int(match.group(2))
        self.assertLess(floor, ceiling, f"{label} declares an empty version range")
        return range(floor, ceiling)

    def _version_classifiers(self, kwargs: dict) -> set:
        return {
            int(match.group(1))
            for match in (
                VERSION_CLASSIFIER_RE.match(c) for c in kwargs.get("classifiers", [])
            )
            if match
        }

    def test_both_distributions_declare_the_same_python_requires(self):
        self.assertEqual(
            self.primary.get("python_requires"),
            self.compat.get("python_requires"),
            "setup.py and compat/setup.py must accept the same interpreters -- the "
            "compat wrapper depends on the primary package, so a wider floor or "
            "ceiling there installs on interpreters its dependency rejects",
        )

    def test_version_classifiers_cover_the_declared_range(self):
        for label, kwargs in (("setup.py", self.primary), ("compat/setup.py", self.compat)):
            with self.subTest(setup=label):
                expected = set(self._declared_range(kwargs, label))
                self.assertEqual(
                    self._version_classifiers(kwargs),
                    expected,
                    f"{label} version classifiers do not match python_requires; "
                    "PyPI's Programming Language facet would advertise the wrong "
                    "interpreters",
                )

    def test_base_python3_classifiers_are_present(self):
        for label, kwargs in (("setup.py", self.primary), ("compat/setup.py", self.compat)):
            with self.subTest(setup=label):
                classifiers = kwargs.get("classifiers", [])
                self.assertIn("Programming Language :: Python :: 3", classifiers)
                self.assertIn("Programming Language :: Python :: 3 :: Only", classifiers)


if __name__ == "__main__":
    unittest.main()
