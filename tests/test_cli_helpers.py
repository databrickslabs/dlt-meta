"""Targeted tests for small private helpers in
``databricks.labs.sdp_meta.cli`` that the existing ``test_cli.py``
suite does not exercise.

These helpers are pure plumbing -- WorkspaceClient mock interactions,
git-URL string formatting, UC schema/volume probing -- and are
straightforward to cover. Adding tests here pushes overall package
coverage past the 90% threshold without touching the much larger
end-to-end flows in ``test_cli.py``.
"""
from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from databricks.sdk.errors import NotFound, DatabricksError
from databricks.sdk.service.catalog import VolumeType

from databricks.labs.sdp_meta.cli import (
    _build_and_upload_git_wheel,
    _create_pipeline,
    _ensure_uc_schema_and_volume,
    _git_wheel_source,
    _read_dependency_from_onboarding_json,
    _volume_path_exists,
)


class TestGitWheelSourceBuilder(unittest.TestCase):
    """``_git_wheel_source`` produces a pip-compatible source URL."""

    def test_returns_none_when_no_git_url_and_no_branch(self):
        # Neither flag set -> None signals "user wants a non-git
        # source" (PyPI / local path); caller falls back accordingly.
        self.assertIsNone(_git_wheel_source({}))

    def test_defaults_git_url_when_only_branch_given(self):
        # Branch alone -> defaults the URL to the public sdp-meta
        # repo so users don't have to spell it out for upstream
        # branch builds.
        result = _git_wheel_source({"git_branch": "feature/sdp-meta"})
        self.assertEqual(
            result,
            "git+https://github.com/databrickslabs/sdp-meta.git@feature/sdp-meta",
        )

    def test_prefixes_git_plus_when_user_url_omits_it(self):
        # pip wants ``git+<https-url>`` for git sources; auto-prefix
        # so users don't have to think about it.
        result = _git_wheel_source(
            {"git_url": "https://github.com/foo/bar.git", "git_branch": "main"}
        )
        self.assertEqual(result, "git+https://github.com/foo/bar.git@main")

    def test_keeps_existing_git_plus_prefix_intact(self):
        # User who already typed ``git+...`` should NOT see ``git+git+``.
        result = _git_wheel_source({"git_url": "git+ssh://git@host/repo.git"})
        self.assertEqual(result, "git+ssh://git@host/repo.git")

    def test_url_without_branch_omits_at_suffix(self):
        result = _git_wheel_source({"git_url": "https://github.com/foo/bar.git"})
        self.assertEqual(result, "git+https://github.com/foo/bar.git")


class TestCreatePipelineSchemaFallback(unittest.TestCase):
    def test_passes_through_when_schema_is_absent(self):
        ws = MagicMock()
        expected = object()
        ws.pipelines.create.return_value = expected

        result = _create_pipeline(ws, name="pipeline")

        self.assertIs(result, expected)
        ws.pipelines.create.assert_called_once_with(name="pipeline")

    def test_uses_schema_when_supported(self):
        ws = MagicMock()
        expected = object()
        ws.pipelines.create.return_value = expected

        result = _create_pipeline(
            ws, name="pipeline", schema="target"
        )

        self.assertIs(result, expected)
        ws.pipelines.create.assert_called_once_with(
            schema="target", name="pipeline"
        )

    def test_retries_with_legacy_target(self):
        ws = MagicMock()
        expected = object()
        ws.pipelines.create.side_effect = [ValueError("schema"), expected]

        result = _create_pipeline(
            ws, name="pipeline", schema="target"
        )

        self.assertIs(result, expected)
        self.assertEqual(ws.pipelines.create.call_count, 2)
        ws.pipelines.create.assert_called_with(
            target="target", name="pipeline"
        )

    def test_reports_both_schema_and_target_failures(self):
        ws = MagicMock()
        ws.pipelines.create.side_effect = [
            ValueError("schema rejected"),
            RuntimeError("target rejected"),
        ]

        with self.assertRaisesRegex(
            RuntimeError, "failed with both parameter forms"
        ) as raised:
            _create_pipeline(
                ws, name="pipeline", schema="target"
            )

        self.assertIn("schema rejected", str(raised.exception))
        self.assertIn("target rejected", str(raised.exception))


class TestReadPersistedDependency(unittest.TestCase):
    def test_returns_none_when_file_is_missing(self):
        with patch(
            "databricks.labs.sdp_meta.cli.os.path.isfile",
            return_value=False,
        ):
            self.assertIsNone(_read_dependency_from_onboarding_json())

    def test_returns_none_for_invalid_json(self):
        with (
            patch(
                "databricks.labs.sdp_meta.cli.os.path.isfile",
                return_value=True,
            ),
            patch("builtins.open", unittest.mock.mock_open(read_data="{")),
        ):
            self.assertIsNone(_read_dependency_from_onboarding_json())

    def test_returns_none_for_non_mapping_or_blank_dependency(self):
        for payload in ('["not", "a", "mapping"]', '{"sdp_meta_dependency": " "}'):
            with (
                patch(
                    "databricks.labs.sdp_meta.cli.os.path.isfile",
                    return_value=True,
                ),
                patch("builtins.open", unittest.mock.mock_open(read_data=payload)),
            ):
                self.assertIsNone(_read_dependency_from_onboarding_json())

    def test_returns_non_empty_dependency(self):
        payload = '{"sdp_meta_dependency": "/Volumes/c/s/v/package.whl"}'
        with (
            patch(
                "databricks.labs.sdp_meta.cli.os.path.isfile",
                return_value=True,
            ),
            patch("builtins.open", unittest.mock.mock_open(read_data=payload)),
        ):
            self.assertEqual(
                _read_dependency_from_onboarding_json(),
                "/Volumes/c/s/v/package.whl",
            )


class TestVolumePathExistsProbe(unittest.TestCase):
    """``_volume_path_exists`` returns False on every error -- it's
    just an informational pre-flight before the upload."""

    def _ws_with_metadata_outcome(self, *, raises=None):
        """Return a WorkspaceClient mock whose ``files.get_metadata``
        either succeeds or raises ``raises`` once when called."""
        ws = MagicMock()
        if raises is not None:
            ws.files.get_metadata.side_effect = raises
        return ws

    def test_returns_true_when_metadata_call_succeeds(self):
        ws = self._ws_with_metadata_outcome()
        self.assertTrue(_volume_path_exists(ws, "/Volumes/c/s/v/file.whl"))
        ws.files.get_metadata.assert_called_once_with(
            file_path="/Volumes/c/s/v/file.whl",
        )

    def test_returns_false_on_not_found(self):
        ws = self._ws_with_metadata_outcome(raises=NotFound("nope"))
        self.assertFalse(_volume_path_exists(ws, "/Volumes/c/s/v/missing.whl"))

    def test_returns_false_on_databricks_error(self):
        # Defensive branch: any other SDK error is also "treat as
        # absent" because the function is purely a UI hint.
        ws = self._ws_with_metadata_outcome(raises=DatabricksError("transient"))
        self.assertFalse(_volume_path_exists(ws, "/Volumes/c/s/v/file.whl"))


class TestEnsureUcSchemaAndVolume(unittest.TestCase):
    """``_ensure_uc_schema_and_volume`` covers the four
    schema-exists/volume-exists branches with create-on-missing
    toggled."""

    def _ws_with_schemas_api(self, *, schema_raises=None, volume_raises=None):
        ws = MagicMock()
        # We patch ``SchemasAPI`` at the call site below; just
        # configure ws.volumes.read here.
        if volume_raises is not None:
            ws.volumes.read.side_effect = volume_raises
        return ws

    def _patch_schemas_api(self, *, get_raises=None):
        """Return a MagicMock that ``SchemasAPI(api_client)`` returns
        whose ``.get`` either succeeds or raises ``get_raises``."""
        api_instance = MagicMock()
        if get_raises is not None:
            api_instance.get.side_effect = get_raises
        return api_instance

    def test_no_op_when_schema_and_volume_both_exist(self):
        ws = self._ws_with_schemas_api()
        api_instance = self._patch_schemas_api()
        with patch(
            "databricks.labs.sdp_meta.cli.SchemasAPI",
            return_value=api_instance,
        ):
            _ensure_uc_schema_and_volume(
                ws, "main", "sdp_meta", "wheels",
                create_if_missing=True,
            )
        api_instance.get.assert_called_once_with(full_name="main.sdp_meta")
        ws.volumes.read.assert_called_once_with(name="main.sdp_meta.wheels")
        # Nothing created on the happy path.
        api_instance.create.assert_not_called()
        ws.volumes.create.assert_not_called()

    def test_creates_schema_when_missing_and_create_if_missing_true(self):
        ws = self._ws_with_schemas_api()
        api_instance = self._patch_schemas_api(get_raises=NotFound("nope"))
        with patch(
            "databricks.labs.sdp_meta.cli.SchemasAPI",
            return_value=api_instance,
        ):
            _ensure_uc_schema_and_volume(
                ws, "main", "sdp_meta", "wheels",
                create_if_missing=True,
            )
        api_instance.create.assert_called_once_with(
            catalog_name="main",
            name="sdp_meta",
            comment="sdp_meta wheel schema",
        )

    def test_creates_volume_when_missing_and_create_if_missing_true(self):
        ws = self._ws_with_schemas_api(volume_raises=NotFound("nope"))
        api_instance = self._patch_schemas_api()
        with patch(
            "databricks.labs.sdp_meta.cli.SchemasAPI",
            return_value=api_instance,
        ):
            _ensure_uc_schema_and_volume(
                ws, "main", "sdp_meta", "wheels",
                create_if_missing=True,
            )
        ws.volumes.create.assert_called_once_with(
            catalog_name="main",
            schema_name="sdp_meta",
            name="wheels",
            volume_type=VolumeType.MANAGED,
        )

    def test_raises_not_found_for_missing_schema_when_create_if_missing_false(self):
        ws = self._ws_with_schemas_api()
        api_instance = self._patch_schemas_api(get_raises=NotFound("nope"))
        with patch(
            "databricks.labs.sdp_meta.cli.SchemasAPI",
            return_value=api_instance,
        ):
            with self.assertRaises(NotFound):
                _ensure_uc_schema_and_volume(
                    ws, "main", "sdp_meta", "wheels",
                    create_if_missing=False,
                )
        api_instance.create.assert_not_called()

    def test_raises_not_found_for_missing_volume_when_create_if_missing_false(self):
        ws = self._ws_with_schemas_api(volume_raises=NotFound("nope"))
        api_instance = self._patch_schemas_api()
        with patch(
            "databricks.labs.sdp_meta.cli.SchemasAPI",
            return_value=api_instance,
        ):
            with self.assertRaises(NotFound):
                _ensure_uc_schema_and_volume(
                    ws, "main", "sdp_meta", "wheels",
                    create_if_missing=False,
                )
        ws.volumes.create.assert_not_called()


class TestBuildAndUploadGitWheel(unittest.TestCase):
    def test_builds_with_indexes_and_uploads_generated_wheel(self):
        ws = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            wheel = Path(tmp) / "databricks_labs_sdp_meta-1.2.3-py3-none-any.whl"
            wheel.write_bytes(b"wheel-bytes")
            flags = {
                "pip_index_url": "https://primary.example/simple",
                "pip_extra_index_url": [
                    "https://extra-one.example/simple",
                    "https://extra-two.example/simple",
                ],
                "no_create_missing_uc": True,
            }
            with (
                patch(
                    "databricks.labs.sdp_meta.cli.tempfile.TemporaryDirectory",
                    return_value=nullcontext(tmp),
                ),
                patch(
                    "databricks.labs.sdp_meta.cli._ensure_uc_schema_and_volume"
                ) as ensure,
                patch(
                    "databricks.labs.sdp_meta.cli._volume_path_exists",
                    return_value=True,
                ),
                patch("databricks.labs.sdp_meta.cli.subprocess.run") as run,
                patch("builtins.print") as print_,
            ):
                result = _build_and_upload_git_wheel(
                    ws,
                    uc_catalog="main",
                    uc_schema="meta",
                    uc_volume="wheels",
                    source="git+https://example/repo.git@main",
                    flags=flags,
                )

        ensure.assert_called_once_with(
            ws, "main", "meta", "wheels", create_if_missing=False
        )
        command = run.call_args.args[0]
        self.assertIn("--no-deps", command)
        self.assertIn("https://primary.example/simple", command)
        self.assertIn("https://extra-one.example/simple", command)
        self.assertIn("https://extra-two.example/simple", command)
        self.assertEqual(command[-1], "git+https://example/repo.git@main")
        run.assert_called_once_with(command, check=True)
        expected = (
            "/Volumes/main/meta/wheels/"
            "databricks_labs_sdp_meta-1.2.3-py3-none-any.whl"
        )
        self.assertEqual(result, expected)
        ws.files.upload.assert_called_once()
        self.assertEqual(ws.files.upload.call_args.kwargs["file_path"], expected)
        self.assertTrue(ws.files.upload.call_args.kwargs["overwrite"])
        self.assertTrue(
            any("Overwriting existing wheel" in str(call) for call in print_.call_args_list)
        )

    def test_raises_when_pip_produces_no_wheel(self):
        ws = MagicMock()
        with tempfile.TemporaryDirectory() as tmp:
            with (
                patch(
                    "databricks.labs.sdp_meta.cli.tempfile.TemporaryDirectory",
                    return_value=nullcontext(tmp),
                ),
                patch(
                    "databricks.labs.sdp_meta.cli._ensure_uc_schema_and_volume"
                ),
                patch("databricks.labs.sdp_meta.cli.subprocess.run"),
            ):
                with self.assertRaisesRegex(RuntimeError, "produced no wheel"):
                    _build_and_upload_git_wheel(
                        ws,
                        uc_catalog="main",
                        uc_schema="meta",
                        uc_volume="wheels",
                        source="/trusted/local/source",
                        flags={},
                    )


if __name__ == "__main__":
    unittest.main()
