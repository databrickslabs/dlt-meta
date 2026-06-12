"""Tests for `databricks.labs.sdp_meta.identifiers`.

The helper rejects anything that isn't a regular SQL identifier so the
rest of the codebase can splice these names into SQL strings without
having to think about quoting (issue #261). The tests pin both the
acceptance set (regular identifiers) and the rejection set (hyphens,
periods, spaces, leading digits, backticks, control chars, empty,
oversized, non-string).
"""
from __future__ import annotations

import unittest

from databricks.labs.sdp_meta.identifiers import (
    SUPPORTED_SCD_TYPES,
    SUPPORTED_SOURCE_FORMATS,
    is_regular_identifier,
    validate_scd_type,
    validate_source_format,
    validate_uc_column_list,
    validate_uc_identifier,
)


class IsRegularIdentifierTests(unittest.TestCase):
    def test_simple_lowercase(self):
        self.assertTrue(is_regular_identifier("main"))

    def test_underscore_prefix(self):
        self.assertTrue(is_regular_identifier("_priv"))

    def test_alnum(self):
        self.assertTrue(is_regular_identifier("Schema123"))

    def test_hyphen_is_not_regular(self):
        self.assertFalse(is_regular_identifier("my-catalog"))

    def test_leading_digit_is_not_regular(self):
        self.assertFalse(is_regular_identifier("9lives"))

    def test_space_is_not_regular(self):
        self.assertFalse(is_regular_identifier("data lake"))

    def test_empty_is_not_regular(self):
        self.assertFalse(is_regular_identifier(""))

    def test_non_string_is_not_regular(self):
        self.assertFalse(is_regular_identifier(None))
        self.assertFalse(is_regular_identifier(123))


class ValidateUcIdentifierAcceptsTests(unittest.TestCase):
    def test_returns_input_on_valid(self):
        self.assertEqual(validate_uc_identifier("main"), "main")
        self.assertEqual(validate_uc_identifier("_x"), "_x")
        self.assertEqual(validate_uc_identifier("Schema123"), "Schema123")
        self.assertEqual(validate_uc_identifier("a_b_c"), "a_b_c")

    def test_max_length_accepted(self):
        validate_uc_identifier("a" * 255, kind="catalog")


class ValidateUcIdentifierRejectsTests(unittest.TestCase):
    def test_hyphen_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("my-catalog", kind="uc_catalog_name")

    def test_period_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad.name", kind="uc_catalog_name")

    def test_space_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("data lake", kind="uc_catalog_name")

    def test_leading_digit_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("9lives", kind="uc_catalog_name")

    def test_backtick_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad`name", kind="uc_catalog_name")

    def test_control_char_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_identifier("bad\x00name", kind="uc_catalog_name")

    def test_too_long_rejected(self):
        with self.assertRaisesRegex(ValueError, r"255"):
            validate_uc_identifier("a" * 256, kind="uc_catalog_name")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier("", kind="uc_catalog_name")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier(None, kind="uc_catalog_name")

    def test_non_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_identifier(123, kind="uc_catalog_name")

    def test_kind_appears_in_message(self):
        with self.assertRaisesRegex(ValueError, r"my custom kind"):
            validate_uc_identifier("", kind="my custom kind")

    def test_error_links_to_docs(self):
        # Make the error message actionable by pointing to the upstream
        # SQL identifier rules so the user knows the *why*.
        with self.assertRaises(ValueError) as ctx:
            validate_uc_identifier("my-cat", kind="uc_catalog_name")
        self.assertIn("sql-ref-identifiers", str(ctx.exception))


class ValidateUcColumnListTests(unittest.TestCase):
    """Pinning the four input shapes the onboarding parser already accepts.

    The helper has to round-trip every shape ``__parse_cluster_by_string``
    and the ``*_partition_columns`` parser handle, otherwise we'd reject
    onboarding files that were valid before this validation existed.
    """

    def test_none_returns_empty(self):
        self.assertEqual(validate_uc_column_list(None), [])

    def test_empty_string_returns_empty(self):
        self.assertEqual(validate_uc_column_list(""), [])

    def test_single_string(self):
        self.assertEqual(validate_uc_column_list("col1"), ["col1"])

    def test_comma_separated_string(self):
        self.assertEqual(
            validate_uc_column_list("col1,col2,col3"),
            ["col1", "col2", "col3"],
        )

    def test_comma_separated_strips_whitespace(self):
        self.assertEqual(
            validate_uc_column_list(" col1 , col2 "),
            ["col1", "col2"],
        )

    def test_python_list(self):
        self.assertEqual(
            validate_uc_column_list(["col1", "col2"]),
            ["col1", "col2"],
        )

    def test_stringified_list(self):
        # __parse_cluster_by_string accepts this shape via ast.literal_eval,
        # so we have to as well.
        self.assertEqual(
            validate_uc_column_list("['col1', 'col2']"),
            ["col1", "col2"],
        )

    def test_hyphen_in_comma_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                "col1,bad-col", kind="bronze_partition_columns"
            )

    def test_hyphen_in_list_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                ["col1", "bad-col"], kind="bronze_cluster_by"
            )

    def test_leading_digit_in_stringified_list_rejected(self):
        with self.assertRaisesRegex(ValueError, r"regular identifier"):
            validate_uc_column_list(
                "['1col', 'col2']", kind="bronze_cluster_by"
            )

    def test_empty_list_returns_empty(self):
        self.assertEqual(validate_uc_column_list([]), [])

    def test_list_with_empty_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_column_list(["col1", ""], kind="bronze_cluster_by")

    def test_list_with_non_string_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_uc_column_list(["col1", 42], kind="bronze_cluster_by")

    def test_unparseable_list_literal_rejected(self):
        with self.assertRaisesRegex(ValueError, r"could not be parsed"):
            validate_uc_column_list("[col1, col2]", kind="bronze_cluster_by")

    def test_non_str_non_list_rejected(self):
        with self.assertRaisesRegex(
            ValueError, r"string or list of column names"
        ):
            validate_uc_column_list(42, kind="bronze_cluster_by")

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"silver_cluster_by"):
            validate_uc_column_list("bad-col", kind="silver_cluster_by")


class ValidateSourceFormatTests(unittest.TestCase):
    """Pin the bronze-reader format set so a typo (``cloudfiles``) fails at
    onboarding instead of silently falling through every if/elif branch
    in ``DataflowPipeline`` and starting a pipeline with no input."""

    def test_supported_set_matches_expected(self):
        # Hard-coded so any drift between this module and dataflow_pipeline.py
        # / pipeline_readers.py shows up as a test failure rather than a
        # silently broken pipeline.
        self.assertEqual(
            SUPPORTED_SOURCE_FORMATS,
            frozenset({"cloudFiles", "delta", "kafka", "eventhub", "snapshot"}),
        )

    def test_each_supported_format_accepted(self):
        for fmt in SUPPORTED_SOURCE_FORMATS:
            self.assertEqual(validate_source_format(fmt), fmt)

    def test_typo_rejected(self):
        with self.assertRaisesRegex(ValueError, r"cloudfiles"):
            validate_source_format("cloudfiles")

    def test_case_sensitive(self):
        # DLT/Spark are case-sensitive on format name; this catches the
        # common ``CloudFiles`` typo.
        with self.assertRaisesRegex(ValueError, r"is not supported"):
            validate_source_format("CloudFiles")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_source_format("")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_source_format(None)

    def test_error_lists_allowed_values(self):
        # Allowed values must appear in the error so the user can fix it
        # without going to docs.
        with self.assertRaises(ValueError) as ctx:
            validate_source_format("parquet")
        msg = str(ctx.exception)
        for fmt in SUPPORTED_SOURCE_FORMATS:
            self.assertIn(fmt, msg)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"my_field"):
            validate_source_format("nope", kind="my_field")


class ValidateScdTypeTests(unittest.TestCase):
    """``stored_as_scd_type`` is a string in DLT's apply_changes API; reject
    integers, ``"3"``, etc., to catch typos at onboarding."""

    def test_supported_set_matches_expected(self):
        self.assertEqual(SUPPORTED_SCD_TYPES, frozenset({"1", "2"}))

    def test_one_accepted(self):
        self.assertEqual(validate_scd_type("1"), "1")

    def test_two_accepted(self):
        self.assertEqual(validate_scd_type("2"), "2")

    def test_int_rejected(self):
        # ``2 == "2"`` is False in Python, but we want to fail loudly
        # rather than silently coerce — see the comment in the validator.
        with self.assertRaisesRegex(ValueError, r"non-empty string"):
            validate_scd_type(2)

    def test_three_rejected(self):
        with self.assertRaisesRegex(ValueError, r"is not supported"):
            validate_scd_type("3")

    def test_empty_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_scd_type("")

    def test_none_rejected(self):
        with self.assertRaisesRegex(ValueError, r"non-empty"):
            validate_scd_type(None)

    def test_error_lists_allowed_values(self):
        with self.assertRaises(ValueError) as ctx:
            validate_scd_type("scd_2")
        msg = str(ctx.exception)
        self.assertIn("1", msg)
        self.assertIn("2", msg)

    def test_kind_appears_in_error(self):
        with self.assertRaisesRegex(ValueError, r"silver scd"):
            validate_scd_type("0", kind="silver scd")


if __name__ == "__main__":
    unittest.main()
