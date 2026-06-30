"""Unity Catalog / SQL identifier and onboarding-enum validation.

`sdp-meta` accepts UC catalog / schema / table / volume names from many
input boundaries (CLI prompts, ``OnboardCommand`` / ``DeployCommand``
constructors, the DAB template, demo scripts, integration test runners).
All of those names eventually get spliced into SQL identifiers downstream
without backtick-quoting, so we reject anything that isn't a *regular*
SQL identifier as defined in
https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers --
i.e. ``[A-Za-z_][A-Za-z0-9_]*``.

This is the entire contract: strict validation at every input boundary,
and the rest of the codebase (``DataflowPipeline``, the rendered DAB
pipeline, the onboarding job) can splice these names into SQL strings
without further escaping (issue #261). Hyphenated catalog names that UC
itself permits are rejected here with a clear, actionable error message
instead of failing later as a cryptic Spark name-resolution error.

This module also pins the bounded enums onboarding accepts —
:data:`SUPPORTED_SOURCE_FORMATS` and :data:`SUPPORTED_SCD_TYPES` — so
typos like ``"cloudfiles"`` or ``scd_type="3"`` fail at the onboarding
boundary with a helpful "use one of …" message, instead of silently
flowing into the DLT pipeline and failing there.
"""
from __future__ import annotations

import ast
import re
import sys
from typing import Optional

_REGULAR_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_MAX_IDENT_LEN = 255

# Source formats supported by the bronze readers in
# ``dataflow_pipeline.py`` and ``pipeline_readers.py`` (the
# ``if/elif source_format == ...`` chains). Pinned here so the bundle CLI,
# DAB template, and onboarding pre-flight all agree on the same set; if
# you add a reader, add the format here too.
SUPPORTED_SOURCE_FORMATS = frozenset(
    {"cloudFiles", "delta", "kafka", "eventhub", "snapshot"}
)

# SCD types DLT's apply_changes / apply_changes_from_snapshot accept.
# DLT requires the value as a string ("1" or "2"); see
# ``dlt.apply_changes(stored_as_scd_type=...)`` and the existing
# ``cdc_apply_changes.scd_type == "2"`` comparison in
# ``dataflow_pipeline.py``.
SUPPORTED_SCD_TYPES = frozenset({"1", "2"})


def is_regular_identifier(name) -> bool:
    """Return True iff ``name`` is a regular SQL identifier
    (``[A-Za-z_][A-Za-z0-9_]*``) and so safe to splice into SQL unquoted."""
    return isinstance(name, str) and bool(_REGULAR_IDENT_RE.match(name))


def validate_uc_identifier(name, *, kind: str = "identifier") -> str:
    """Validate ``name`` as a UC identifier; return it unchanged on success.

    Raises ``ValueError`` with an actionable message when ``name`` cannot be
    safely used as a UC identifier inside the sdp-meta toolchain. The rule
    is strict: regular SQL identifiers only (letters, digits, underscores;
    must start with a letter or underscore; max 255 chars).

    Hyphens, periods, spaces, leading digits and backticks are all
    rejected. UC itself permits hyphens in catalog / schema names, but the
    sdp-meta deployed pipeline reads ``bronze.dataflowspecTable`` /
    ``silver.dataflowspecTable`` directly via ``spark.read.table(...)``
    without backtick-quoting, so we reject hyphens at the input boundary
    rather than letting users hit a confusing Spark error at runtime
    (issue #261).
    """
    if not isinstance(name, str):
        raise ValueError(
            f"{kind} must be a non-empty string, got "
            f"{type(name).__name__}: {name!r}"
        )
    if not name:
        raise ValueError(f"{kind} must be a non-empty string")
    if len(name) > _MAX_IDENT_LEN:
        raise ValueError(
            f"{kind} {name!r} is {len(name)} characters; maximum allowed is "
            f"{_MAX_IDENT_LEN}"
        )
    if not _REGULAR_IDENT_RE.match(name):
        raise ValueError(
            f"{kind} {name!r} is not a valid Databricks SQL regular identifier. "
            f"Names must match {_REGULAR_IDENT_RE.pattern} (letters, digits "
            f"and underscores only; must start with a letter or underscore). "
            f"Hyphens, periods, spaces and leading digits are not supported. "
            f"See "
            f"https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-identifiers"
        )
    return name


def validate_uc_full_name(name, *, kind: str = "identifier", max_parts: int = 3) -> str:
    """Validate a dotted multi-part UC name, e.g. ``catalog.schema.table``.

    Each segment must independently pass :func:`validate_uc_identifier`,
    so the full name is safe to splice into SQL unquoted. Accepts 1, 2, or
    3 parts (``table`` / ``schema.table`` / ``catalog.schema.table``);
    callers can override ``max_parts`` if they want to be strict about a
    specific arity.

    Returns ``name`` unchanged on success. Raises ``ValueError`` with an
    actionable message on failure.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(f"{kind} must be a non-empty string, got {name!r}")
    parts = name.split(".")
    if len(parts) < 1 or len(parts) > max_parts:
        raise ValueError(
            f"{kind} {name!r} has {len(parts)} dotted segments; expected "
            f"between 1 and {max_parts}."
        )
    for i, part in enumerate(parts):
        # Each segment gets a positional kind so the error message points
        # at the offending one (e.g. "schema in database 'main.bad-name'").
        validate_uc_identifier(part, kind=f"segment {i + 1} of {kind}")
    return name


def validate_uc_column_list(value, *, kind: str = "column list") -> list:
    """Validate a column-name list/string and return the parsed column names.

    Onboarding accepts column-name fields in several shapes that the
    existing parser code in ``onboard_dataflowspec.py`` handles:

    * a single column name string (``"col1"``)
    * a comma-separated string (``"col1,col2"``) — used for
      ``*_partition_columns`` / ``*_quarantine_table_partitions``
    * a Python list (``["col1", "col2"]``) — used for ``*_cluster_by``
    * a string representation of a list (``"[col1, col2]"``) — also
      ``*_cluster_by``, parsed via ``ast.literal_eval``

    Each resulting column name must be a regular SQL identifier so it can
    be safely spliced into ``PARTITIONED BY`` / ``CLUSTER BY`` DDL or
    passed to DLT's ``partition_cols`` / ``cluster_by`` kwargs without
    surprising failures (issue #261).

    ``None`` and empty strings are treated as "no columns" and return an
    empty list rather than raising — matches the onboarding code's
    "if present and truthy" pattern so optional fields don't trip the
    pre-flight check.

    Returns the validated list of column names. Raises ``ValueError`` if
    any element fails :func:`validate_uc_identifier`.
    """
    if value is None or value == "":
        return []

    columns: list
    if isinstance(value, list):
        columns = list(value)
    elif isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            # Stringified list, e.g. "[col1, col2]". Use literal_eval so we
            # accept the exact same shape the existing __parse_cluster_by_string
            # parser does — no surprises if it parsed there but failed here.
            try:
                parsed = ast.literal_eval(stripped)
            except (SyntaxError, ValueError) as exc:
                raise ValueError(
                    f"{kind} {value!r} looks like a list literal but could "
                    f"not be parsed: {exc}"
                ) from exc
            if not isinstance(parsed, list):
                raise ValueError(
                    f"{kind} {value!r} parsed to {type(parsed).__name__}, "
                    f"expected list"
                )
            columns = parsed
        else:
            columns = [c.strip() for c in stripped.split(",")]
    else:
        raise ValueError(
            f"{kind} must be a string or list of column names, got "
            f"{type(value).__name__}: {value!r}"
        )

    out = []
    for i, col in enumerate(columns):
        if not isinstance(col, str) or not col:
            raise ValueError(
                f"{kind} entry {i} is not a non-empty string: {col!r}"
            )
        validate_uc_identifier(col, kind=f"{kind} entry {i}")
        out.append(col)
    return out


def validate_source_format(value, *, kind: str = "source_format") -> str:
    """Validate ``value`` is one of :data:`SUPPORTED_SOURCE_FORMATS`.

    The bronze readers branch on this string in
    ``dataflow_pipeline.py``; an unknown value silently falls through
    every ``elif`` and the pipeline starts with no input. Catching it
    at onboarding turns that "pipeline runs but does nothing" failure
    into a clear actionable error.

    Match is case-sensitive on purpose — DLT and Spark both treat
    ``"cloudFiles"`` and ``"cloudfiles"`` as different format names, so
    accepting variants here would mask a real bug.
    """
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"{kind} must be a non-empty string, got {value!r}"
        )
    if value not in SUPPORTED_SOURCE_FORMATS:
        # Sort for deterministic, alphabetic error messages.
        allowed = ", ".join(sorted(SUPPORTED_SOURCE_FORMATS))
        raise ValueError(
            f"{kind}={value!r} is not supported. Use one of: {allowed}."
        )
    return value


def validate_scd_type(value, *, kind: str = "scd_type") -> str:
    """Validate ``value`` is one of :data:`SUPPORTED_SCD_TYPES` (``"1"``/``"2"``).

    DLT's ``apply_changes`` / ``apply_changes_from_snapshot`` accept the
    SCD type as a string. Catching a typo (``"3"``, integer ``2``, etc.)
    here surfaces it during onboarding with the allowed values inlined,
    instead of bubbling up later as an opaque DLT runtime error.
    """
    # We deliberately reject ``int`` here even though Python's ``2 == "2"``
    # is False — accepting both would mean the onboarding-spec dataclasses
    # carry mixed types, and the existing ``cdc_apply_changes.scd_type
    # == "2"`` comparisons in dataflow_pipeline.py would silently miss the
    # int variant. The onboarding contract is "string", so we enforce it.
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"{kind} must be a non-empty string, got {type(value).__name__}: {value!r}"
        )
    if value not in SUPPORTED_SCD_TYPES:
        allowed = ", ".join(sorted(SUPPORTED_SCD_TYPES))
        raise ValueError(
            f"{kind}={value!r} is not supported. Use one of: {allowed}."
        )
    return value


# SQL fragments (e.g. an optional WHERE clause on the App's Metadata
# Browse tab) cannot be parameterised by the Databricks Statement
# Execution API for structural positions, so anything user-supplied
# must be denylist-validated before being spliced into a query. We
# block the tokens that make second-statement / comment-out / DDL
# escapes possible. The set is intentionally narrow: legitimate
# WHERE clauses (column comparisons, AND / OR / IN / LIKE on a list
# of literal values) all pass; anything that smells like
# ``'; DROP TABLE x --`` or ``UNION SELECT … FROM system.…`` is
# rejected at the App boundary with an actionable 400.
#
# Why a denylist rather than a strict allowlist regex: even a
# minimal real-world WHERE clause exercises CASE / BETWEEN /
# subexpressions / quoted string literals containing arbitrary
# bytes, all of which are awkward to express as a single allow
# pattern without rejecting legitimate input. The denylist below
# covers every escape vector the Databricks SQL grammar exposes for
# stacking, commenting out the trailing LIMIT, or invoking DDL /
# DML — those are exactly the structural primitives the dialect
# requires for an injection to do damage. Everything else stays
# scoped to the SELECT we own.
_DANGEROUS_SQL_TOKENS = (
    ";",      # statement separator
    "--",     # line comment (would comment out the trailing LIMIT)
    "/*",     # block comment open
    "*/",     # block comment close
    "`",      # identifier delimiter — would let caller escape to a
              # different table reference
)

# Case-insensitive keyword denylist. A WHERE clause genuinely needs
# none of these — they only appear in injection payloads that try to
# escape the SELECT we built. Word-boundary matched (``\bUNION\b``)
# so column names like ``unionized_state`` aren't false-positives.
_DANGEROUS_SQL_KEYWORDS = (
    "UNION", "INTERSECT", "EXCEPT",      # set operations (data exfil)
    "INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "COPY",
    "DROP", "CREATE", "ALTER", "RENAME", "REPLACE",
    "GRANT", "REVOKE",
    "EXEC", "EXECUTE", "CALL",
)

_DANGEROUS_KEYWORD_RE = re.compile(
    r"\b(" + "|".join(_DANGEROUS_SQL_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

_MAX_WHERE_CLAUSE_LEN = 2000


def validate_sql_where_clause(value, *, kind: str = "where_clause") -> str:
    """Reject SQL fragments that contain statement-separation,
    comment, or DDL/DML escape tokens.

    Returns ``value`` unchanged on success. Raises ``ValueError`` with
    an actionable message identifying the offending token so the user
    can adjust their input.

    Intended for the narrow case where a SELECT's WHERE clause is
    composed from user input (Metadata Browse table preview) and the
    Statement Execution API cannot bind it as a parameter. Callers
    that can use named parameters (``ws.statement_execution.execute_statement(..., parameters=[...])``)
    should prefer that path.
    """
    if value is None or value == "":
        return ""
    if not isinstance(value, str):
        raise ValueError(
            f"{kind} must be a string, got {type(value).__name__}: {value!r}"
        )
    if len(value) > _MAX_WHERE_CLAUSE_LEN:
        raise ValueError(
            f"{kind} is {len(value)} characters; maximum allowed is "
            f"{_MAX_WHERE_CLAUSE_LEN}"
        )
    for token in _DANGEROUS_SQL_TOKENS:
        if token in value:
            raise ValueError(
                f"{kind} contains disallowed token {token!r}. Statement "
                f"separators, comments, and identifier delimiters are not "
                f"permitted in user-supplied WHERE clauses."
            )
    match = _DANGEROUS_KEYWORD_RE.search(value)
    if match:
        raise ValueError(
            f"{kind} contains disallowed keyword "
            f"{match.group(1).upper()!r}. Only simple filter expressions "
            f"(column comparisons joined by AND / OR) are permitted; set "
            f"operations and DDL / DML are not."
        )
    return value


def _format_prompt_error(message: str) -> str:
    """Format an error message for an interactive UC-identifier prompt.

    Uses ANSI red ONLY when stderr is a real TTY; piped/captured output
    (CI logs, file redirects) gets plain text so the escape codes don't
    end up rendered as garbage in build logs.
    """
    if sys.stderr.isatty():
        return f"\033[31m[ERROR] {message}\033[0m"
    return f"[ERROR] {message}"


def prompt_uc_identifier(
    wsi,
    text: str,
    *,
    kind: str,
    default: Optional[str] = None,
    max_attempts: int = 10,
) -> str:
    """Interactively prompt for a UC identifier with validation + retry.

    Wraps ``wsi._question`` (a
    ``databricks.labs.blueprint.installer.WorkspaceInstaller`` method):
    on each attempt, the user-supplied value is run through
    :func:`validate_uc_identifier`. Validation failures print a clear
    error message and re-prompt instead of crashing — matches the
    behavior every CLI / DAB prompt expects (issue #261).

    After ``max_attempts`` consecutive bad answers we raise so
    non-interactive runs (e.g. piped stdin returning the same junk every
    time) still terminate. The error stream is gated on
    ``sys.stderr.isatty()`` so CI logs don't get ANSI escape garbage.
    """
    for _ in range(max_attempts):
        value = wsi._question(text, default=default)
        try:
            return validate_uc_identifier(value, kind=kind)
        except ValueError as exc:
            print(_format_prompt_error(str(exc)) + "\n", file=sys.stderr)
    raise ValueError(
        f"Could not get a valid {kind} after {max_attempts} attempts"
    )
