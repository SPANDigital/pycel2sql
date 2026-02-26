"""Validation helpers, escaping, and regex conversion utilities."""

from __future__ import annotations

import re

from pycel2sql._errors import (
    InvalidFieldNameError,
    InvalidRegexPatternError,
)

MAX_POSTGRESQL_IDENTIFIER_LENGTH = 63

FIELD_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

RESERVED_SQL_KEYWORDS: set[str] = {
    "all", "alter", "and", "any", "array", "as", "asc", "between",
    "by", "case", "cast", "check", "column", "constraint", "create",
    "cross", "current", "current_date", "current_time", "current_timestamp",
    "current_user", "default", "delete", "desc", "distinct", "drop",
    "else", "end", "except", "exists", "false", "for", "foreign",
    "from", "full", "grant", "group", "having", "in", "index", "inner",
    "insert", "intersect", "into", "is", "join", "left", "like", "limit",
    "not", "null", "offset", "on", "or", "order", "outer", "primary",
    "references", "right", "select", "session_user", "set", "some",
    "table", "then", "to", "true", "union", "unique", "update", "user",
    "using", "values", "when", "where", "with",
}

# RE2 -> POSIX regex conversion limits
MAX_REGEX_LENGTH = 500
MAX_REGEX_GROUPS = 20
MAX_REGEX_NESTING = 10


def validate_field_name(name: str) -> None:
    """Validate a SQL field/identifier name."""
    if not name:
        raise InvalidFieldNameError(
            "field name cannot be empty",
            "empty field name provided",
        )
    if len(name) > MAX_POSTGRESQL_IDENTIFIER_LENGTH:
        raise InvalidFieldNameError(
            "field name too long",
            f"field name '{name}' exceeds {MAX_POSTGRESQL_IDENTIFIER_LENGTH} characters",
        )
    if not FIELD_NAME_RE.match(name):
        raise InvalidFieldNameError(
            "invalid field name format",
            f"field name '{name}' contains invalid characters",
        )
    if name.lower() in RESERVED_SQL_KEYWORDS:
        raise InvalidFieldNameError(
            "field name is a reserved SQL keyword",
            f"field name '{name}' is a reserved SQL keyword",
        )


def escape_like_pattern(pattern: str) -> str:
    """Escape special characters in a SQL LIKE pattern."""
    result = pattern.replace("\\", "\\\\")
    result = result.replace("%", "\\%")
    result = result.replace("_", "\\_")
    result = result.replace("'", "''")
    return result


def escape_json_field_name(field_name: str) -> str:
    """Escape a JSON field name for SQL."""
    return field_name.replace("'", "''")


def escape_string_literal(value: str) -> str:
    """Escape a string for use as a SQL string literal."""
    return value.replace("'", "''")


def validate_no_null_bytes(value: str, context: str = "string literals") -> None:
    """Reject strings containing null bytes."""
    if "\x00" in value:
        raise InvalidFieldNameError(
            f"{context} cannot contain null bytes",
            f"null byte found in {context}: {value!r}",
        )


# Nested quantifier patterns for ReDoS detection
# Only flag truly dangerous patterns: quantifier on group containing quantifier
_REDOS_NESTED_QUANTIFIER = re.compile(
    r"\([^)]*[+*?]\)[+*?]"  # e.g., (a+)+ or (a*)*
)
_REDOS_QUANTIFIED_ALTERNATION = re.compile(
    r"\([^)]*\|[^)]*\)[+*?]"
)


def convert_re2_to_posix(re2_pattern: str) -> tuple[str, bool]:
    """Convert an RE2 regex pattern to PostgreSQL POSIX ERE.

    Returns (posix_pattern, case_insensitive).
    """
    if len(re2_pattern) > MAX_REGEX_LENGTH:
        raise InvalidRegexPatternError(
            "regex pattern too long",
            f"pattern length {len(re2_pattern)} exceeds limit {MAX_REGEX_LENGTH}",
        )

    validate_no_null_bytes(re2_pattern, "regex patterns")

    case_insensitive = False
    pattern = re2_pattern

    # Extract (?i) flag
    if pattern.startswith("(?i)"):
        case_insensitive = True
        pattern = pattern[4:]

    # Reject unsupported features
    if re.search(r"\(\?[!=<]", pattern):
        raise InvalidRegexPatternError(
            "lookahead/lookbehind not supported",
            f"pattern contains lookahead/lookbehind: {re2_pattern}",
        )
    if re.search(r"\(\?P<", pattern):
        raise InvalidRegexPatternError(
            "named captures not supported",
            f"pattern contains named captures: {re2_pattern}",
        )
    # Reject inline flags other than (?i) at start
    if re.search(r"\(\?[imsx]", pattern):
        raise InvalidRegexPatternError(
            "inline flags not supported",
            f"pattern contains inline flags: {re2_pattern}",
        )

    # ReDoS detection
    if _REDOS_NESTED_QUANTIFIER.search(pattern):
        raise InvalidRegexPatternError(
            "potential ReDoS: nested quantifiers detected",
            f"pattern has nested quantifiers: {re2_pattern}",
        )

    # Check nesting depth
    depth = 0
    max_depth = 0
    for ch in pattern:
        if ch == "(":
            depth += 1
            max_depth = max(max_depth, depth)
        elif ch == ")":
            depth -= 1
    if max_depth > MAX_REGEX_NESTING:
        raise InvalidRegexPatternError(
            "regex nesting too deep",
            f"pattern nesting depth {max_depth} exceeds limit {MAX_REGEX_NESTING}",
        )

    # Count groups
    group_count = pattern.count("(") - pattern.count("(?:")
    if group_count > MAX_REGEX_GROUPS:
        raise InvalidRegexPatternError(
            "too many regex groups",
            f"pattern has {group_count} groups, limit is {MAX_REGEX_GROUPS}",
        )

    # Convert RE2 shorthand classes to POSIX
    pattern = pattern.replace("\\d", "[[:digit:]]")
    pattern = pattern.replace("\\D", "[^[:digit:]]")
    pattern = pattern.replace("\\w", "[[:alnum:]_]")
    pattern = pattern.replace("\\W", "[^[:alnum:]_]")
    pattern = pattern.replace("\\s", "[[:space:]]")
    pattern = pattern.replace("\\S", "[^[:space:]]")
    pattern = pattern.replace("\\b", "\\y")
    pattern = pattern.replace("\\B", "\\Y")

    # Convert non-capturing groups to regular groups
    pattern = pattern.replace("(?:", "(")

    return pattern, case_insensitive
