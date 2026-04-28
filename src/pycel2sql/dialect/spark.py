"""Apache Spark SQL dialect implementation.

Ported from cel2sql Go (PR #117) and cel2sql4j (PR #10). Spark runs on the JVM
and uses ``java.util.regex.Pattern``, so the regex translator is mostly a
passthrough. Spark has no separate JSONB type — JSON fields are accessed via
``get_json_object``; arrays use the native ``ARRAY<T>`` type with
``array_contains`` / ``size`` / ``EXPLODE``.

Spark indexing is storage-layer-specific (Delta Z-order vs Iceberg sort vs
plain Parquet) and not portable as a single set of SQL recommendations. For
this reason ``SparkDialect`` deliberately does NOT implement ``IndexAdvisor``;
``get_index_advisor()`` returns None for Spark, and ``analyze()`` produces an
empty recommendation list.
"""

from __future__ import annotations

import re
from io import StringIO

from pycel2sql._errors import (
    InvalidFieldNameError,
    InvalidRegexPatternError,
    UnsupportedDialectFeatureError,
)
from pycel2sql.dialect._base import Dialect, WriteFunc

# Spark / Hive identifier limit.
_MAX_IDENTIFIER_LENGTH = 128

# Apache Spark SQL reserved keywords (lowercased). Sourced from the Apache
# Spark docs (sql-ref-ansi-compliance.html#sql-keywords) plus the standard SQL
# set.
_SPARK_RESERVED: set[str] = {
    "all", "alter", "and", "anti", "any", "array", "as", "asc", "between",
    "both", "by", "case", "cast", "check", "cluster", "collate", "column",
    "create", "cross", "cube", "current", "current_date", "current_time",
    "current_timestamp", "current_user", "default", "delete", "desc",
    "describe", "distinct", "drop", "else", "end", "escape", "except",
    "exists", "false", "fetch", "filter", "for", "foreign", "from", "full",
    "function", "grant", "group", "grouping", "having", "hour", "in", "inner",
    "insert", "intersect", "interval", "into", "is", "join", "lateral",
    "leading", "left", "like", "limit", "local", "map", "minute", "month",
    "natural", "no", "not", "null", "of", "on", "only", "or", "order",
    "outer", "overlaps", "primary", "references", "right", "rollup", "row",
    "rows", "second", "select", "semi", "session_user", "set", "some",
    "struct", "table", "tablesample", "then", "time", "to", "trailing",
    "true", "union", "unique", "unknown", "update", "user", "using", "values",
    "when", "where", "window", "with", "year",
}

_FIELD_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# CEL type name -> Spark SQL type name.
_TYPE_MAP: dict[str, str] = {
    "bool": "BOOLEAN",
    "bytes": "BINARY",
    "double": "DOUBLE",
    "int": "BIGINT",
    "uint": "BIGINT",
    "string": "STRING",
}

# Lowercased element-type aliases for write_empty_typed_array.
_SPARK_ELEMENT_TYPE_MAP: dict[str, str] = {
    "text": "STRING",
    "string": "STRING",
    "varchar": "STRING",
    "char": "STRING",
    "int": "BIGINT",
    "integer": "BIGINT",
    "bigint": "BIGINT",
    "int64": "BIGINT",
    "long": "BIGINT",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "real": "DOUBLE",
    "float64": "DOUBLE",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "bytes": "BINARY",
    "bytea": "BINARY",
    "blob": "BINARY",
    "binary": "BINARY",
}

# Regex limits — same numbers as DuckDB / Postgres regex helpers.
_MAX_PATTERN_LENGTH = 500
_MAX_GROUPS = 20
_MAX_NESTING_DEPTH = 10

_NESTED_QUANTIFIERS_RE = re.compile(r"[*+][*+]")
_QUANTIFIED_ALTERNATION_RE = re.compile(r"\([^)]*\|[^)]*\)[*+]")
# Captures the flag-letter set inside a `(?<flags>)` or `(?<flags>:` group.
_INLINE_FLAG_GROUP_RE = re.compile(r"\(\?([a-zA-Z]+)[:)]")


def _spark_element_type(type_name: str) -> str:
    return _SPARK_ELEMENT_TYPE_MAP.get(type_name.lower(), type_name.upper())


def _validate_spark_field_name(name: str) -> None:
    if not name:
        raise InvalidFieldNameError(
            "field name cannot be empty",
            "empty field name provided",
        )
    if len(name) > _MAX_IDENTIFIER_LENGTH:
        raise InvalidFieldNameError(
            "field name exceeds Spark identifier length limit",
            f"field name length {len(name)} exceeds Spark limit of "
            f"{_MAX_IDENTIFIER_LENGTH}",
        )
    if not _FIELD_NAME_RE.match(name):
        raise InvalidFieldNameError(
            "invalid field name format",
            f"field name '{name}' must start with a letter or underscore and "
            "contain only alphanumeric characters and underscores",
        )
    if name.lower() in _SPARK_RESERVED:
        raise InvalidFieldNameError(
            "field name is a reserved SQL keyword",
            f"field name '{name}' is a reserved Spark SQL keyword",
        )


def _count_unescaped_parens(pattern: str) -> int:
    count = 0
    for i, ch in enumerate(pattern):
        if ch == "(" and (i == 0 or pattern[i - 1] != "\\"):
            count += 1
    return count


def _max_nesting_depth(pattern: str) -> int:
    max_depth = 0
    current = 0
    for i, ch in enumerate(pattern):
        if ch == "(" and (i == 0 or pattern[i - 1] != "\\"):
            current += 1
            if current > max_depth:
                max_depth = current
        elif ch == ")" and (i == 0 or pattern[i - 1] != "\\"):
            current -= 1
    return max_depth


def _validate_no_nested_quantifiers(pattern: str) -> None:
    """Raise InvalidRegexPatternError if a quantifier is nested inside another
    quantified group (potential ReDoS).
    """
    stack: list[bool] = []  # True if the group has seen a quantifier
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if i > 0 and pattern[i - 1] == "\\":
            i += 1
            continue
        if ch == "(":
            stack.append(False)
        elif ch == ")":
            if not stack:
                i += 1
                continue
            inner_quantified = stack.pop()
            next_ch = pattern[i + 1] if i + 1 < len(pattern) else ""
            if next_ch in ("*", "+", "?", "{") and inner_quantified:
                raise InvalidRegexPatternError(
                    "invalid regex pattern",
                    "regex contains catastrophic nested quantifiers that "
                    "could cause ReDoS",
                )
            if stack and inner_quantified:
                stack[-1] = True
        elif ch in ("*", "+", "?", "{"):
            if stack:
                stack[-1] = True
        i += 1


def _convert_re2_to_spark(pattern: str) -> tuple[str, bool]:
    """Validate an RE2-style regex pattern and pass it through to Spark.

    Spark uses java.util.regex which handles inline ``(?i)`` natively, so the
    returned ``case_insensitive`` flag is always False — the pattern is emitted
    verbatim and the engine honours any inline flag.
    """
    if len(pattern) > _MAX_PATTERN_LENGTH:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            f"pattern length {len(pattern)} exceeds limit of "
            f"{_MAX_PATTERN_LENGTH} characters",
        )
    try:
        re.compile(pattern)
    except re.error as e:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            f"regex pattern does not compile: {e}",
        ) from e
    if "(?=" in pattern or "(?!" in pattern:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "lookahead assertions (?=...), (?!...) are not supported in Spark regex",
        )
    if "(?<=" in pattern or "(?<!" in pattern:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "lookbehind assertions (?<=...), (?<!...) are not supported in Spark regex",
        )
    if "(?P<" in pattern:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "named capture groups (?P<name>...) are not supported in Spark regex",
        )
    if _NESTED_QUANTIFIERS_RE.search(pattern):
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "regex contains catastrophic nested quantifiers that could cause ReDoS",
        )
    _validate_no_nested_quantifiers(pattern)
    if _count_unescaped_parens(pattern) > _MAX_GROUPS:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            f"regex contains more than {_MAX_GROUPS} capture groups",
        )
    if _QUANTIFIED_ALTERNATION_RE.search(pattern):
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "regex contains quantified alternation that could cause ReDoS",
        )
    if _max_nesting_depth(pattern) > _MAX_NESTING_DEPTH:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            f"nesting depth exceeds limit of {_MAX_NESTING_DEPTH}",
        )
    # Reject any inline flag group whose flag-letter set contains anything
    # other than `i`. This catches combined groups like `(?im)` / `(?mi)` /
    # `(?ix:...)` that the previous substring-only check missed. The `(?-...)`
    # flag-clear form is rejected too because the regex matches its leading
    # `(?` followed by letters; a literal `(?-i)` (no letters before `-`) is
    # caught by the explicit `(?-` substring check.
    for match in _INLINE_FLAG_GROUP_RE.finditer(pattern):
        flags = match.group(1)
        if any(f != "i" for f in flags):
            raise InvalidRegexPatternError(
                "invalid pattern in expression",
                "inline flags other than (?i) are not supported in Spark regex",
            )
    if "(?-" in pattern:
        raise InvalidRegexPatternError(
            "invalid pattern in expression",
            "inline flags other than (?i) are not supported in Spark regex",
        )
    return pattern, False


class SparkDialect(Dialect):
    """Apache Spark SQL dialect for CEL-to-SQL conversion."""

    # --- Literals ---

    def write_string_literal(self, w: StringIO, value: str) -> None:
        escaped = value.replace("'", "''")
        w.write(f"'{escaped}'")

    def write_bytes_literal(self, w: StringIO, value: bytes) -> None:
        hex_str = value.hex().upper()
        w.write(f"X'{hex_str}'")

    def write_param_placeholder(self, w: StringIO, param_index: int) -> None:
        # Spark JDBC uses positional ? placeholders.
        w.write("?")

    # --- Operators ---

    def write_string_concat(
        self, w: StringIO, write_lhs: WriteFunc, write_rhs: WriteFunc
    ) -> None:
        # concat() works in all Spark versions; the || operator was added in 3.0+.
        w.write("concat(")
        write_lhs()
        w.write(", ")
        write_rhs()
        w.write(")")

    def write_regex_match(
        self, w: StringIO, write_target: WriteFunc, pattern: str, case_insensitive: bool
    ) -> None:
        # Spark regex uses Java pattern syntax; (?i) inline flag is honoured by
        # the engine, so the case_insensitive flag is always False here (folded
        # into the pattern by _convert_re2_to_spark).
        write_target()
        escaped = pattern.replace("'", "''")
        w.write(f" RLIKE '{escaped}'")

    def write_like_escape(self, w: StringIO) -> None:
        w.write(" ESCAPE '\\\\'")

    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        w.write("array_contains(")
        write_array()
        w.write(", ")
        write_elem()
        w.write(")")

    # --- Type Casting ---

    def write_cast_to_numeric(self, w: StringIO, write_expr: WriteFunc) -> None:
        # Spark has no postfix `::TYPE` cast; arithmetic coercion `+ 0` works
        # (same trick MySQL/SQLite use), forcing string→number coercion.
        write_expr()
        w.write(" + 0")

    def write_type_name(self, w: StringIO, cel_type_name: str) -> None:
        sql_type = _TYPE_MAP.get(cel_type_name, cel_type_name.upper())
        w.write(sql_type)

    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("UNIX_TIMESTAMP(")
        write_expr()
        w.write(")")

    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(")
        write_expr()
        w.write(" AS TIMESTAMP)")

    # --- Arrays ---

    def write_array_literal_open(self, w: StringIO) -> None:
        w.write("array(")

    def write_array_literal_close(self, w: StringIO) -> None:
        w.write(")")

    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None:
        if dimension > 1:
            raise UnsupportedDialectFeatureError(
                "multi-dimensional array length is not supported in Spark",
                f"Spark dialect does not support multi-dimensional array "
                f"length (dimension={dimension})",
            )
        # In Spark SQL, size(NULL) evaluates to NULL; COALESCE converts that to 0.
        w.write("COALESCE(size(")
        write_expr()
        w.write("), 0)")

    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None:
        # Spark arrays are 0-indexed (Java/Scala convention).
        write_array()
        w.write("[")
        write_index()
        w.write("]")

    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None:
        write_array()
        w.write(f"[{index}]")

    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None:
        w.write(f"CAST(array() AS ARRAY<{_spark_element_type(type_name)}>)")

    # --- JSON ---

    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None:
        # Spark's get_json_object always returns a string; the same function is
        # used for both intermediate and final access (no JSON_QUERY equivalent).
        escaped = field_name.replace("'", "''")
        w.write("get_json_object(")
        write_base()
        w.write(f", '$.{escaped}')")

    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None:
        escaped = field_name.replace("'", "''")
        w.write("get_json_object(")
        write_base()
        w.write(f", '$.{escaped}') IS NOT NULL")

    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None:
        # Element type is fixed to STRING; numeric comparisons coerce via
        # write_cast_to_numeric.
        w.write("EXPLODE(from_json(")
        write_expr()
        w.write(", 'ARRAY<STRING>'))")

    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("COALESCE(size(from_json(")
        write_expr()
        w.write(", 'ARRAY<STRING>')), 0)")

    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_expr: WriteFunc
    ) -> None:
        # The converter emits `lhs = <subquery>` for this construct, and a
        # scalar subquery built from EXPLODE(from_json(...)) can return
        # multiple rows — Spark rejects that at runtime. The dialect contract
        # here does not provide the candidate element, so we cannot rewrite
        # to a boolean predicate (e.g. array_contains(from_json(...), elem)).
        # Failing fast at conversion time is preferable to emitting SQL that
        # fails at execution.
        raise UnsupportedDialectFeatureError(
            "JSON array membership is not supported in Spark",
            "Spark JSON array membership requires a boolean predicate "
            "(array_contains/EXISTS); the dialect contract does not provide "
            "the candidate element to build one. Use a typed ARRAY<T> column "
            "or rewrite the expression in application code.",
        )

    def write_nested_json_array_membership(
        self, w: StringIO, write_expr: WriteFunc
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "nested JSON array membership is not supported in Spark",
            "Spark nested JSON array membership requires a boolean predicate "
            "(array_contains/EXISTS); the dialect contract does not provide "
            "the candidate element to build one. Use a typed ARRAY<T> column "
            "or rewrite the expression in application code.",
        )

    # --- Timestamps ---

    def write_duration(self, w: StringIO, value: int, unit: str) -> None:
        w.write(f"INTERVAL {value} {unit}")

    def write_interval(
        self, w: StringIO, write_value: WriteFunc, unit: str
    ) -> None:
        w.write("INTERVAL ")
        write_value()
        w.write(f" {unit}")

    def write_extract(
        self,
        w: StringIO,
        part: str,
        write_expr: WriteFunc,
        write_tz: WriteFunc | None,
    ) -> None:
        # Spark dayofweek() returns 1=Sunday..7=Saturday; CEL convention is
        # 0=Sunday..6=Saturday. Adjust by subtracting 1.
        if part == "DOW":
            w.write("(dayofweek(")
            write_expr()
            if write_tz is not None:
                w.write(" AT TIME ZONE ")
                write_tz()
            w.write(") - 1)")
            return
        w.write(f"EXTRACT({part} FROM ")
        write_expr()
        if write_tz is not None:
            w.write(" AT TIME ZONE ")
            write_tz()
        w.write(")")

    def write_timestamp_arithmetic(
        self,
        w: StringIO,
        op: str,
        write_ts: WriteFunc,
        write_dur: WriteFunc,
    ) -> None:
        write_ts()
        w.write(f" {op} ")
        write_dur()

    # --- String Functions ---

    def write_contains(
        self, w: StringIO, write_haystack: WriteFunc, write_needle: WriteFunc
    ) -> None:
        # LOCATE(substr, str) returns 1-based position or 0 when not found.
        w.write("LOCATE(")
        write_needle()
        w.write(", ")
        write_haystack()
        w.write(") > 0")

    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("split(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(")")

    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None:
        # Spark 3.x+ supports the 3-arg split.
        w.write("split(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(f", {limit})")

    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("array_join(")
        write_array()
        w.write(", ")
        write_delim()
        w.write(")")

    def write_format(
        self, w: StringIO, fmt_string: str, write_args: list[WriteFunc]
    ) -> None:
        # Spark's format_string() is its printf-equivalent (supports %s/%d/%f
        # directly).
        w.write("format_string(")
        self.write_string_literal(w, fmt_string)
        for arg in write_args:
            w.write(", ")
            arg()
        w.write(")")

    # --- Comprehensions ---

    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None:
        w.write("EXPLODE(")
        write_source()
        w.write(")")

    def write_array_subquery_open(self, w: StringIO) -> None:
        # Spark has no ARRAY(SELECT ...) constructor; collect_list() is the
        # closest equivalent.
        w.write("(SELECT collect_list(")

    def write_array_subquery_expr_close(self, w: StringIO) -> None:
        w.write(")")

    # --- Regex ---

    def convert_regex(self, re2_pattern: str) -> tuple[str, bool]:
        return _convert_re2_to_spark(re2_pattern)

    # --- Struct ---

    def write_struct_open(self, w: StringIO) -> None:
        w.write("struct(")

    def write_struct_close(self, w: StringIO) -> None:
        w.write(")")

    # --- Validation ---

    def max_identifier_length(self) -> int:
        return _MAX_IDENTIFIER_LENGTH

    def validate_field_name(self, name: str) -> None:
        _validate_spark_field_name(name)

    # --- Capabilities ---

    def supports_native_arrays(self) -> bool:
        return True

    def supports_jsonb(self) -> bool:
        return False
