"""SQLite dialect implementation."""

from __future__ import annotations

import re

from io import StringIO

from pycel2sql._errors import InvalidFieldNameError, UnsupportedDialectFeatureError
from pycel2sql.dialect._base import Dialect, WriteFunc

# SQLite reserved keywords
_SQLITE_RESERVED: set[str] = {
    "abort", "action", "add", "after", "all", "alter", "always", "analyze",
    "and", "as", "asc", "attach", "autoincrement", "before", "begin",
    "between", "by", "cascade", "case", "cast", "check", "collate",
    "column", "commit", "conflict", "constraint", "create", "cross",
    "current", "current_date", "current_time", "current_timestamp",
    "database", "default", "deferrable", "deferred", "delete", "desc",
    "detach", "distinct", "do", "drop", "each", "else", "end", "escape",
    "except", "exclude", "exclusive", "exists", "explain", "fail",
    "filter", "first", "following", "for", "foreign", "from", "full",
    "generated", "glob", "group", "groups", "having", "if", "ignore",
    "immediate", "in", "index", "indexed", "initially", "inner", "insert",
    "instead", "intersect", "into", "is", "isnull", "join", "key",
    "last", "left", "like", "limit", "match", "materialized", "natural",
    "no", "not", "nothing", "notnull", "null", "nulls", "of", "offset",
    "on", "or", "order", "others", "outer", "over", "partition", "plan",
    "pragma", "preceding", "primary", "query", "raise", "range",
    "recursive", "references", "regexp", "reindex", "release", "rename",
    "replace", "restrict", "returning", "right", "rollback", "row",
    "rows", "savepoint", "select", "set", "table", "temp", "temporary",
    "then", "ties", "to", "transaction", "trigger", "true", "unbounded",
    "union", "unique", "update", "using", "vacuum", "values", "view",
    "virtual", "when", "where", "window", "with", "without",
}

_FIELD_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# CEL type name -> SQLite type name
_TYPE_MAP: dict[str, str] = {
    "bool": "INTEGER",
    "bytes": "BLOB",
    "double": "REAL",
    "int": "INTEGER",
    "uint": "INTEGER",
    "string": "TEXT",
    "timestamp": "TEXT",
}

# strftime format map for EXTRACT-style operations
_STRFTIME_MAP: dict[str, str] = {
    "YEAR": "%Y",
    "MONTH": "%m",
    "DAY": "%d",
    "HOUR": "%H",
    "MINUTE": "%M",
    "SECOND": "%S",
    "DOY": "%j",
    "DOW": "%w",
    "MILLISECONDS": "%f",
}


class SQLiteDialect(Dialect):
    """SQLite dialect for CEL-to-SQL conversion."""

    # --- Literals ---

    def write_string_literal(self, w: StringIO, value: str) -> None:
        escaped = value.replace("'", "''")
        w.write(f"'{escaped}'")

    def write_bytes_literal(self, w: StringIO, value: bytes) -> None:
        hex_str = value.hex().upper()
        w.write(f"X'{hex_str}'")

    def write_param_placeholder(self, w: StringIO, param_index: int) -> None:
        w.write("?")

    # --- Operators ---

    def write_string_concat(
        self, w: StringIO, write_lhs: WriteFunc, write_rhs: WriteFunc
    ) -> None:
        write_lhs()
        w.write(" || ")
        write_rhs()

    def write_regex_match(
        self, w: StringIO, write_target: WriteFunc, pattern: str, case_insensitive: bool
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "regex not supported by SQLite dialect",
            "SQLite does not have built-in regex support",
        )

    def write_like_escape(self, w: StringIO) -> None:
        w.write(" ESCAPE '\\'")

    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        write_elem()
        w.write(" IN (SELECT value FROM json_each(")
        write_array()
        w.write("))")

    # --- Type Casting ---

    def write_cast_to_numeric(self, w: StringIO, write_expr: WriteFunc) -> None:
        write_expr()
        w.write(" + 0")

    def write_type_name(self, w: StringIO, cel_type_name: str) -> None:
        sql_type = _TYPE_MAP.get(cel_type_name, cel_type_name.upper())
        w.write(sql_type)

    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(strftime('%s', ")
        write_expr()
        w.write(") AS INTEGER)")

    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("datetime(")
        write_expr()
        w.write(")")

    # --- Arrays ---

    def write_array_literal_open(self, w: StringIO) -> None:
        w.write("json_array(")

    def write_array_literal_close(self, w: StringIO) -> None:
        w.write(")")

    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None:
        w.write("COALESCE(json_array_length(")
        write_expr()
        w.write("), 0)")

    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None:
        w.write("json_extract(")
        write_array()
        w.write(", '$[' || ")
        write_index()
        w.write(" || ']')")

    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None:
        w.write("json_extract(")
        write_array()
        w.write(f", '$[{index}]')")

    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None:
        w.write("json_array()")

    # --- JSON ---

    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None:
        escaped = field_name.replace("'", "''")
        w.write("json_extract(")
        write_base()
        w.write(f", '$.{escaped}')")

    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None:
        escaped = field_name.replace("'", "''")
        w.write("json_type(")
        write_base()
        w.write(f", '$.{escaped}') IS NOT NULL")

    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None:
        w.write("json_each(")
        write_expr()
        w.write(")")

    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("COALESCE(json_array_length(")
        write_expr()
        w.write("), 0)")

    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_expr: WriteFunc
    ) -> None:
        w.write("(SELECT value FROM json_each(")
        write_expr()
        w.write("))")

    def write_nested_json_array_membership(
        self, w: StringIO, write_expr: WriteFunc
    ) -> None:
        w.write("(SELECT value FROM json_each(")
        write_expr()
        w.write("))")

    # --- Timestamps ---

    def write_duration(self, w: StringIO, value: int, unit: str) -> None:
        # SQLite uses modifier strings for datetime()
        unit_lower = unit.lower()
        # Normalize singular
        if unit_lower.endswith("s"):
            unit_lower = unit_lower[:-1]
        if unit_lower == "millisecond":
            # Convert to fractional seconds
            secs = value / 1000.0
            w.write(f"'+{secs} seconds'")
        elif unit_lower == "microsecond":
            secs = value / 1_000_000.0
            w.write(f"'+{secs} seconds'")
        elif unit_lower == "nanosecond":
            secs = value / 1_000_000_000.0
            w.write(f"'+{secs} seconds'")
        else:
            w.write(f"'+{value} {unit_lower}s'")

    def write_interval(
        self, w: StringIO, write_value: WriteFunc, unit: str
    ) -> None:
        unit_lower = unit.lower()
        if unit_lower.endswith("s"):
            unit_lower = unit_lower[:-1]
        w.write("'+' || ")
        write_value()
        w.write(f" || ' {unit_lower}s'")

    def write_extract(
        self,
        w: StringIO,
        part: str,
        write_expr: WriteFunc,
        write_tz: WriteFunc | None,
    ) -> None:
        fmt = _STRFTIME_MAP.get(part)
        if fmt is None:
            w.write(f"EXTRACT({part} FROM ")
            write_expr()
            w.write(")")
            return
        w.write(f"CAST(strftime('{fmt}', ")
        write_expr()
        w.write(") AS INTEGER)")

    def write_timestamp_arithmetic(
        self,
        w: StringIO,
        op: str,
        write_ts: WriteFunc,
        write_dur: WriteFunc,
    ) -> None:
        w.write("datetime(")
        write_ts()
        w.write(", ")
        if op == "-":
            # Negate the modifier: replace '+' with '-'
            w.write("REPLACE(")
            write_dur()
            w.write(", '+', '-')")
        else:
            write_dur()
        w.write(")")

    # --- String Functions ---

    def write_contains(
        self, w: StringIO, write_haystack: WriteFunc, write_needle: WriteFunc
    ) -> None:
        w.write("INSTR(")
        write_haystack()
        w.write(", ")
        write_needle()
        w.write(") > 0")

    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "split not supported by SQLite dialect",
            "SQLite does not have a native string split function",
        )

    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "split not supported by SQLite dialect",
            "SQLite does not have a native string split function",
        )

    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "join not supported by SQLite dialect",
            "SQLite does not have a native array join function",
        )

    # --- Comprehensions ---

    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None:
        w.write("json_each(")
        write_source()
        w.write(")")

    def write_array_subquery_open(self, w: StringIO) -> None:
        w.write("(SELECT json_group_array(")

    def write_array_subquery_expr_close(self, w: StringIO) -> None:
        w.write(")")  # Close json_group_array(

    # --- Regex ---

    def convert_regex(self, re2_pattern: str) -> tuple[str, bool]:
        raise UnsupportedDialectFeatureError(
            "regex not supported by SQLite dialect",
            "SQLite does not have built-in regex support",
        )

    # --- Struct ---

    def write_struct_open(self, w: StringIO) -> None:
        w.write("json_object(")

    def write_struct_close(self, w: StringIO) -> None:
        w.write(")")

    # --- Validation ---

    def max_identifier_length(self) -> int:
        return 0  # No limit

    def validate_field_name(self, name: str) -> None:
        if not name:
            raise InvalidFieldNameError(
                "field name cannot be empty",
                "empty field name provided",
            )
        if not _FIELD_NAME_RE.match(name):
            raise InvalidFieldNameError(
                "invalid field name format",
                f"field name '{name}' contains invalid characters",
            )
        if name.lower() in _SQLITE_RESERVED:
            raise InvalidFieldNameError(
                "field name is a reserved SQL keyword",
                f"field name '{name}' is a reserved SQLite keyword",
            )

    # --- Capabilities ---

    def supports_native_arrays(self) -> bool:
        return False

    def supports_jsonb(self) -> bool:
        return False
