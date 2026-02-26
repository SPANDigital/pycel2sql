"""BigQuery dialect implementation."""

from __future__ import annotations

import re

from io import StringIO

from pycel2sql._errors import InvalidFieldNameError
from pycel2sql._utils import convert_re2_to_re2_native
from pycel2sql.dialect._base import Dialect, WriteFunc

# BigQuery reserved keywords
_BIGQUERY_RESERVED: set[str] = {
    "all", "alter", "and", "any", "array", "as", "asc", "assert_rows_modified",
    "at", "between", "by", "case", "cast", "collate", "contains", "create",
    "cross", "cube", "current", "default", "define", "desc", "distinct",
    "else", "end", "enum", "escape", "except", "exclude", "exists", "extract",
    "false", "fetch", "following", "for", "from", "full", "group", "grouping",
    "groups", "hash", "having", "if", "ignore", "in", "inner", "insert",
    "intersect", "interval", "into", "is", "join", "lateral", "left", "like",
    "limit", "lookup", "merge", "natural", "new", "no", "not", "null",
    "nulls", "of", "on", "or", "order", "outer", "over", "partition",
    "preceding", "proto", "range", "recursive", "respect", "right",
    "rollup", "rows", "select", "set", "some", "struct", "tablesample",
    "then", "to", "treat", "true", "unbounded", "union", "unnest", "using",
    "when", "where", "window", "with", "within",
}

_FIELD_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

# CEL type name -> BigQuery type name
_TYPE_MAP: dict[str, str] = {
    "bool": "BOOL",
    "bytes": "BYTES",
    "double": "FLOAT64",
    "int": "INT64",
    "uint": "INT64",
    "string": "STRING",
    "timestamp": "TIMESTAMP",
}

# For ARRAY<TYPE>[] empty typed arrays
_BQ_TYPE_NORMALIZE: dict[str, str] = {
    "text": "STRING",
    "string": "STRING",
    "varchar": "STRING",
    "int": "INT64",
    "integer": "INT64",
    "bigint": "INT64",
    "int64": "INT64",
    "double": "FLOAT64",
    "float": "FLOAT64",
    "real": "FLOAT64",
    "float64": "FLOAT64",
    "boolean": "BOOL",
    "bool": "BOOL",
    "bytes": "BYTES",
    "bytea": "BYTES",
    "blob": "BYTES",
}


class BigQueryDialect(Dialect):
    """BigQuery dialect for CEL-to-SQL conversion."""

    # --- Literals ---

    def write_string_literal(self, w: StringIO, value: str) -> None:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        w.write(f"'{escaped}'")

    def write_bytes_literal(self, w: StringIO, value: bytes) -> None:
        # BigQuery b"..." with octal encoding
        parts = []
        for byte in value:
            parts.append(f"\\{byte:03o}")
        w.write(f'b"{"".join(parts)}"')

    def write_param_placeholder(self, w: StringIO, param_index: int) -> None:
        w.write(f"@p{param_index}")

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
        w.write("REGEXP_CONTAINS(")
        write_target()
        w.write(", ")
        if case_insensitive:
            escaped = pattern.replace("\\", "\\\\").replace("'", "\\'")
            w.write(f"'(?i){escaped}'")
        else:
            escaped = pattern.replace("\\", "\\\\").replace("'", "\\'")
            w.write(f"'{escaped}'")
        w.write(")")

    def write_like_escape(self, w: StringIO) -> None:
        pass  # BigQuery uses backslash as default escape

    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        write_elem()
        w.write(" IN UNNEST(")
        write_array()
        w.write(")")

    # --- Type Casting ---

    def write_cast_to_numeric(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(")
        write_expr()
        w.write(" AS FLOAT64)")

    def write_type_name(self, w: StringIO, cel_type_name: str) -> None:
        sql_type = _TYPE_MAP.get(cel_type_name, cel_type_name.upper())
        w.write(sql_type)

    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("UNIX_SECONDS(")
        write_expr()
        w.write(")")

    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(")
        write_expr()
        w.write(" AS TIMESTAMP)")

    # --- Arrays ---

    def write_array_literal_open(self, w: StringIO) -> None:
        w.write("[")

    def write_array_literal_close(self, w: StringIO) -> None:
        w.write("]")

    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None:
        w.write("ARRAY_LENGTH(")
        write_expr()
        w.write(")")

    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None:
        write_array()
        w.write("[OFFSET(")
        write_index()
        w.write(")]")

    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None:
        write_array()
        w.write(f"[OFFSET({index})]")

    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None:
        bq_type = _BQ_TYPE_NORMALIZE.get(type_name.lower(), type_name.upper())
        w.write(f"ARRAY<{bq_type}>[]")

    # --- JSON ---

    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None:
        escaped = field_name.replace("\\", "\\\\").replace("'", "\\'")
        if is_final:
            w.write("JSON_VALUE(")
            write_base()
            w.write(f", '$.{escaped}')")
        else:
            w.write("JSON_QUERY(")
            write_base()
            w.write(f", '$.{escaped}')")

    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None:
        escaped = field_name.replace("\\", "\\\\").replace("'", "\\'")
        w.write("JSON_VALUE(")
        write_base()
        w.write(f", '$.{escaped}') IS NOT NULL")

    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None:
        w.write("UNNEST(JSON_QUERY_ARRAY(")
        write_expr()
        w.write("))")

    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("ARRAY_LENGTH(JSON_QUERY_ARRAY(")
        write_expr()
        w.write("))")

    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_expr: WriteFunc
    ) -> None:
        w.write("UNNEST(JSON_VALUE_ARRAY(")
        write_expr()
        w.write("))")

    def write_nested_json_array_membership(
        self, w: StringIO, write_expr: WriteFunc
    ) -> None:
        w.write("UNNEST(JSON_VALUE_ARRAY(")
        write_expr()
        w.write("))")

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
        if part == "DOW":
            part = "DAYOFWEEK"
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
        if op == "+":
            w.write("TIMESTAMP_ADD(")
            write_ts()
            w.write(", ")
            write_dur()
            w.write(")")
        else:
            w.write("TIMESTAMP_SUB(")
            write_ts()
            w.write(", ")
            write_dur()
            w.write(")")

    # --- String Functions ---

    def write_contains(
        self, w: StringIO, write_haystack: WriteFunc, write_needle: WriteFunc
    ) -> None:
        w.write("STRPOS(")
        write_haystack()
        w.write(", ")
        write_needle()
        w.write(") > 0")

    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("SPLIT(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(")")

    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None:
        # BigQuery doesn't have a native split with limit; use subquery approach
        w.write("ARRAY(SELECT x FROM UNNEST(SPLIT(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(f")) AS x WITH OFFSET WHERE OFFSET < {limit})")

    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("ARRAY_TO_STRING(")
        write_array()
        w.write(", ")
        write_delim()
        w.write(")")

    # --- Comprehensions ---

    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None:
        w.write("UNNEST(")
        write_source()
        w.write(")")

    def write_array_subquery_open(self, w: StringIO) -> None:
        w.write("ARRAY(SELECT ")

    def write_array_subquery_expr_close(self, w: StringIO) -> None:
        pass  # No-op for BigQuery

    # --- Regex ---

    def convert_regex(self, re2_pattern: str) -> tuple[str, bool]:
        return convert_re2_to_re2_native(re2_pattern)

    # --- Struct ---

    def write_struct_open(self, w: StringIO) -> None:
        w.write("STRUCT(")

    def write_struct_close(self, w: StringIO) -> None:
        w.write(")")

    # --- Validation ---

    def max_identifier_length(self) -> int:
        return 300

    def validate_field_name(self, name: str) -> None:
        if not name:
            raise InvalidFieldNameError(
                "field name cannot be empty",
                "empty field name provided",
            )
        if len(name) > 300:
            raise InvalidFieldNameError(
                "field name too long",
                f"field name '{name}' exceeds 300 characters",
            )
        if not _FIELD_NAME_RE.match(name):
            raise InvalidFieldNameError(
                "invalid field name format",
                f"field name '{name}' contains invalid characters",
            )
        if name.lower() in _BIGQUERY_RESERVED:
            raise InvalidFieldNameError(
                "field name is a reserved SQL keyword",
                f"field name '{name}' is a reserved BigQuery keyword",
            )

    # --- Capabilities ---

    def supports_native_arrays(self) -> bool:
        return True

    def supports_jsonb(self) -> bool:
        return False
