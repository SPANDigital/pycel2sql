"""PostgreSQL dialect implementation."""

from __future__ import annotations

from io import StringIO

from pycel2sql._utils import validate_field_name
from pycel2sql.dialect._base import Dialect, WriteFunc

# CEL type name -> PostgreSQL type name
_TYPE_MAP: dict[str, str] = {
    "bool": "BOOLEAN",
    "bytes": "BYTEA",
    "double": "DOUBLE PRECISION",
    "int": "BIGINT",
    "uint": "BIGINT",
    "string": "TEXT",
    "timestamp": "TIMESTAMP WITH TIME ZONE",
}


class PostgresDialect(Dialect):
    """PostgreSQL dialect for CEL-to-SQL conversion."""

    # --- Literals ---

    def write_string_literal(self, w: StringIO, value: str) -> None:
        escaped = value.replace("'", "''")
        w.write(f"'{escaped}'")

    def write_bytes_literal(self, w: StringIO, value: bytes) -> None:
        hex_str = value.hex().upper()
        w.write(f"'\\x{hex_str}'")

    def write_param_placeholder(self, w: StringIO, param_index: int) -> None:
        w.write(f"${param_index}")

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
        write_target()
        if case_insensitive:
            w.write(" ~* ")
        else:
            w.write(" ~ ")
        escaped = pattern.replace("'", "''")
        w.write(f"'{escaped}'")

    def write_like_escape(self, w: StringIO) -> None:
        w.write(" ESCAPE E'\\\\'")

    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        write_elem()
        w.write(" = ANY(")
        write_array()
        w.write(")")

    # --- Type Casting ---

    def write_cast_to_numeric(self, w: StringIO) -> None:
        w.write("::numeric")

    def write_type_name(self, w: StringIO, cel_type_name: str) -> None:
        sql_type = _TYPE_MAP.get(cel_type_name, cel_type_name.upper())
        w.write(sql_type)

    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("EXTRACT(EPOCH FROM ")
        write_expr()
        w.write(")::bigint")

    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(")
        write_expr()
        w.write(" AS TIMESTAMP WITH TIME ZONE)")

    # --- Arrays ---

    def write_array_literal_open(self, w: StringIO) -> None:
        w.write("ARRAY[")

    def write_array_literal_close(self, w: StringIO) -> None:
        w.write("]")

    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None:
        w.write("COALESCE(ARRAY_LENGTH(")
        write_expr()
        w.write(f", {dimension}), 0)")

    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None:
        write_array()
        w.write("[")
        write_index()
        w.write(" + 1]")

    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None:
        write_array()
        # Convert 0-based to 1-based
        w.write(f"[{index + 1}]")

    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None:
        w.write(f"ARRAY[]::{type_name}[]")

    # --- JSON ---

    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None:
        write_base()
        escaped = field_name.replace("'", "''")
        if is_final:
            w.write(f"->>'{escaped}'")
        else:
            w.write(f"->'{escaped}'")

    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None:
        escaped = field_name.replace("'", "''")
        if is_jsonb:
            write_base()
            w.write(f" ? '{escaped}'")
        else:
            write_base()
            w.write(f"->'{escaped}' IS NOT NULL")

    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None:
        if is_jsonb:
            func = "jsonb_array_elements_text" if as_text else "jsonb_array_elements"
        else:
            func = "json_array_elements_text" if as_text else "json_array_elements"
        w.write(f"{func}(")
        write_expr()
        w.write(")")

    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("COALESCE(jsonb_array_length(")
        write_expr()
        w.write("), 0)")

    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_expr: WriteFunc
    ) -> None:
        w.write(f"ANY(ARRAY(SELECT {json_func}(")
        write_expr()
        w.write(")))")

    def write_nested_json_array_membership(
        self, w: StringIO, write_expr: WriteFunc
    ) -> None:
        w.write("ANY(ARRAY(SELECT jsonb_array_elements_text(")
        write_expr()
        w.write(")))")

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
        w.write("POSITION(")
        write_needle()
        w.write(" IN ")
        write_haystack()
        w.write(") > 0")

    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("STRING_TO_ARRAY(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(")")

    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None:
        w.write("(STRING_TO_ARRAY(")
        write_str()
        w.write(", ")
        write_delim()
        w.write(f"))[1:{limit}]")

    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("ARRAY_TO_STRING(")
        write_array()
        w.write(", ")
        write_delim()
        w.write(", '')")

    # --- Comprehensions ---

    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None:
        w.write("UNNEST(")
        write_source()
        w.write(")")

    def write_array_subquery_open(self, w: StringIO) -> None:
        w.write("ARRAY(SELECT ")

    # --- Struct ---

    def write_struct_open(self, w: StringIO) -> None:
        w.write("ROW(")

    def write_struct_close(self, w: StringIO) -> None:
        w.write(")")

    # --- Validation ---

    def max_identifier_length(self) -> int:
        return 63

    def validate_field_name(self, name: str) -> None:
        validate_field_name(name)

    # --- Capabilities ---

    def supports_native_arrays(self) -> bool:
        return True

    def supports_jsonb(self) -> bool:
        return True
