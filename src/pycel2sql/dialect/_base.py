"""Abstract base class for SQL dialects."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from collections.abc import Callable
from io import StringIO


class DialectName(enum.StrEnum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"


WriteFunc = Callable[[], None]
"""Callback that writes a sub-expression to the shared StringIO buffer."""


class Dialect(ABC):
    """Abstract base class defining the SQL dialect interface.

    All SQL-syntax-specific code lives behind this interface.
    Methods receive a StringIO writer and callback functions for sub-expressions.
    """

    # --- Literals ---

    @abstractmethod
    def write_string_literal(self, w: StringIO, value: str) -> None: ...

    @abstractmethod
    def write_bytes_literal(self, w: StringIO, value: bytes) -> None: ...

    @abstractmethod
    def write_param_placeholder(self, w: StringIO, param_index: int) -> None: ...

    # --- Operators ---

    @abstractmethod
    def write_string_concat(
        self, w: StringIO, write_lhs: WriteFunc, write_rhs: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_regex_match(
        self, w: StringIO, write_target: WriteFunc, pattern: str, case_insensitive: bool
    ) -> None: ...

    @abstractmethod
    def write_like_escape(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None: ...

    # --- Type Casting ---

    @abstractmethod
    def write_cast_to_numeric(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_type_name(self, w: StringIO, cel_type_name: str) -> None: ...

    @abstractmethod
    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None: ...

    @abstractmethod
    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None: ...

    # --- Arrays ---

    @abstractmethod
    def write_array_literal_open(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_array_literal_close(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None: ...

    @abstractmethod
    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None: ...

    # --- JSON ---

    @abstractmethod
    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None: ...

    @abstractmethod
    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None: ...

    @abstractmethod
    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_expr: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_nested_json_array_membership(
        self, w: StringIO, write_expr: WriteFunc
    ) -> None: ...

    # --- Timestamps ---

    @abstractmethod
    def write_duration(self, w: StringIO, value: int, unit: str) -> None: ...

    @abstractmethod
    def write_interval(
        self, w: StringIO, write_value: WriteFunc, unit: str
    ) -> None: ...

    @abstractmethod
    def write_extract(
        self,
        w: StringIO,
        part: str,
        write_expr: WriteFunc,
        write_tz: WriteFunc | None,
    ) -> None: ...

    @abstractmethod
    def write_timestamp_arithmetic(
        self,
        w: StringIO,
        op: str,
        write_ts: WriteFunc,
        write_dur: WriteFunc,
    ) -> None: ...

    # --- String Functions ---

    @abstractmethod
    def write_contains(
        self, w: StringIO, write_haystack: WriteFunc, write_needle: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None: ...

    @abstractmethod
    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None: ...

    @abstractmethod
    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None: ...

    # --- Comprehensions ---

    @abstractmethod
    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None: ...

    @abstractmethod
    def write_array_subquery_open(self, w: StringIO) -> None: ...

    # --- Struct ---

    @abstractmethod
    def write_struct_open(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_struct_close(self, w: StringIO) -> None: ...

    # --- Validation ---

    @abstractmethod
    def max_identifier_length(self) -> int: ...

    @abstractmethod
    def validate_field_name(self, name: str) -> None: ...

    # --- Capabilities ---

    @abstractmethod
    def supports_native_arrays(self) -> bool: ...

    @abstractmethod
    def supports_jsonb(self) -> bool: ...
