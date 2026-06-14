"""Abstract base class for SQL dialects."""

from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from collections.abc import Callable
from io import StringIO
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pycel2sql._analysis_types import (
        IndexPattern,
        IndexRecommendation,
        PatternType,
    )


class DialectName(enum.StrEnum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    DUCKDB = "duckdb"
    BIGQUERY = "bigquery"
    SPARK = "spark"


WriteFunc = Callable[[], None]
"""Callback that writes a sub-expression to the shared StringIO buffer."""


class Dialect(ABC):
    """Abstract base class defining the SQL dialect interface.

    All SQL-syntax-specific code lives behind this interface.
    Methods receive a StringIO writer and callback functions for sub-expressions.
    """

    # CEL type name -> SQL type name. Subclasses populate this; consumed by the
    # default ``write_type_name`` below.
    TYPE_MAP: dict[str, str] = {}

    # --- Literals ---

    def write_string_literal(self, w: StringIO, value: str) -> None:
        """Default: single-quoted with ``'`` doubled. Override for other escaping."""
        escaped = value.replace("'", "''")
        w.write(f"'{escaped}'")

    @abstractmethod
    def write_bytes_literal(self, w: StringIO, value: bytes) -> None: ...

    @abstractmethod
    def write_param_placeholder(self, w: StringIO, param_index: int) -> None: ...

    # --- Operators ---

    def write_string_concat(
        self, w: StringIO, write_lhs: WriteFunc, write_rhs: WriteFunc
    ) -> None:
        """Default: SQL-standard ``||`` concatenation. Override for function-style concat."""
        write_lhs()
        w.write(" || ")
        write_rhs()

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
    def write_cast_to_numeric(self, w: StringIO, write_expr: WriteFunc) -> None: ...

    def write_type_name(self, w: StringIO, cel_type_name: str) -> None:
        """Default: look up ``TYPE_MAP``, falling back to the upper-cased CEL name."""
        w.write(self.TYPE_MAP.get(cel_type_name, cel_type_name.upper()))

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
        self, w: StringIO, json_func: str, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        """Write a JSON array membership test (``elem in arr``) for the IN operator.

        ``write_elem`` emits the candidate element; ``write_array`` emits the
        JSON array expression. The dialect owns the full boolean predicate.
        """
        ...

    @abstractmethod
    def write_nested_json_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        """Write a nested JSON array membership test for the IN operator."""
        ...

    # --- Timestamps ---

    def write_duration(self, w: StringIO, value: int, unit: str) -> None:
        """Default: ``INTERVAL <n> <unit>``. Override where intervals differ (e.g. SQLite)."""
        w.write(f"INTERVAL {value} {unit}")

    def write_interval(
        self, w: StringIO, write_value: WriteFunc, unit: str
    ) -> None:
        """Default: ``INTERVAL <expr> <unit>``. Override where intervals differ (e.g. SQLite)."""
        w.write("INTERVAL ")
        write_value()
        w.write(f" {unit}")

    @abstractmethod
    def write_extract(
        self,
        w: StringIO,
        part: str,
        write_expr: WriteFunc,
        write_tz: WriteFunc | None,
    ) -> None: ...

    def write_timestamp_arithmetic(
        self,
        w: StringIO,
        op: str,
        write_ts: WriteFunc,
        write_dur: WriteFunc,
    ) -> None:
        """Default: infix ``<ts> <op> <dur>``. Override for function-style (BigQuery)
        or modifier-style (SQLite) arithmetic."""
        write_ts()
        w.write(f" {op} ")
        write_dur()

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

    @abstractmethod
    def write_format(
        self, w: StringIO, fmt_string: str, write_args: list[WriteFunc]
    ) -> None: ...

    # --- Comprehensions ---

    @abstractmethod
    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None: ...

    @abstractmethod
    def write_array_subquery_open(self, w: StringIO) -> None: ...

    @abstractmethod
    def write_array_subquery_expr_close(self, w: StringIO) -> None: ...

    # --- Regex ---

    @abstractmethod
    def convert_regex(self, re2_pattern: str) -> tuple[str, bool]: ...

    # --- Struct ---

    @abstractmethod
    def write_struct_open(self, w: StringIO) -> None: ...

    def write_struct_close(self, w: StringIO) -> None:
        """Default: ``)``. Pairs with the dialect-specific ``write_struct_open``."""
        w.write(")")

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


@runtime_checkable
class IndexAdvisor(Protocol):
    """Protocol for dialects that can recommend indexes."""

    def recommend_index(self, pattern: IndexPattern) -> IndexRecommendation | None: ...

    def supported_patterns(self) -> list[PatternType]: ...


def get_index_advisor(dialect: Dialect) -> IndexAdvisor | None:
    """Get the IndexAdvisor if the dialect supports index analysis."""
    if isinstance(dialect, IndexAdvisor):
        return dialect
    return None
