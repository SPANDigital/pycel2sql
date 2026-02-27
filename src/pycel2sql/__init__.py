"""pycel2sql - Convert CEL expressions to SQL WHERE clauses."""

from __future__ import annotations

try:
    from pycel2sql._version import __version__
except ModuleNotFoundError:  # editable install without VCS metadata
    __version__ = "0.0.0.dev0"

from dataclasses import dataclass, field
from typing import Any

from celpy.celparser import CELParser

from pycel2sql._converter import Converter
from pycel2sql._errors import ConversionError, IntrospectionError
from pycel2sql.dialect._base import Dialect
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect
from pycel2sql.introspect import introspect
from pycel2sql.schema import Schema

__all__ = [
    "analyze",
    "convert",
    "convert_parameterized",
    "introspect",
    "AnalysisResult",
    "Result",
    "ConversionError",
    "IntrospectionError",
    "Dialect",
    "BigQueryDialect",
    "DuckDBDialect",
    "MySQLDialect",
    "PostgresDialect",
    "SQLiteDialect",
]

_parser = CELParser()


@dataclass(frozen=True)
class Result:
    """Result of a parameterized conversion."""

    sql: str
    parameters: list[Any] = field(default_factory=list)


def convert(
    cel_expr: str,
    *,
    dialect: Dialect | None = None,
    schemas: dict[str, Schema] | None = None,
    max_depth: int | None = None,
    max_output_length: int | None = None,
) -> str:
    """Convert a CEL expression to an inline SQL WHERE clause string.

    Args:
        cel_expr: The CEL expression to convert.
        dialect: SQL dialect to use. Defaults to PostgreSQL.
        schemas: Optional table schemas for JSON/array field detection.
        max_depth: Maximum recursion depth. Defaults to 100.
        max_output_length: Maximum SQL output length. Defaults to 50000.

    Returns:
        The SQL WHERE clause string.

    Raises:
        ConversionError: If conversion fails.
    """
    if dialect is None:
        dialect = PostgresDialect()

    tree = _parser.parse(cel_expr)

    kwargs: dict[str, Any] = {}
    if schemas is not None:
        kwargs["schemas"] = schemas
    if max_depth is not None:
        kwargs["max_depth"] = max_depth
    if max_output_length is not None:
        kwargs["max_output_length"] = max_output_length

    converter = Converter(dialect, **kwargs)
    converter.visit(tree)
    return converter.result


def convert_parameterized(
    cel_expr: str,
    *,
    dialect: Dialect | None = None,
    schemas: dict[str, Schema] | None = None,
    max_depth: int | None = None,
    max_output_length: int | None = None,
) -> Result:
    """Convert a CEL expression to a parameterized SQL WHERE clause.

    Args:
        cel_expr: The CEL expression to convert.
        dialect: SQL dialect to use. Defaults to PostgreSQL.
        schemas: Optional table schemas for JSON/array field detection.
        max_depth: Maximum recursion depth. Defaults to 100.
        max_output_length: Maximum SQL output length. Defaults to 50000.

    Returns:
        Result with SQL containing $1, $2, ... placeholders and parameter list.

    Raises:
        ConversionError: If conversion fails.
    """
    if dialect is None:
        dialect = PostgresDialect()

    tree = _parser.parse(cel_expr)

    kwargs: dict[str, Any] = {"parameterize": True}
    if schemas is not None:
        kwargs["schemas"] = schemas
    if max_depth is not None:
        kwargs["max_depth"] = max_depth
    if max_output_length is not None:
        kwargs["max_output_length"] = max_output_length

    converter = Converter(dialect, **kwargs)
    converter.visit(tree)
    return Result(sql=converter.result, parameters=converter.parameters)


@dataclass(frozen=True)
class AnalysisResult:
    """Result of CEL expression analysis."""

    sql: str
    recommendations: list[Any] = field(default_factory=list)


def analyze(
    cel_expr: str,
    *,
    dialect: Dialect | None = None,
    schemas: dict[str, Schema] | None = None,
    max_depth: int | None = None,
    max_output_length: int | None = None,
) -> AnalysisResult:
    """Analyze a CEL expression for SQL conversion and index recommendations.

    Args:
        cel_expr: The CEL expression to analyze.
        dialect: SQL dialect to use. Defaults to PostgreSQL.
        schemas: Optional table schemas for JSON/array field detection.
        max_depth: Maximum recursion depth.
        max_output_length: Maximum SQL output length.

    Returns:
        AnalysisResult with SQL and index recommendations.
    """
    from pycel2sql._analysis import analyze_patterns
    from pycel2sql.dialect._base import get_index_advisor

    if dialect is None:
        dialect = PostgresDialect()

    tree = _parser.parse(cel_expr)

    # Pass 1: Generate SQL
    kwargs: dict[str, Any] = {}
    if schemas is not None:
        kwargs["schemas"] = schemas
    if max_depth is not None:
        kwargs["max_depth"] = max_depth
    if max_output_length is not None:
        kwargs["max_output_length"] = max_output_length

    converter = Converter(dialect, **kwargs)
    converter.visit(tree)
    sql = converter.result

    # Pass 2: Analyze for index recommendations
    advisor = get_index_advisor(dialect)
    recommendations = []
    if advisor is not None:
        tree2 = _parser.parse(cel_expr)
        recommendations = analyze_patterns(tree2, advisor, schemas)

    return AnalysisResult(sql=sql, recommendations=recommendations)
