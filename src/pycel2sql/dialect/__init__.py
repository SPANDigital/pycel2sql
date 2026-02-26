"""SQL dialect system for CEL-to-SQL conversion."""

from pycel2sql.dialect._base import Dialect, DialectName
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect

__all__ = [
    "Dialect",
    "DialectName",
    "BigQueryDialect",
    "DuckDBDialect",
    "MySQLDialect",
    "PostgresDialect",
    "SQLiteDialect",
    "get_dialect",
]

_REGISTRY: dict[str, type[Dialect]] = {
    DialectName.POSTGRESQL: PostgresDialect,
    DialectName.DUCKDB: DuckDBDialect,
    DialectName.BIGQUERY: BigQueryDialect,
    DialectName.MYSQL: MySQLDialect,
    DialectName.SQLITE: SQLiteDialect,
}


def get_dialect(name: str) -> Dialect:
    """Get a dialect instance by name.

    Args:
        name: Dialect name (e.g., "postgresql", "mysql", "sqlite", "duckdb", "bigquery").

    Returns:
        A Dialect instance.

    Raises:
        ValueError: If the dialect name is unknown.
    """
    cls = _REGISTRY.get(name)
    if cls is None:
        raise ValueError(
            f"unknown dialect: {name!r}. "
            f"Available: {', '.join(sorted(_REGISTRY))}"
        )
    return cls()
