"""Shared test fixtures."""

import pytest

from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect


@pytest.fixture
def pg_dialect():
    return PostgresDialect()


@pytest.fixture
def duckdb_dialect():
    return DuckDBDialect()


@pytest.fixture
def bigquery_dialect():
    return BigQueryDialect()


@pytest.fixture
def mysql_dialect():
    return MySQLDialect()


@pytest.fixture
def sqlite_dialect():
    return SQLiteDialect()


ALL_DIALECTS = [
    PostgresDialect(),
    DuckDBDialect(),
    BigQueryDialect(),
    MySQLDialect(),
    SQLiteDialect(),
]
