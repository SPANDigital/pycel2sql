"""Integration tests for comprehensions — executed against real databases.

These exist because comprehension SQL can parse and still be wrong: SQLite's
json_each and MySQL's JSON_TABLE are table-valued, so a bare reference to the
iteration variable is "no such column"; DuckDB's UNNEST binds the alias to a
STRUCT row; and MySQL 8.x turns a correlated EXISTS into a semijoin that loses
the JSON_TABLE correlation and silently matches nothing.
"""

from __future__ import annotations

import pytest

from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect
from pycel2sql.schema import FieldSchema, Schema

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration

COMPREHENSION_DBS = [
    pytest.param(("pg", PostgresDialect()), id="pg", marks=pytest.mark.postgres),
    pytest.param(("duckdb", DuckDBDialect()), id="duckdb", marks=pytest.mark.duckdb),
    pytest.param(("sqlite", SQLiteDialect()), id="sqlite", marks=pytest.mark.sqlite),
    pytest.param(("mysql", MySQLDialect()), id="mysql", marks=pytest.mark.mysql),
]

SCHEMAS = {"t": Schema([FieldSchema("tags", repeated=True)])}


@pytest.fixture(params=COMPREHENSION_DBS)
def comp_db(request):
    db_name, dialect = request.param
    conn = request.getfixturevalue(f"{db_name}_db")
    return conn, dialect, db_name


class TestComprehensions:
    def test_exists(self, comp_db):
        conn, dialect, name = comp_db
        rows = execute_cel(
            conn, 't.tags.exists(x, x == "python")', dialect, name,
            schemas=SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_exists_no_match(self, comp_db):
        conn, dialect, name = comp_db
        rows = execute_cel(
            conn, 't.tags.exists(x, x == "cobol")', dialect, name,
            schemas=SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == set()

    def test_all(self, comp_db):
        conn, dialect, name = comp_db
        rows = execute_cel(
            conn, 't.tags.all(x, x != "rust")', dialect, name,
            schemas=SCHEMAS, table_alias="t",
        )
        # Eve has no tags: all() over an empty array is vacuously true.
        assert get_names(rows) == {"Alice", "Charlie", "Eve"}

    def test_exists_one(self, comp_db):
        conn, dialect, name = comp_db
        rows = execute_cel(
            conn, 't.tags.exists_one(x, x == "go")', dialect, name,
            schemas=SCHEMAS, table_alias="t",
        )
        assert get_names(rows) == {"Bob", "Charlie"}
