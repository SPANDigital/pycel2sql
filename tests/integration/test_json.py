"""Integration tests for JSON field access — PG and MySQL."""

from __future__ import annotations

import pytest

from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.schema import FieldSchema, Schema

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration

PG_JSON_SCHEMAS = {"usr": Schema([FieldSchema("metadata", is_json=True, is_jsonb=True)])}
MYSQL_JSON_SCHEMAS = {"usr": Schema([FieldSchema("metadata", is_json=True, is_jsonb=False)])}
BQ_JSON_SCHEMAS = {"usr": Schema([FieldSchema("metadata", is_json=True, is_jsonb=False)])}

JSON_DBS = [
    pytest.param(
        ("pg", PostgresDialect(), PG_JSON_SCHEMAS),
        id="pg", marks=pytest.mark.postgres,
    ),
    pytest.param(
        ("mysql", MySQLDialect(), MYSQL_JSON_SCHEMAS),
        id="mysql", marks=pytest.mark.mysql,
    ),
    pytest.param(
        ("bq", BigQueryDialect(), BQ_JSON_SCHEMAS),
        id="bq", marks=pytest.mark.bigquery,
    ),
]


@pytest.fixture(params=JSON_DBS)
def json_db(request):
    db_name, dialect, schemas = request.param
    conn = request.getfixturevalue(f"{db_name}_db")
    return conn, dialect, db_name, schemas


class TestJson:
    def test_json_string_field(self, json_db):
        conn, dialect, name, schemas = json_db
        rows = execute_cel(
            conn, 'usr.metadata.role == "admin"', dialect, name,
            schemas=schemas, table_alias="usr",
        )
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_json_numeric_field(self, json_db):
        conn, dialect, name, schemas = json_db
        rows = execute_cel(
            conn, 'usr.metadata.level > 3', dialect, name,
            schemas=schemas, table_alias="usr",
        )
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_json_has(self, json_db):
        conn, dialect, name, schemas = json_db
        rows = execute_cel(
            conn, 'has(usr.metadata.role)', dialect, name,
            schemas=schemas, table_alias="usr",
        )
        assert get_names(rows) == {"Alice", "Bob", "Charlie", "Diana", "Eve"}
