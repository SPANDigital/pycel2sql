"""Unit tests for schema introspection (mock connections)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from pycel2sql._errors import IntrospectionError
from pycel2sql.introspect import introspect
from pycel2sql.introspect.bigquery import introspect_bigquery
from pycel2sql.introspect.duckdb import introspect_duckdb
from pycel2sql.introspect.mysql import introspect_mysql
from pycel2sql.introspect.postgres import introspect_postgres
from pycel2sql.introspect.sqlite import introspect_sqlite


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_pg_conn(rows: list[tuple[Any, ...]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _make_duckdb_conn(rows: list[tuple[Any, ...]]) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = rows
    conn = MagicMock()
    conn.execute.return_value = result
    return conn


def _make_mysql_conn(rows: list[tuple[Any, ...]]) -> MagicMock:
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _make_sqlite_conn(rows_by_table: dict[str, list[tuple[Any, ...]]]) -> MagicMock:
    def execute_side_effect(query: str, *_: Any) -> MagicMock:
        cursor = MagicMock()
        for table_name, rows in rows_by_table.items():
            if f"table_info({table_name})" in query:
                cursor.fetchall.return_value = rows
                return cursor
        cursor.fetchall.return_value = []
        return cursor

    conn = MagicMock()
    conn.execute.side_effect = execute_side_effect
    return conn


@dataclass
class FakeSchemaField:
    name: str
    field_type: str
    mode: str = "NULLABLE"


@dataclass
class FakeTable:
    schema: list[FakeSchemaField]


def _make_bq_client(tables: dict[str, list[FakeSchemaField]]) -> MagicMock:
    def get_table_side_effect(ref: str) -> FakeTable:
        if ref in tables:
            return FakeTable(schema=tables[ref])
        raise Exception(f"Not found: {ref}")

    client = MagicMock()
    client.get_table.side_effect = get_table_side_effect
    return client


# ---------------------------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------------------------


class TestPostgres:
    def test_basic_columns(self) -> None:
        rows = [
            ("users", "id", "integer", "int4"),
            ("users", "name", "character varying", "varchar"),
            ("users", "metadata", "jsonb", "jsonb"),
            ("users", "tags", "ARRAY", "_text"),
            ("users", "config", "json", "json"),
        ]
        conn = _make_pg_conn(rows)
        schemas = introspect_postgres(conn, table_names=["users"])

        assert "users" in schemas
        s = schemas["users"]
        assert len(s) == 5

        id_f = s.find_field("id")
        assert id_f is not None
        assert not id_f.is_json
        assert not id_f.repeated

        meta_f = s.find_field("metadata")
        assert meta_f is not None
        assert meta_f.is_json
        assert meta_f.is_jsonb

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated
        assert not tags_f.is_json

        config_f = s.find_field("config")
        assert config_f is not None
        assert config_f.is_json
        assert not config_f.is_jsonb

    def test_table_not_found(self) -> None:
        conn = _make_pg_conn([])
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_postgres(conn, table_names=["missing"])

    def test_empty_table_names(self) -> None:
        conn = _make_pg_conn([])
        assert introspect_postgres(conn, table_names=[]) == {}

    def test_custom_schema_name(self) -> None:
        rows = [("t1", "id", "integer", "int4")]
        conn = _make_pg_conn(rows)
        schemas = introspect_postgres(
            conn, table_names=["t1"], schema_name="myschema"
        )
        assert "t1" in schemas
        # Verify schema_name was passed to query
        cur = conn.cursor()
        call_args = cur.execute.call_args
        assert "myschema" in call_args[0][1]

    def test_multiple_tables(self) -> None:
        rows = [
            ("t1", "id", "integer", "int4"),
            ("t2", "name", "text", "text"),
        ]
        conn = _make_pg_conn(rows)
        schemas = introspect_postgres(conn, table_names=["t1", "t2"])
        assert "t1" in schemas
        assert "t2" in schemas


# ---------------------------------------------------------------------------
# DuckDB
# ---------------------------------------------------------------------------


class TestDuckDB:
    def test_basic_columns(self) -> None:
        rows = [
            ("events", "id", "INTEGER"),
            ("events", "payload", "JSON"),
            ("events", "tags", "VARCHAR[]"),
            ("events", "nums", "INTEGER[]"),
        ]
        conn = _make_duckdb_conn(rows)
        schemas = introspect_duckdb(conn, table_names=["events"])

        assert "events" in schemas
        s = schemas["events"]
        assert len(s) == 4

        id_f = s.find_field("id")
        assert id_f is not None
        assert not id_f.is_json
        assert not id_f.repeated

        payload_f = s.find_field("payload")
        assert payload_f is not None
        assert payload_f.is_json
        assert not payload_f.is_jsonb

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated
        assert not tags_f.is_json

    def test_table_not_found(self) -> None:
        conn = _make_duckdb_conn([])
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_duckdb(conn, table_names=["missing"])

    def test_empty_table_names(self) -> None:
        conn = _make_duckdb_conn([])
        assert introspect_duckdb(conn, table_names=[]) == {}


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


class TestMySQL:
    def test_basic_columns(self) -> None:
        rows = [
            ("orders", "id", "int"),
            ("orders", "data", "json"),
            ("orders", "name", "varchar"),
        ]
        conn = _make_mysql_conn(rows)
        schemas = introspect_mysql(conn, table_names=["orders"])

        assert "orders" in schemas
        s = schemas["orders"]
        assert len(s) == 3

        data_f = s.find_field("data")
        assert data_f is not None
        assert data_f.is_json
        assert not data_f.repeated

        name_f = s.find_field("name")
        assert name_f is not None
        assert not name_f.is_json

    def test_table_not_found(self) -> None:
        conn = _make_mysql_conn([])
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_mysql(conn, table_names=["missing"])

    def test_explicit_database(self) -> None:
        rows = [("t1", "id", "int")]
        conn = _make_mysql_conn(rows)
        schemas = introspect_mysql(
            conn, table_names=["t1"], database="mydb"
        )
        assert "t1" in schemas
        cur = conn.cursor()
        call_args = cur.execute.call_args
        assert "mydb" in call_args[0][1]

    def test_empty_table_names(self) -> None:
        conn = _make_mysql_conn([])
        assert introspect_mysql(conn, table_names=[]) == {}


# ---------------------------------------------------------------------------
# SQLite
# ---------------------------------------------------------------------------


class TestSQLite:
    def test_basic_columns(self) -> None:
        rows = {
            "items": [
                (0, "id", "INTEGER", 1, None, 1),
                (1, "name", "TEXT", 0, None, 0),
                (2, "data", "JSON", 0, None, 0),
                (3, "blob", "BLOB", 0, None, 0),
            ],
        }
        conn = _make_sqlite_conn(rows)
        schemas = introspect_sqlite(conn, table_names=["items"])

        assert "items" in schemas
        s = schemas["items"]
        assert len(s) == 4

        data_f = s.find_field("data")
        assert data_f is not None
        assert data_f.is_json

        name_f = s.find_field("name")
        assert name_f is not None
        assert not name_f.is_json

    def test_json_columns_override(self) -> None:
        rows = {
            "items": [
                (0, "id", "INTEGER", 1, None, 1),
                (1, "config", "TEXT", 0, None, 0),
            ],
        }
        conn = _make_sqlite_conn(rows)
        schemas = introspect_sqlite(
            conn,
            table_names=["items"],
            json_columns={"items": ["config"]},
        )

        config_f = schemas["items"].find_field("config")
        assert config_f is not None
        assert config_f.is_json

    def test_json_type_detection(self) -> None:
        """Columns with 'json' anywhere in type name are detected."""
        rows = {
            "t": [
                (0, "a", "jsontext", 0, None, 0),
                (1, "b", "TEXT", 0, None, 0),
            ],
        }
        conn = _make_sqlite_conn(rows)
        schemas = introspect_sqlite(conn, table_names=["t"])

        assert schemas["t"].find_field("a") is not None
        assert schemas["t"].find_field("a").is_json  # type: ignore[union-attr]
        assert schemas["t"].find_field("b") is not None
        assert not schemas["t"].find_field("b").is_json  # type: ignore[union-attr]

    def test_table_not_found(self) -> None:
        conn = _make_sqlite_conn({})
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_sqlite(conn, table_names=["missing"])

    def test_invalid_table_name(self) -> None:
        conn = _make_sqlite_conn({})
        with pytest.raises(IntrospectionError, match="invalid table name"):
            introspect_sqlite(conn, table_names=["Robert'; DROP TABLE--"])

    def test_empty_table_names(self) -> None:
        conn = _make_sqlite_conn({})
        assert introspect_sqlite(conn, table_names=[]) == {}


# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------


class TestBigQuery:
    def test_basic_columns(self) -> None:
        tables = {
            "my_dataset.events": [
                FakeSchemaField("id", "INTEGER"),
                FakeSchemaField("payload", "JSON"),
                FakeSchemaField("tags", "STRING", "REPEATED"),
                FakeSchemaField("name", "STRING"),
            ],
        }
        client = _make_bq_client(tables)
        schemas = introspect_bigquery(
            client, table_names=["my_dataset.events"]
        )

        assert "events" in schemas
        s = schemas["events"]
        assert len(s) == 4

        payload_f = s.find_field("payload")
        assert payload_f is not None
        assert payload_f.is_json

        tags_f = s.find_field("tags")
        assert tags_f is not None
        assert tags_f.repeated
        assert not tags_f.is_json

    def test_unqualified_with_dataset(self) -> None:
        tables = {
            "ds.t1": [FakeSchemaField("id", "INTEGER")],
        }
        client = _make_bq_client(tables)
        schemas = introspect_bigquery(
            client, table_names=["t1"], dataset="ds"
        )
        assert "t1" in schemas

    def test_unqualified_without_dataset(self) -> None:
        client = _make_bq_client({})
        with pytest.raises(IntrospectionError, match="dataset required"):
            introspect_bigquery(client, table_names=["t1"])

    def test_table_not_found(self) -> None:
        client = _make_bq_client({})
        with pytest.raises(IntrospectionError, match="table not found"):
            introspect_bigquery(
                client, table_names=["missing"], dataset="ds"
            )

    def test_empty_table_names(self) -> None:
        client = _make_bq_client({})
        assert introspect_bigquery(client, table_names=[]) == {}


# ---------------------------------------------------------------------------
# Dispatch function
# ---------------------------------------------------------------------------


class TestDispatch:
    def test_postgresql(self) -> None:
        rows = [("t1", "id", "integer", "int4")]
        conn = _make_pg_conn(rows)
        schemas = introspect("postgresql", conn, table_names=["t1"])
        assert "t1" in schemas

    def test_duckdb(self) -> None:
        rows = [("t1", "id", "INTEGER")]
        conn = _make_duckdb_conn(rows)
        schemas = introspect("duckdb", conn, table_names=["t1"])
        assert "t1" in schemas

    def test_mysql(self) -> None:
        rows = [("t1", "id", "int")]
        conn = _make_mysql_conn(rows)
        schemas = introspect("mysql", conn, table_names=["t1"])
        assert "t1" in schemas

    def test_sqlite(self) -> None:
        rows_by_table = {"t1": [(0, "id", "INTEGER", 1, None, 1)]}
        conn = _make_sqlite_conn(rows_by_table)
        schemas = introspect("sqlite", conn, table_names=["t1"])
        assert "t1" in schemas

    def test_bigquery(self) -> None:
        tables = {"ds.t1": [FakeSchemaField("id", "INTEGER")]}
        client = _make_bq_client(tables)
        schemas = introspect(
            "bigquery", client, table_names=["t1"], dataset="ds"
        )
        assert "t1" in schemas

    def test_unknown_dialect(self) -> None:
        with pytest.raises(ValueError, match="unknown dialect"):
            introspect("oracle", MagicMock(), table_names=["t1"])

    def test_empty_table_names(self) -> None:
        assert introspect("postgresql", MagicMock(), table_names=[]) == {}


# ---------------------------------------------------------------------------
# Lazy import / __getattr__
# ---------------------------------------------------------------------------


class TestLazyImports:
    def test_import_introspect_postgres(self) -> None:
        from pycel2sql.introspect import introspect_postgres as fn

        assert callable(fn)

    def test_import_introspect_duckdb(self) -> None:
        from pycel2sql.introspect import introspect_duckdb as fn

        assert callable(fn)

    def test_import_introspect_mysql(self) -> None:
        from pycel2sql.introspect import introspect_mysql as fn

        assert callable(fn)

    def test_import_introspect_sqlite(self) -> None:
        from pycel2sql.introspect import introspect_sqlite as fn

        assert callable(fn)

    def test_import_introspect_bigquery(self) -> None:
        from pycel2sql.introspect import introspect_bigquery as fn

        assert callable(fn)

    def test_import_nonexistent_raises(self) -> None:
        with pytest.raises(AttributeError, match="no attribute"):
            from pycel2sql import introspect as mod

            mod.__getattr__("does_not_exist")


# ---------------------------------------------------------------------------
# Top-level package exports
# ---------------------------------------------------------------------------


class TestTopLevelExports:
    def test_introspect_importable(self) -> None:
        from pycel2sql import introspect as fn

        assert callable(fn)

    def test_introspection_error_importable(self) -> None:
        from pycel2sql import IntrospectionError as cls

        assert issubclass(cls, Exception)
