"""Fixtures and helpers for integration tests against real databases."""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
from typing import Any

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql.dialect._base import Dialect
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.dialect.sqlite import SQLiteDialect
from pycel2sql.schema import Schema


# ---------------------------------------------------------------------------
# Container runtime detection (Docker or Podman)
# ---------------------------------------------------------------------------

def _get_podman_socket() -> str | None:
    """Get the Podman machine socket path, if available."""
    try:
        result = subprocess.run(
            ["podman", "machine", "inspect", "--format",
             "{{.ConnectionInfo.PodmanSocket.Path}}"],
            capture_output=True, text=True, check=True, timeout=5,
        )
        sock = result.stdout.strip()
        if sock and os.path.exists(sock):
            return sock
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            FileNotFoundError):
        pass
    return None


def _container_runtime_available() -> bool:
    """Check if Docker or Podman is available as a container runtime."""
    # Check Docker first
    for cmd in ["docker", "podman"]:
        if shutil.which(cmd):
            try:
                subprocess.run(
                    [cmd, "info"], capture_output=True, check=True, timeout=10,
                )
                return True
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                    FileNotFoundError):
                continue
    return False


def _configure_testcontainers_for_podman() -> None:
    """Configure testcontainers to work with Podman."""
    if not shutil.which("podman"):
        return
    # Disable Ryuk (resource reaper) — Podman doesn't always support it
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
    # Point DOCKER_HOST to the Podman socket if not already set
    if "DOCKER_HOST" not in os.environ:
        sock = _get_podman_socket()
        if sock:
            os.environ["DOCKER_HOST"] = f"unix://{sock}"


CONTAINER_RUNTIME_AVAILABLE = _container_runtime_available()

if CONTAINER_RUNTIME_AVAILABLE:
    _configure_testcontainers_for_podman()


# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

SEED_ROWS = [
    {
        "name": "Alice",
        "age": 30,
        "height": 1.65,
        "active": True,
        "email": "alice@example.com",
        "tags": ["python", "sql"],
        "metadata": {"role": "admin", "level": 5},
        "created_at": "2024-01-15T10:00:00Z",
    },
    {
        "name": "Bob",
        "age": 25,
        "height": 1.80,
        "active": True,
        "email": "bob@test.com",
        "tags": ["go", "rust"],
        "metadata": {"role": "user", "level": 2},
        "created_at": "2024-03-20T14:30:00Z",
    },
    {
        "name": "Charlie",
        "age": 35,
        "height": 1.75,
        "active": False,
        "email": None,
        "tags": ["python", "go", "sql"],
        "metadata": {"role": "admin", "level": 8},
        "created_at": "2023-06-01T08:00:00Z",
    },
    {
        "name": "Diana",
        "age": 28,
        "height": 1.60,
        "active": True,
        "email": "diana@example.com",
        "tags": ["rust"],
        "metadata": {"role": "viewer", "level": 1},
        "created_at": "2024-07-10T16:45:00Z",
    },
    {
        "name": "Eve",
        "age": 22,
        "height": 1.70,
        "active": False,
        "email": "eve@test.com",
        "tags": [],
        "metadata": {"role": "user", "level": 3},
        "created_at": "2024-11-05T12:00:00Z",
    },
]


# ---------------------------------------------------------------------------
# Table setup per dialect
# ---------------------------------------------------------------------------

def _setup_postgres(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            height DOUBLE PRECISION NOT NULL,
            active BOOLEAN NOT NULL,
            email TEXT,
            tags TEXT[],
            metadata JSONB,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL
        )
    """)
    for row in SEED_ROWS:
        cur.execute(
            """INSERT INTO test_data (name, age, height, active, email, tags, metadata, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                row["name"], row["age"], row["height"], row["active"],
                row["email"], row["tags"], json.dumps(row["metadata"]),
                row["created_at"],
            ),
        )
    conn.commit()
    cur.close()


def _setup_duckdb(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            name VARCHAR NOT NULL,
            age INTEGER NOT NULL,
            height DOUBLE NOT NULL,
            active BOOLEAN NOT NULL,
            email VARCHAR,
            tags VARCHAR[],
            metadata JSON,
            created_at TIMESTAMPTZ NOT NULL
        )
    """)
    for row in SEED_ROWS:
        conn.execute(
            """INSERT INTO test_data (name, age, height, active, email, tags, metadata, created_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""",
            [
                row["name"], row["age"], row["height"], row["active"],
                row["email"], row["tags"], json.dumps(row["metadata"]),
                row["created_at"],
            ],
        )


def _setup_mysql(conn) -> None:
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            name VARCHAR(255) NOT NULL,
            age INTEGER NOT NULL,
            height DOUBLE NOT NULL,
            active BOOLEAN NOT NULL,
            email VARCHAR(255),
            tags JSON,
            metadata JSON,
            created_at DATETIME NOT NULL
        )
    """)
    for row in SEED_ROWS:
        cur.execute(
            """INSERT INTO test_data (name, age, height, active, email, tags, metadata, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                row["name"], row["age"], row["height"], row["active"],
                row["email"], json.dumps(row["tags"]), json.dumps(row["metadata"]),
                row["created_at"].replace("T", " ").replace("Z", ""),
            ),
        )
    conn.commit()
    cur.close()


def _setup_bigquery(client) -> None:
    client.query("""
        CREATE TABLE IF NOT EXISTS test_dataset.test_data (
            name STRING NOT NULL,
            age INT64 NOT NULL,
            height FLOAT64 NOT NULL,
            active BOOL NOT NULL,
            email STRING,
            tags ARRAY<STRING>,
            metadata JSON,
            created_at TIMESTAMP NOT NULL
        )
    """).result()
    for row in SEED_ROWS:
        tags_literal = ", ".join(f"'{t}'" for t in row["tags"])
        metadata_json = json.dumps(row["metadata"])
        active_str = "TRUE" if row["active"] else "FALSE"
        email_str = f"'{row['email']}'" if row["email"] else "NULL"
        client.query(f"""
            INSERT INTO test_dataset.test_data
            (name, age, height, active, email, tags, metadata, created_at)
            VALUES (
                '{row["name"]}', {row["age"]}, {row["height"]}, {active_str},
                {email_str}, [{tags_literal}],
                JSON '{metadata_json}', TIMESTAMP '{row["created_at"]}'
            )
        """).result()


def _setup_sqlite(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS test_data (
            name TEXT NOT NULL,
            age INTEGER NOT NULL,
            height REAL NOT NULL,
            active INTEGER NOT NULL,
            email TEXT,
            tags TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL
        )
    """)
    for row in SEED_ROWS:
        conn.execute(
            """INSERT INTO test_data (name, age, height, active, email, tags, metadata, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                row["name"], row["age"], row["height"], 1 if row["active"] else 0,
                row["email"], json.dumps(row["tags"]), json.dumps(row["metadata"]),
                row["created_at"].replace("T", " ").replace("Z", ""),
            ),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Session-scoped container fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_container():
    if not CONTAINER_RUNTIME_AVAILABLE:
        pytest.skip("No container runtime (Docker/Podman) available")
    from testcontainers.postgres import PostgresContainer
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="session")
def mysql_container():
    if not CONTAINER_RUNTIME_AVAILABLE:
        pytest.skip("No container runtime (Docker/Podman) available")
    from testcontainers.mysql import MySqlContainer
    with MySqlContainer("mysql:8.4") as mysql:
        yield mysql


@pytest.fixture(scope="session")
def bq_container():
    if not CONTAINER_RUNTIME_AVAILABLE:
        pytest.skip("No container runtime (Docker/Podman) available")
    # BigQuery emulator only has amd64 images; the Go runtime crashes under
    # QEMU emulation on ARM hosts.  Skip on non-x86_64 machines.
    if platform.machine() not in ("x86_64", "AMD64"):
        pytest.skip(
            f"BigQuery emulator requires x86_64 (current: {platform.machine()})"
        )
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs
    container = (
        DockerContainer("ghcr.io/goccy/bigquery-emulator:latest")
        .with_exposed_ports(9050)
        .with_command("--project=test-project --dataset=test_dataset")
    )
    with container:
        wait_for_logs(container, "listening", timeout=60)
        yield container


# ---------------------------------------------------------------------------
# Session-scoped database fixtures (connection + table + data)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_db(pg_container):
    import psycopg
    # Build a psycopg3-compatible connection string (not SQLAlchemy URL)
    host = pg_container.get_container_host_ip()
    port = pg_container.get_exposed_port(5432)
    conn = psycopg.connect(
        host=host, port=port,
        user=pg_container.username,
        password=pg_container.password,
        dbname=pg_container.dbname,
    )
    _setup_postgres(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def mysql_db(mysql_container):
    import mysql.connector
    conn = mysql.connector.connect(
        host=mysql_container.get_container_host_ip(),
        port=int(mysql_container.get_exposed_port(3306)),
        user=mysql_container.username,
        password=mysql_container.password,
        database=mysql_container.dbname,
    )
    _setup_mysql(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def duckdb_db():
    import duckdb
    conn = duckdb.connect(":memory:")
    _setup_duckdb(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sqlite_db():
    import sqlite3
    conn = sqlite3.connect(":memory:")
    _setup_sqlite(conn)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def bq_db(bq_container):
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    host = bq_container.get_container_host_ip()
    port = bq_container.get_exposed_port(9050)
    client = bigquery.Client(
        project="test-project",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://{host}:{port}"),
    )
    _setup_bigquery(client)
    yield client
    client.close()


# ---------------------------------------------------------------------------
# Query execution helpers
# ---------------------------------------------------------------------------

def _adapt_params_for_driver(sql: str, dialect_name: str) -> str:
    """Adapt parameter placeholders for the database driver.

    - PostgreSQL ($1, $2): psycopg3 uses %s placeholders
    - DuckDB ($1, $2): native support, no change needed
    - MySQL (?): mysql-connector uses %s
    - SQLite (?): native support, no change needed
    - BigQuery (@p1, @p2): uses QueryJobConfig with named params
    """
    if dialect_name == "pg":
        # Replace $1, $2, ... with %s
        return re.sub(r"\$\d+", "%s", sql)
    if dialect_name == "mysql":
        return sql.replace("?", "%s")
    return sql


def _rows_to_dicts(cursor_or_result, db_name: str) -> list[dict[str, Any]]:
    """Convert database results to list of dicts."""
    if db_name == "duckdb":
        columns = [d[0] for d in cursor_or_result.description]
        rows = cursor_or_result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    elif db_name == "sqlite":
        columns = [d[0] for d in cursor_or_result.description]
        rows = cursor_or_result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    else:
        # psycopg3 and mysql-connector
        columns = [d[0] for d in cursor_or_result.description]
        rows = cursor_or_result.fetchall()
        return [dict(zip(columns, row)) for row in rows]


def execute_cel(
    conn,
    cel_expr: str,
    dialect: Dialect,
    db_name: str,
    *,
    schemas: dict[str, Schema] | None = None,
    table_alias: str | None = None,
) -> list[dict[str, Any]]:
    """Convert CEL to SQL WHERE clause and execute against the database.

    Returns list of row dicts.
    """
    sql_where = convert(cel_expr, dialect=dialect, schemas=schemas)

    if db_name == "bq":
        table_expr = "test_dataset.test_data"
        if table_alias:
            table_expr = f"test_dataset.test_data AS {table_alias}"
        query = f"SELECT * FROM {table_expr} WHERE {sql_where}"
        return _execute_bq(conn, query)

    table_expr = "test_data"
    if table_alias:
        table_expr = f"test_data AS {table_alias}"
    query = f"SELECT * FROM {table_expr} WHERE {sql_where}"

    if db_name == "duckdb":
        result = conn.execute(query)
        return _rows_to_dicts(result, db_name)
    elif db_name == "sqlite":
        cur = conn.execute(query)
        return _rows_to_dicts(cur, db_name)
    else:
        # pg or mysql — use cursor
        cur = conn.cursor()
        cur.execute(query)
        rows = _rows_to_dicts(cur, db_name)
        cur.close()
        return rows


def execute_cel_parameterized(
    conn,
    cel_expr: str,
    dialect: Dialect,
    db_name: str,
    *,
    schemas: dict[str, Schema] | None = None,
    table_alias: str | None = None,
) -> list[dict[str, Any]]:
    """Convert CEL to parameterized SQL and execute against the database."""
    result = convert_parameterized(cel_expr, dialect=dialect, schemas=schemas)

    if db_name == "bq":
        table_expr = "test_dataset.test_data"
        if table_alias:
            table_expr = f"test_dataset.test_data AS {table_alias}"
        query = f"SELECT * FROM {table_expr} WHERE {result.sql}"
        return _execute_bq_parameterized(conn, query, result.parameters)

    table_expr = "test_data"
    if table_alias:
        table_expr = f"test_data AS {table_alias}"
    query = f"SELECT * FROM {table_expr} WHERE {result.sql}"
    query = _adapt_params_for_driver(query, db_name)
    params = result.parameters

    if db_name == "duckdb":
        # DuckDB uses $1 natively, pass as list
        original_query = f"SELECT * FROM {table_expr} WHERE {result.sql}"
        res = conn.execute(original_query, params)
        return _rows_to_dicts(res, db_name)
    elif db_name == "sqlite":
        cur = conn.execute(query, params)
        return _rows_to_dicts(cur, db_name)
    else:
        cur = conn.cursor()
        cur.execute(query, tuple(params))
        rows = _rows_to_dicts(cur, db_name)
        cur.close()
        return rows


def _execute_bq(client, query: str) -> list[dict[str, Any]]:
    """Execute a query against BigQuery and return list of dicts."""
    result = client.query(query).result()
    return [dict(row) for row in result]


def _execute_bq_parameterized(
    client, query: str, params: list[Any],
) -> list[dict[str, Any]]:
    """Execute a parameterized query against BigQuery."""
    from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

    _BQ_TYPE_MAP = {
        str: "STRING",
        int: "INT64",
        float: "FLOAT64",
        bool: "BOOL",
    }

    job_params = []
    for i, val in enumerate(params, 1):
        bq_type = _BQ_TYPE_MAP.get(type(val), "STRING")
        job_params.append(ScalarQueryParameter(f"p{i}", bq_type, val))

    config = QueryJobConfig(query_parameters=job_params)
    result = client.query(query, job_config=config).result()
    return [dict(row) for row in result]


def get_names(rows: list[dict[str, Any]]) -> set[str]:
    """Extract the set of 'name' values from result rows."""
    return {row["name"] for row in rows}


# ---------------------------------------------------------------------------
# Parametrized database fixture
# ---------------------------------------------------------------------------

ALL_DBS = ["pg", "duckdb", "mysql", "sqlite", "bq"]
NO_DOCKER_DBS = ["duckdb", "sqlite"]

_DIALECTS: dict[str, Dialect] = {
    "pg": PostgresDialect(),
    "duckdb": DuckDBDialect(),
    "mysql": MySQLDialect(),
    "sqlite": SQLiteDialect(),
    "bq": BigQueryDialect(),
}


@pytest.fixture(params=ALL_DBS)
def db(request):
    """Yields (connection, dialect, db_name) for each database."""
    name = request.param
    conn = request.getfixturevalue(f"{name}_db")
    return conn, _DIALECTS[name], name


@pytest.fixture(params=NO_DOCKER_DBS)
def local_db(request):
    """Yields (connection, dialect, db_name) for non-Docker databases only."""
    name = request.param
    conn = request.getfixturevalue(f"{name}_db")
    return conn, _DIALECTS[name], name
