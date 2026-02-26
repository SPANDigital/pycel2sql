"""Shared test fixtures."""

import pytest

from pycel2sql.dialect.postgres import PostgresDialect


@pytest.fixture
def pg_dialect():
    return PostgresDialect()
