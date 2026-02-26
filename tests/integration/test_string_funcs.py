"""Integration tests for string functions."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestStringFunctions:
    def test_contains(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name.contains("li")', dialect, name)
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_starts_with(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name.startsWith("A")', dialect, name)
        assert get_names(rows) == {"Alice"}

    def test_ends_with(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name.endsWith("e")', dialect, name)
        assert get_names(rows) == {"Alice", "Charlie", "Eve"}

    def test_size(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name.size() == 3', dialect, name)
        assert get_names(rows) == {"Bob", "Eve"}
