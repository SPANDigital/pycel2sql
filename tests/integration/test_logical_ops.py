"""Integration tests for logical operators (AND, OR, NOT)."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestLogicalOps:
    def test_and(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age > 25 && active == true', dialect, name)
        assert get_names(rows) == {"Alice", "Diana"}

    def test_or(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age < 25 || age > 33', dialect, name)
        assert get_names(rows) == {"Eve", "Charlie"}

    def test_not(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, '!active', dialect, name)
        assert get_names(rows) == {"Charlie", "Eve"}

    def test_parenthesized(self, db):
        conn, dialect, name = db
        rows = execute_cel(
            conn,
            'age >= 25 && (active == true || email == null)',
            dialect,
            name,
        )
        assert get_names(rows) == {"Alice", "Bob", "Charlie", "Diana"}
