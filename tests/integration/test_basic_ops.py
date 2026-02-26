"""Integration tests for basic comparison and null/boolean operations."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestEquality:
    def test_string_equality(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name == "Alice"', dialect, name)
        assert get_names(rows) == {"Alice"}

    def test_string_inequality(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age != 30', dialect, name)
        assert get_names(rows) == {"Bob", "Charlie", "Diana", "Eve"}


class TestComparisons:
    def test_less_than(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age < 28', dialect, name)
        assert get_names(rows) == {"Bob", "Eve"}

    def test_greater_equal(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age >= 30', dialect, name)
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_float_comparison(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'height >= 1.70', dialect, name)
        assert get_names(rows) == {"Bob", "Charlie", "Eve"}


class TestNulls:
    def test_is_null(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'email == null', dialect, name)
        assert get_names(rows) == {"Charlie"}

    def test_is_not_null(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'email != null', dialect, name)
        assert get_names(rows) == {"Alice", "Bob", "Diana", "Eve"}


class TestBooleans:
    def test_true(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'active == true', dialect, name)
        assert get_names(rows) == {"Alice", "Bob", "Diana"}

    def test_false(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'active == false', dialect, name)
        assert get_names(rows) == {"Charlie", "Eve"}
