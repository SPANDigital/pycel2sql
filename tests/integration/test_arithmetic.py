"""Integration tests for arithmetic expressions."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestArithmetic:
    def test_addition(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age + 5 > 33', dialect, name)
        assert get_names(rows) == {"Alice", "Charlie"}

    def test_multiplication(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age * 2 < 55', dialect, name)
        assert get_names(rows) == {"Bob", "Eve"}

    def test_subtraction(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age - 10 == 20', dialect, name)
        assert get_names(rows) == {"Alice"}
