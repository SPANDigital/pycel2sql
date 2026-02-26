"""Integration tests for parameterized queries with real DB execution."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel_parameterized, get_names


pytestmark = pytest.mark.integration


class TestParameterized:
    def test_string_and_int_params(self, db):
        conn, dialect, name = db
        rows = execute_cel_parameterized(
            conn, 'name == "Alice" && age > 20', dialect, name,
        )
        assert get_names(rows) == {"Alice"}

    def test_in_list_params(self, db):
        conn, dialect, name = db
        rows = execute_cel_parameterized(
            conn, 'age in [25, 30]', dialect, name,
        )
        assert get_names(rows) == {"Alice", "Bob"}
