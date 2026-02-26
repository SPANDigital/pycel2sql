"""Integration tests for the IN operator."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestInOperator:
    def test_int_in_list(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'age in [25, 30, 35]', dialect, name)
        assert get_names(rows) == {"Alice", "Bob", "Charlie"}

    def test_string_in_list(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'name in ["Alice", "Eve"]', dialect, name)
        assert get_names(rows) == {"Alice", "Eve"}
