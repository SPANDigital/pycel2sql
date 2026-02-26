"""Integration tests for type casting (int(), string(), double())."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestTypeCast:
    def test_cast_to_int(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'int(height * 100) > 170', dialect, name)
        assert get_names(rows) == {"Bob", "Charlie"}

    def test_cast_to_string(self, db):
        conn, dialect, name = db
        rows = execute_cel(conn, 'string(age) == "30"', dialect, name)
        assert get_names(rows) == {"Alice"}
