"""Integration tests for ternary (conditional) expressions."""

from __future__ import annotations

import pytest

from tests.integration.conftest import execute_cel, get_names


pytestmark = pytest.mark.integration


class TestTernary:
    def test_conditional(self, db):
        conn, dialect, name = db
        rows = execute_cel(
            conn,
            '(age > 30 ? "senior" : "junior") == "senior"',
            dialect,
            name,
        )
        assert get_names(rows) == {"Charlie"}
