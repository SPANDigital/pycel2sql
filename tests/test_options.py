"""Tests for the json_variables, column_aliases, and param_start_index options.

Ported from cel2sql Go (PR #113) and cel2sql4j (PR #9).
"""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql._errors import InvalidFieldNameError
from pycel2sql.dialect.bigquery import BigQueryDialect
from pycel2sql.dialect.duckdb import DuckDBDialect
from pycel2sql.dialect.mysql import MySQLDialect
from pycel2sql.dialect.sqlite import SQLiteDialect


class TestParamStartIndex:
    def test_default_starts_at_1(self):
        result = convert_parameterized("name == 'alice'")
        assert result.sql == "name = $1"
        assert result.parameters == ["alice"]

    def test_postgres_offset_5(self):
        result = convert_parameterized("name == 'alice'", param_start_index=5)
        assert result.sql == "name = $5"
        assert result.parameters == ["alice"]

    def test_bigquery_offset_10_two_params(self):
        result = convert_parameterized(
            "name == 'alice' && age > 30",
            dialect=BigQueryDialect(),
            param_start_index=10,
        )
        assert "@p10" in result.sql
        assert "@p11" in result.sql
        assert result.parameters == ["alice", 30]

    def test_postgres_two_params_count_up(self):
        result = convert_parameterized(
            "name == 'alice' && age > 30",
            param_start_index=7,
        )
        assert result.sql == "name = $7 AND age > $8"
        assert result.parameters == ["alice", 30]

    @pytest.mark.parametrize("dialect_cls", [MySQLDialect, SQLiteDialect])
    def test_positional_dialects_param_text_unchanged(self, dialect_cls):
        # MySQL/SQLite use ? placeholders that don't carry an index in the text,
        # but the parameters list ordering must still be preserved.
        result = convert_parameterized(
            "name == 'a' && age > 30",
            dialect=dialect_cls(),
            param_start_index=5,
        )
        assert result.sql.count("?") == 2
        assert result.parameters == ["a", 30]

    @pytest.mark.parametrize("bad", [0, -5, -100])
    def test_clamping_below_one(self, bad):
        result = convert_parameterized("name == 'a'", param_start_index=bad)
        assert result.sql == "name = $1"

    def test_many_params_count_up_past_9(self):
        # 11 params, starting at 5: should yield $5..$15
        expr = " && ".join([f"x{i} == 'v{i}'" for i in range(11)])
        result = convert_parameterized(expr, param_start_index=5)
        for i in range(5, 16):
            assert f"${i}" in result.sql
        assert len(result.parameters) == 11


class TestColumnAliases:
    def test_bare_ident(self):
        result = convert("name == 'a'", column_aliases={"name": "usr_name"})
        assert result == "usr_name = 'a'"

    def test_ident_inside_member_dot(self):
        # name.first → user_name.first (alias applied at the root ident emission)
        result = convert(
            "name.first == 'a'", column_aliases={"name": "user_name"}
        )
        assert result == "user_name.first = 'a'"

    def test_no_alias_passes_through(self):
        result = convert("name == 'a'", column_aliases={"other": "x"})
        assert result == "name = 'a'"

    def test_invalid_alias_raises(self):
        with pytest.raises(InvalidFieldNameError):
            convert("name == 'a'", column_aliases={"name": "bad name; DROP"})

    def test_alias_in_parameterized_form(self):
        result = convert_parameterized(
            "name == 'a'", column_aliases={"name": "usr_name"}
        )
        assert result.sql == "usr_name = $1"
        assert result.parameters == ["a"]


class TestJsonVariables:
    def test_postgres_single_level_dot(self):
        result = convert(
            "context.host == 'a'", json_variables={"context"}
        )
        assert result == "context->>'host' = 'a'"

    def test_postgres_bracket_notation(self):
        result = convert(
            'context["host"] == "a"', json_variables={"context"}
        )
        assert result == "context->>'host' = 'a'"

    def test_postgres_nested_three_levels(self):
        result = convert(
            "tags.corpus.section == 'a'", json_variables={"tags"}
        )
        assert result == "tags->'corpus'->>'section' = 'a'"

    def test_duckdb_single_level(self):
        result = convert(
            "context.host == 'a'",
            dialect=DuckDBDialect(),
            json_variables={"context"},
        )
        assert "context->>'host'" in result

    def test_bigquery_single_level(self):
        result = convert(
            "context.host == 'a'",
            dialect=BigQueryDialect(),
            json_variables={"context"},
        )
        assert "JSON_VALUE(context, '$.host')" in result

    def test_sqlite_single_level(self):
        result = convert(
            "context.host == 'a'",
            dialect=SQLiteDialect(),
            json_variables={"context"},
        )
        assert "json_extract(context, '$.host')" in result

    def test_mysql_single_level(self):
        result = convert(
            "context.host == 'a'",
            dialect=MySQLDialect(),
            json_variables={"context"},
        )
        # MySQL's JSON ->> operator extracts as text.
        assert result == "context->>'$.host' = 'a'"

    def test_no_json_variable_no_change(self):
        # Without json_variables, dot notation stays plain.
        result = convert("context.host == 'a'")
        assert result == "context.host = 'a'"

    def test_json_variable_with_alias(self):
        # Aliased name is the column root in the SQL output.
        result = convert(
            "context.host == 'a'",
            json_variables={"context"},
            column_aliases={"context": "ctx_jsonb"},
        )
        assert result == "ctx_jsonb->>'host' = 'a'"

    def test_comprehension_iter_var_not_treated_as_json_variable(self):
        # Iter var "x" colliding with a json_variable name should not be
        # treated as JSON inside the comprehension body.
        result = convert(
            "items.exists(x, x == 1)",
            json_variables={"x"},
        )
        # x should appear plainly inside the EXISTS body, not as JSON path.
        assert "->>" not in result

    def test_has_against_json_variable(self):
        # has(context.host) with context a json_variable should emit the
        # dialect's JSON-existence operator (Postgres `?` for JSONB).
        result = convert(
            "has(context.host)",
            json_variables={"context"},
        )
        assert result == "context ? 'host'"
