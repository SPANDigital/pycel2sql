"""Apache Spark dialect-specific tests.

Mirrors the Java Cel2SqlSparkTest surface from cel2sql4j PR #10.
"""

import pytest

from pycel2sql import convert, convert_parameterized
from pycel2sql._errors import (
    InvalidFieldNameError,
    InvalidRegexPatternError,
    UnsupportedDialectFeatureError,
)
from pycel2sql.dialect.spark import (
    SparkDialect,
    _convert_re2_to_spark,
    _validate_spark_field_name,
)
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def d():
    return SparkDialect()


class TestSparkLiterals:
    def test_string_literal(self, d):
        assert convert('name == "alice"', dialect=d) == "name = 'alice'"

    def test_int_literal(self, d):
        assert convert("age == 25", dialect=d) == "age = 25"

    def test_bool_literal(self, d):
        assert convert("active == true", dialect=d) == "active IS TRUE"

    def test_bytes_literal_hex(self, d):
        result = convert('b"abc" == data', dialect=d)
        # Hex form: X'<HEX>'
        assert "X'" in result and "61" in result.upper()


class TestSparkParams:
    def test_positional_placeholder(self, d):
        result = convert_parameterized('name == "alice"', dialect=d)
        assert result.sql == "name = ?"
        assert result.parameters == ["alice"]

    def test_multiple_positional(self, d):
        result = convert_parameterized("age > 10 && age < 30", dialect=d)
        assert result.sql == "age > ? AND age < ?"
        assert result.parameters == [10, 30]

    def test_param_start_index_is_a_no_op_for_text(self, d):
        result = convert_parameterized(
            "age > 10 && age < 30", dialect=d, param_start_index=5
        )
        # Spark uses positional ? — placeholder text is identical.
        assert result.sql == "age > ? AND age < ?"
        assert result.parameters == [10, 30]


class TestSparkArrays:
    def test_array_literal(self, d):
        assert convert("[1, 2, 3]", dialect=d) == "array(1, 2, 3)"

    def test_array_membership_arg_order(self, d):
        # Spark: array_contains(arr, elem) — arg order swap.
        result = convert("x in [1, 2, 3]", dialect=d)
        assert result == "array_contains(array(1, 2, 3), x)"

    def test_array_index_const_zero_based(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[0]", dialect=d, schemas=schemas)
        assert result == "t.arr[0]"

    def test_array_index_const_nonzero(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr[2]", dialect=d, schemas=schemas)
        assert result == "t.arr[2]"

    def test_array_length(self, d):
        schemas = {"t": Schema([FieldSchema("arr", repeated=True)])}
        result = convert("t.arr.size()", dialect=d, schemas=schemas)
        assert result == "COALESCE(size(t.arr), 0)"

    def test_empty_typed_array(self, d):
        result = convert('"a,b".split(",", 0)', dialect=d)
        assert "CAST(array() AS ARRAY<" in result


class TestSparkStringFunctions:
    def test_contains_locate_arg_order(self, d):
        # Spark: LOCATE(needle, haystack) > 0 (arg order: needle then haystack).
        result = convert('name.contains("test")', dialect=d)
        assert result == "LOCATE('test', name) > 0"

    def test_starts_with_escape(self, d):
        result = convert('name.startsWith("a")', dialect=d)
        assert "LIKE 'a%'" in result
        assert "ESCAPE '\\\\'" in result

    def test_concat_for_string_plus(self, d):
        result = convert('"a" + "b" == "ab"', dialect=d)
        assert "concat('a', 'b')" in result

    def test_split_basic(self, d):
        result = convert('"a,b,c".split(",")', dialect=d)
        assert "split('a,b,c', ',')" in result

    def test_split_with_limit(self, d):
        result = convert('"a,b,c".split(",", 3)', dialect=d)
        assert "split('a,b,c', ',', 3)" in result

    def test_join(self, d):
        result = convert('["a", "b"].join(",")', dialect=d)
        assert "array_join(array('a', 'b'), ',')" in result

    def test_format_uses_format_string(self, d):
        result = convert("'%s = %d'.format([name, 10])", dialect=d)
        assert result == "format_string('%s = %s', name, 10)"


class TestSparkRegex:
    def test_basic_match_rlike(self, d):
        result = convert('name.matches("^[a-z]+$")', dialect=d)
        assert result == "name RLIKE '^[a-z]+$'"

    def test_inline_case_insensitive_passthrough(self, d):
        # Spark honours (?i) inline; we pass it through verbatim.
        result = convert('name.matches("(?i)test")', dialect=d)
        assert result == "name RLIKE '(?i)test'"

    def test_lookahead_rejected(self, d):
        with pytest.raises(InvalidRegexPatternError):
            convert('name.matches("(?=test)abc")', dialect=d)

    def test_lookbehind_rejected(self, d):
        with pytest.raises(InvalidRegexPatternError):
            convert('name.matches("(?<=test)abc")', dialect=d)

    def test_named_group_rejected(self, d):
        with pytest.raises(InvalidRegexPatternError):
            convert('name.matches("(?P<x>abc)")', dialect=d)

    def test_quantified_alternation_rejected(self, d):
        with pytest.raises(InvalidRegexPatternError):
            convert('name.matches("(a|b)+")', dialect=d)

    def test_inline_flag_other_than_i_rejected(self, d):
        with pytest.raises(InvalidRegexPatternError):
            convert('name.matches("(?m)foo")', dialect=d)

    def test_overlong_pattern_rejected_at_validator(self):
        big = "a" * 600
        with pytest.raises(InvalidRegexPatternError):
            _convert_re2_to_spark(big)

    def test_nested_quantifier_rejected_at_validator(self):
        # Direct validator test (the converter's deeper machinery may rewrite
        # before reaching here in some forms; the validator is the source of
        # truth for ReDoS guarding).
        with pytest.raises(InvalidRegexPatternError):
            _convert_re2_to_spark("(a+)+")


class TestSparkTimestamps:
    def test_int_of_timestamp_uses_unix_timestamp(self, d):
        # Bare 'ts' identifier hits the timestamp-name heuristic.
        result = convert("int(ts) == 100", dialect=d)
        assert result == "UNIX_TIMESTAMP(ts) = 100"

    def test_timestamp_cast(self, d):
        result = convert('timestamp("2024-01-01T00:00:00Z") == ts', dialect=d)
        assert "CAST(" in result and "AS TIMESTAMP" in result

    def test_duration_interval(self, d):
        result = convert('ts + duration("24h") == ts2', dialect=d)
        assert "INTERVAL" in result

    def test_extract_year(self, d):
        result = convert("ts.getFullYear() == 2024", dialect=d)
        assert "EXTRACT(YEAR FROM ts)" in result

    def test_dow_special_case(self, d):
        result = convert("ts.getDayOfWeek() == 0", dialect=d)
        assert "(dayofweek(ts) - 1)" in result


class TestSparkJSON:
    def test_json_field_access(self, d):
        schemas = {"t": Schema([FieldSchema("data", is_json=True)])}
        result = convert('t.data.field == "x"', dialect=d, schemas=schemas)
        assert "get_json_object(t.data, '$.field')" in result

    def test_json_existence_via_has(self, d):
        result = convert("has(context.host)", dialect=d, json_variables={"context"})
        assert result == "get_json_object(context, '$.host') IS NOT NULL"

    def test_json_variable_dot_access(self, d):
        result = convert("context.host == 'a'", dialect=d, json_variables={"context"})
        assert result == "get_json_object(context, '$.host') = 'a'"

    def test_json_array_membership_dialect_method_raises(self, d):
        # Direct dialect-level call: the converter doesn't currently route
        # `in` against a JSON-array field through write_json_array_membership,
        # but the Spark dialect must raise if it ever does.
        from io import StringIO

        with pytest.raises(UnsupportedDialectFeatureError):
            d.write_json_array_membership(StringIO(), "x", lambda: None)

    def test_nested_json_array_membership_dialect_method_raises(self, d):
        from io import StringIO

        with pytest.raises(UnsupportedDialectFeatureError):
            d.write_nested_json_array_membership(StringIO(), lambda: None)


class TestSparkValidation:
    def test_reserved_keyword_rejected(self, d):
        with pytest.raises(InvalidFieldNameError):
            convert("select == 1", dialect=d)

    def test_overlong_field_rejected_at_validator(self):
        # Direct dialect-validator test (the generic field-name validator
        # caps at 63 chars and would reject 130 first; testing the
        # Spark-specific check directly).
        with pytest.raises(InvalidFieldNameError):
            _validate_spark_field_name("a" * 130)

    def test_empty_field_rejected_at_validator(self):
        with pytest.raises(InvalidFieldNameError):
            _validate_spark_field_name("")


class TestSparkTypeCasting:
    def test_cel_int_to_bigint(self, d):
        # Plain int(x) on a non-timestamp ident emits CAST(... AS BIGINT).
        result = convert("int(x) == 1", dialect=d)
        assert result == "CAST(x AS BIGINT) = 1"

    def test_cel_string_cast(self, d):
        result = convert('string(x) == "a"', dialect=d)
        assert result == "CAST(x AS STRING) = 'a'"

    def test_cel_bool_cast(self, d):
        result = convert("bool(x) == true", dialect=d)
        assert "CAST(x AS BOOLEAN)" in result


class TestSparkComprehensions:
    def test_exists_uses_explode_and_collect_list(self, d):
        result = convert("[1, 2, 3].exists(x, x > 1)", dialect=d)
        # The comprehension scaffolding should reference EXPLODE.
        assert "EXPLODE" in result


class TestSparkStructs:
    def test_struct_literal(self, d):
        # CEL struct/map literal: {a: 1, b: 2}
        result = convert("{'a': 1, 'b': 2} == x", dialect=d)
        assert "struct(" in result.lower()
