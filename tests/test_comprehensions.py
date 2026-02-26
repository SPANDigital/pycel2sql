"""Comprehension tests - ported from comprehensions_test.go."""

from pycel2sql import convert
from pycel2sql.schema import FieldSchema, Schema


class TestComprehensionBasics:
    def test_all(self):
        result = convert("[1, 2, 3, 4].all(x, x > 0)")
        assert result == "NOT EXISTS (SELECT 1 FROM UNNEST(ARRAY[1, 2, 3, 4]) AS x WHERE NOT (x > 0))"

    def test_exists(self):
        result = convert("[1, 2, 3, 4].exists(x, x > 3)")
        assert result == "EXISTS (SELECT 1 FROM UNNEST(ARRAY[1, 2, 3, 4]) AS x WHERE x > 3)"

    def test_exists_one(self):
        result = convert("[1, 2, 3].exists_one(x, x > 2)")
        assert result == "(SELECT COUNT(*) FROM UNNEST(ARRAY[1, 2, 3]) AS x WHERE x > 2) = 1"

    def test_map(self):
        result = convert("[1, 2, 3].map(x, x * 2)")
        assert "ARRAY(SELECT x * 2 FROM UNNEST(ARRAY[1, 2, 3]) AS x)" == result

    def test_filter(self):
        result = convert("[1, 2, 3, 4].filter(x, x > 2)")
        assert result == "ARRAY(SELECT x FROM UNNEST(ARRAY[1, 2, 3, 4]) AS x WHERE x > 2)"


class TestComprehensionWithSchemaFields:
    def _schemas(self):
        return {
            "data": Schema([
                FieldSchema(name="tags", type="text", repeated=True),
            ]),
        }

    def test_exists_with_field(self):
        result = convert(
            "data.tags.exists(t, t == 'target')",
            schemas=self._schemas(),
        )
        assert result == "EXISTS (SELECT 1 FROM UNNEST(data.tags) AS t WHERE t = 'target')"

    def test_all_with_field(self):
        result = convert(
            "data.tags.all(t, t.size() > 0)",
            schemas=self._schemas(),
        )
        assert result == "NOT EXISTS (SELECT 1 FROM UNNEST(data.tags) AS t WHERE NOT (LENGTH(t) > 0))"

    def test_filter_with_field(self):
        result = convert(
            "data.tags.filter(t, t.startsWith('a')).size() > 0",
            schemas=self._schemas(),
        )
        assert result == "COALESCE(ARRAY_LENGTH(ARRAY(SELECT t FROM UNNEST(data.tags) AS t WHERE t LIKE 'a%' ESCAPE E'\\\\'), 1), 0) > 0"

    def test_map_with_field(self):
        result = convert(
            "data.tags.map(t, t.upperAscii())",
            schemas=self._schemas(),
        )
        assert result == "ARRAY(SELECT UPPER(t) FROM UNNEST(data.tags) AS t)"


class TestComprehensionStringFunctions:
    def _schemas(self):
        return {
            "data": Schema([
                FieldSchema(name="tags", type="text", repeated=True),
            ]),
        }

    def test_size_in_exists_one(self):
        result = convert(
            "data.tags.exists_one(t, t.size() == 10)",
            schemas=self._schemas(),
        )
        assert result == "(SELECT COUNT(*) FROM UNNEST(data.tags) AS t WHERE LENGTH(t) = 10) = 1"

    def test_upper_in_map(self):
        result = convert(
            "data.tags.map(t, t.upperAscii())",
            schemas=self._schemas(),
        )
        assert result == "ARRAY(SELECT UPPER(t) FROM UNNEST(data.tags) AS t)"

    def test_lower_in_map(self):
        result = convert(
            "data.tags.map(t, t.lowerAscii())",
            schemas=self._schemas(),
        )
        assert result == "ARRAY(SELECT LOWER(t) FROM UNNEST(data.tags) AS t)"

    def test_size_in_filter(self):
        result = convert(
            "data.tags.filter(t, t.size() > 5)",
            schemas=self._schemas(),
        )
        assert result == "ARRAY(SELECT t FROM UNNEST(data.tags) AS t WHERE LENGTH(t) > 5)"


class TestSplitInComprehensions:
    def test_split_in_exists(self):
        result = convert("person.csv.split(',').exists(x, x == 'target')")
        assert result == "EXISTS (SELECT 1 FROM UNNEST(STRING_TO_ARRAY(person.csv, ',')) AS x WHERE x = 'target')"

    def test_split_in_all(self):
        result = convert("person.csv.split(',').all(x, x.size() > 0)")
        assert result == "NOT EXISTS (SELECT 1 FROM UNNEST(STRING_TO_ARRAY(person.csv, ',')) AS x WHERE NOT (LENGTH(x) > 0))"

    def test_split_in_filter(self):
        result = convert("person.csv.split(',').filter(x, x.startsWith('a')).size() > 0")
        assert result == "COALESCE(ARRAY_LENGTH(ARRAY(SELECT x FROM UNNEST(STRING_TO_ARRAY(person.csv, ',')) AS x WHERE x LIKE 'a%' ESCAPE E'\\\\'), 1), 0) > 0"

    def test_split_in_map(self):
        result = convert("person.csv.split(',').map(x, x.upperAscii())")
        assert result == "ARRAY(SELECT UPPER(x) FROM UNNEST(STRING_TO_ARRAY(person.csv, ',')) AS x)"


class TestJoinWithComprehensions:
    def _schemas(self):
        return {
            "person": Schema([
                FieldSchema(name="tags", type="text", repeated=True),
            ]),
        }

    def test_join_filtered(self):
        result = convert(
            "person.tags.filter(t, t.startsWith('a')).join(',') == 'apple,apricot'",
            schemas=self._schemas(),
        )
        assert result == "ARRAY_TO_STRING(ARRAY(SELECT t FROM UNNEST(person.tags) AS t WHERE t LIKE 'a%' ESCAPE E'\\\\'), ',', '') = 'apple,apricot'"

    def test_join_mapped(self):
        result = convert(
            "person.tags.map(t, t.upperAscii()).join(',') == 'TAG1,TAG2'",
            schemas=self._schemas(),
        )
        assert result == "ARRAY_TO_STRING(ARRAY(SELECT UPPER(t) FROM UNNEST(person.tags) AS t), ',', '') = 'TAG1,TAG2'"
