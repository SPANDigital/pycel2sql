"""Index analysis tests."""

import pytest

from pycel2sql import analyze
from pycel2sql._analysis_types import IndexType, PatternType
from pycel2sql.dialect.postgres import PostgresDialect
from pycel2sql.schema import FieldSchema, Schema


@pytest.fixture
def pg():
    return PostgresDialect()


@pytest.fixture
def json_schema():
    return {"t": Schema([FieldSchema("data", is_json=True, is_jsonb=True)])}


@pytest.fixture
def array_schema():
    return {"t": Schema([FieldSchema("tags", repeated=True)])}


class TestComparisonOperations:
    def test_equality_recommends_btree(self, pg):
        result = analyze('name == "alice"', dialect=pg)
        assert len(result.recommendations) == 1
        rec = result.recommendations[0]
        assert rec.column == "name"
        assert rec.index_type == IndexType.BTREE
        assert "CREATE INDEX" in rec.expression

    def test_range_comparison_recommends_btree(self, pg):
        result = analyze("age > 18", dialect=pg)
        assert len(result.recommendations) == 1
        assert result.recommendations[0].index_type == IndexType.BTREE

    def test_less_than(self, pg):
        result = analyze("price < 100", dialect=pg)
        assert any(r.column == "price" for r in result.recommendations)

    def test_greater_equal(self, pg):
        result = analyze("score >= 90", dialect=pg)
        assert any(r.column == "score" for r in result.recommendations)

    def test_not_equal(self, pg):
        result = analyze("status != 0", dialect=pg)
        assert any(r.column == "status" for r in result.recommendations)

    def test_sql_also_generated(self, pg):
        result = analyze("age > 18", dialect=pg)
        assert result.sql == "age > 18"


class TestJSONPathOperations:
    def test_json_field_access_recommends_gin(self, pg, json_schema):
        result = analyze("t.data.name", dialect=pg, schemas=json_schema)
        recs = [r for r in result.recommendations if r.column == "data"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN
        assert "GIN" in recs[0].expression

    def test_json_nested_access(self, pg, json_schema):
        result = analyze('t.data.address == "home"', dialect=pg, schemas=json_schema)
        recs = [r for r in result.recommendations if r.column == "data"]
        assert len(recs) >= 1
        assert any(r.index_type == IndexType.GIN for r in recs)


class TestRegexOperations:
    def test_matches_recommends_gin_trgm(self, pg):
        result = analyze('name.matches("^[a-z]+$")', dialect=pg)
        recs = [r for r in result.recommendations if r.column == "name"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN
        assert "trgm" in recs[0].expression

    def test_function_style_matches(self, pg):
        result = analyze('matches(name, "^test")', dialect=pg)
        recs = [r for r in result.recommendations if r.column == "name"]
        assert len(recs) >= 1


class TestArrayOperations:
    def test_array_membership_recommends_gin(self, pg, array_schema):
        result = analyze("x in t.tags", dialect=pg, schemas=array_schema)
        # The 'in' operator should detect array membership
        assert len(result.recommendations) >= 1


class TestComprehensions:
    def test_exists_comprehension(self, pg, array_schema):
        result = analyze("t.tags.exists(x, x > 5)", dialect=pg, schemas=array_schema)
        recs = [r for r in result.recommendations if r.column == "tags"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN

    def test_all_comprehension(self, pg, array_schema):
        result = analyze("t.tags.all(x, x > 0)", dialect=pg, schemas=array_schema)
        recs = [r for r in result.recommendations if r.column == "tags"]
        assert len(recs) == 1

    def test_map_comprehension(self, pg, array_schema):
        result = analyze("t.tags.map(x, x + 1)", dialect=pg, schemas=array_schema)
        recs = [r for r in result.recommendations if r.column == "tags"]
        assert len(recs) == 1

    def test_filter_comprehension(self, pg, array_schema):
        result = analyze("t.tags.filter(x, x > 0)", dialect=pg, schemas=array_schema)
        recs = [r for r in result.recommendations if r.column == "tags"]
        assert len(recs) == 1

    def test_json_array_comprehension(self, pg, json_schema):
        result = analyze("t.data.exists(x, x > 0)", dialect=pg, schemas=json_schema)
        recs = [r for r in result.recommendations if r.column == "data"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN


class TestMultipleRecommendations:
    def test_multiple_columns(self, pg):
        result = analyze('name == "alice" && age > 18', dialect=pg)
        columns = {r.column for r in result.recommendations}
        assert "name" in columns
        assert "age" in columns

    def test_three_columns(self, pg):
        result = analyze('name == "a" && age > 10 && score < 100', dialect=pg)
        assert len(result.recommendations) >= 3

    def test_deduplication(self, pg):
        # Same column in two comparisons -> single recommendation
        result = analyze("age > 10 && age < 30", dialect=pg)
        age_recs = [r for r in result.recommendations if r.column == "age"]
        assert len(age_recs) == 1


class TestNoRecommendations:
    def test_constant_expression(self, pg):
        result = analyze("1 + 2 == 3", dialect=pg)
        assert len(result.recommendations) == 0

    def test_literal_true(self, pg):
        result = analyze("true", dialect=pg)
        assert len(result.recommendations) == 0

    def test_null_literal(self, pg):
        result = analyze("null", dialect=pg)
        assert len(result.recommendations) == 0


class TestIndexPriority:
    def test_gin_replaces_btree_for_same_column(self, pg, json_schema):
        # JSON access (GIN) + comparison (BTREE) on same effective column
        result = analyze('t.data.num > 5', dialect=pg, schemas=json_schema)
        recs = [r for r in result.recommendations if r.column == "data"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN

    def test_regex_replaces_comparison(self, pg):
        result = analyze('name == "a" && name.matches("^test")', dialect=pg)
        recs = [r for r in result.recommendations if r.column == "name"]
        assert len(recs) == 1
        assert recs[0].index_type == IndexType.GIN


class TestPostgresAdvisorDirectly:
    def test_comparison_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("col", PatternType.COMPARISON, "my_table"))
        assert rec is not None
        assert rec.index_type == IndexType.BTREE
        assert "my_table" in rec.expression

    def test_json_access_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("data", PatternType.JSON_ACCESS, "users"))
        assert rec is not None
        assert rec.index_type == IndexType.GIN
        assert "GIN" in rec.expression

    def test_regex_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("name", PatternType.REGEX_MATCH, "users"))
        assert rec is not None
        assert "trgm" in rec.expression

    def test_array_membership_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("tags", PatternType.ARRAY_MEMBERSHIP, "posts"))
        assert rec is not None
        assert rec.index_type == IndexType.GIN

    def test_array_comprehension_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("scores", PatternType.ARRAY_COMPREHENSION))
        assert rec is not None
        assert rec.index_type == IndexType.GIN

    def test_json_array_comprehension_pattern(self, pg):
        from pycel2sql._analysis_types import IndexPattern
        rec = pg.recommend_index(IndexPattern("data", PatternType.JSON_ARRAY_COMPREHENSION))
        assert rec is not None
        assert rec.index_type == IndexType.GIN

    def test_supported_patterns(self, pg):
        patterns = pg.supported_patterns()
        assert PatternType.COMPARISON in patterns
        assert PatternType.JSON_ACCESS in patterns
        assert PatternType.REGEX_MATCH in patterns
        assert len(patterns) == 6
