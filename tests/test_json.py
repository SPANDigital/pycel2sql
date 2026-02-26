"""JSON/JSONB tests."""

from pycel2sql import convert
from pycel2sql.schema import FieldSchema, Schema


def _json_schemas():
    return {
        "usr": Schema([
            FieldSchema(name="metadata", type="jsonb", is_json=True, is_jsonb=True),
        ]),
    }


class TestJSONFieldAccess:
    def test_simple_json_access(self):
        result = convert('usr.metadata.username == "john"', schemas=_json_schemas())
        assert result == "usr.metadata->>'username' = 'john'"

    def test_nested_json_access(self):
        result = convert('usr.metadata.settings.theme == "dark"', schemas=_json_schemas())
        assert result == "usr.metadata->'settings'->>'theme' = 'dark'"

    def test_deeply_nested_json(self):
        result = convert(
            'usr.metadata.a.b.c == "v"',
            schemas=_json_schemas(),
        )
        assert result == "usr.metadata->'a'->'b'->>'c' = 'v'"


class TestJSONHas:
    def test_has_json_field(self):
        result = convert("has(usr.metadata)", schemas=_json_schemas())
        assert result == "usr.metadata IS NOT NULL"
