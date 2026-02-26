"""Schema tests."""

from pycel2sql.schema import FieldSchema, Schema


class TestSchema:
    def test_create_schema(self):
        schema = Schema([
            FieldSchema(name="id", type="integer"),
            FieldSchema(name="name", type="text"),
        ])
        assert len(schema) == 2

    def test_find_field(self):
        schema = Schema([
            FieldSchema(name="id", type="integer"),
            FieldSchema(name="tags", type="text", repeated=True),
        ])
        field = schema.find_field("tags")
        assert field is not None
        assert field.repeated is True

    def test_find_field_not_found(self):
        schema = Schema([FieldSchema(name="id", type="integer")])
        assert schema.find_field("nonexistent") is None

    def test_fields_property(self):
        fields = [
            FieldSchema(name="a", type="text"),
            FieldSchema(name="b", type="integer"),
        ]
        schema = Schema(fields)
        assert schema.fields == fields

    def test_json_field(self):
        schema = Schema([
            FieldSchema(name="meta", type="jsonb", is_json=True, is_jsonb=True),
        ])
        field = schema.find_field("meta")
        assert field is not None
        assert field.is_json is True
        assert field.is_jsonb is True
