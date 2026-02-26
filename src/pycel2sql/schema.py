"""Schema types for CEL-to-SQL conversion."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FieldSchema:
    """Schema for a single field/column."""

    name: str
    type: str = "text"
    repeated: bool = False
    dimensions: int = 0
    schema: list[FieldSchema] = field(default_factory=list)
    is_json: bool = False
    is_jsonb: bool = False
    element_type: str = ""


class Schema:
    """Table schema with O(1) field lookup."""

    def __init__(self, fields: list[FieldSchema]) -> None:
        self._fields = list(fields)
        self._index: dict[str, FieldSchema] = {f.name: f for f in fields}

    @property
    def fields(self) -> list[FieldSchema]:
        return list(self._fields)

    def find_field(self, name: str) -> FieldSchema | None:
        return self._index.get(name)

    def __len__(self) -> int:
        return len(self._fields)
