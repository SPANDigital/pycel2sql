"""Domain types for index analysis."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class PatternType(enum.Enum):
    """Types of SQL patterns that may benefit from indexing."""

    COMPARISON = "comparison"
    JSON_ACCESS = "json_access"
    REGEX_MATCH = "regex_match"
    ARRAY_MEMBERSHIP = "array_membership"
    ARRAY_COMPREHENSION = "array_comprehension"
    JSON_ARRAY_COMPREHENSION = "json_array_comprehension"


class IndexType(enum.StrEnum):
    """Database index types."""

    BTREE = "btree"
    GIN = "gin"
    GIST = "gist"
    ART = "art"
    CLUSTERING = "clustering"
    SEARCH_INDEX = "search_index"
    FULLTEXT = "fulltext"


@dataclass(frozen=True)
class IndexPattern:
    """A detected pattern that may benefit from indexing."""

    column: str
    pattern: PatternType
    table_hint: str = ""


@dataclass(frozen=True)
class IndexRecommendation:
    """A specific index recommendation with DDL."""

    column: str
    index_type: IndexType
    expression: str
    reason: str
