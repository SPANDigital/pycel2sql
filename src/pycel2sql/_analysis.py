"""Index analysis for CEL expressions.

Walks the CEL parse tree to detect index-worthy patterns
and generates index recommendations via the dialect's IndexAdvisor.
"""

from __future__ import annotations

from typing import Any

from lark import Token, Tree
from lark.visitors import Interpreter

from pycel2sql._analysis_types import (
    IndexPattern,
    IndexRecommendation,
    IndexType,
    PatternType,
)
from pycel2sql.dialect._base import IndexAdvisor
from pycel2sql.schema import Schema


class IndexAnalyzer(Interpreter):
    """Lightweight Lark Interpreter that walks the parse tree detecting index-worthy patterns.

    Does NOT generate SQL - only collects patterns.
    """

    def __init__(
        self,
        advisor: IndexAdvisor,
        schemas: dict[str, Schema] | None = None,
    ) -> None:
        self._advisor = advisor
        self._schemas = schemas or {}
        self._patterns: dict[str, IndexPattern] = {}

    @property
    def patterns(self) -> list[IndexPattern]:
        return list(self._patterns.values())

    def visit(self, tree: Tree) -> Any:
        if isinstance(tree, Token):
            return None
        return super().visit(tree)

    def _add_pattern(self, pattern: IndexPattern) -> None:
        """Add a pattern, with specialized types replacing basic types."""
        key = pattern.column
        existing = self._patterns.get(key)
        if existing is None:
            self._patterns[key] = pattern
        elif _pattern_priority(pattern.pattern) > _pattern_priority(existing.pattern):
            self._patterns[key] = pattern

    # --- Tree walking ---

    def _visit_children(self, tree: Tree) -> None:
        for child in tree.children:
            if isinstance(child, Tree):
                self.visit(child)

    # Top-level passthrough handlers
    def expr(self, tree: Tree) -> None:
        self._visit_children(tree)

    def conditionalor(self, tree: Tree) -> None:
        self._visit_children(tree)

    def conditionaland(self, tree: Tree) -> None:
        self._visit_children(tree)

    def addition(self, tree: Tree) -> None:
        self._visit_children(tree)

    def multiplication(self, tree: Tree) -> None:
        self._visit_children(tree)

    def unary(self, tree: Tree) -> None:
        self._visit_children(tree)

    def member(self, tree: Tree) -> None:
        self._visit_children(tree)

    def primary(self, tree: Tree) -> None:
        self._visit_children(tree)

    def paren_expr(self, tree: Tree) -> None:
        self._visit_children(tree)

    def literal(self, tree: Tree) -> None:
        pass

    def list_lit(self, tree: Tree) -> None:
        self._visit_children(tree)

    def map_lit(self, tree: Tree) -> None:
        self._visit_children(tree)

    def exprlist(self, tree: Tree) -> None:
        self._visit_children(tree)

    def mapinits(self, tree: Tree) -> None:
        self._visit_children(tree)

    def fieldinits(self, tree: Tree) -> None:
        self._visit_children(tree)

    def ident(self, tree: Tree) -> None:
        pass

    def ident_arg(self, tree: Tree) -> None:
        func_name = str(tree.children[0])
        args_node = tree.children[1] if len(tree.children) > 1 else None
        args = args_node.children if args_node is not None else []

        if func_name == "matches" and len(args) >= 1:
            col = self._extract_column_name(args[0])
            if col:
                table = self._extract_table_name(args[0])
                self._add_pattern(IndexPattern(
                    column=col,
                    pattern=PatternType.REGEX_MATCH,
                    table_hint=table,
                ))

        self._visit_children(tree)

    def dot_ident_arg(self, tree: Tree) -> None:
        self._visit_children(tree)

    def dot_ident(self, tree: Tree) -> None:
        pass

    def member_index(self, tree: Tree) -> None:
        self._visit_children(tree)

    def member_object(self, tree: Tree) -> None:
        pass

    # Operator prefix handlers
    def addition_add(self, tree: Tree) -> None:
        self._visit_children(tree)

    def addition_sub(self, tree: Tree) -> None:
        self._visit_children(tree)

    def multiplication_mul(self, tree: Tree) -> None:
        self._visit_children(tree)

    def multiplication_div(self, tree: Tree) -> None:
        self._visit_children(tree)

    def multiplication_mod(self, tree: Tree) -> None:
        self._visit_children(tree)

    def unary_not(self, tree: Tree) -> None:
        pass

    def unary_neg(self, tree: Tree) -> None:
        pass

    # --- Key detection points ---

    def relation(self, tree: Tree) -> None:
        """Detect comparison operators -> COMPARISON pattern."""
        children = tree.children
        if len(children) == 2:
            op_node = children[0]
            if isinstance(op_node, Tree):
                lhs = op_node.children[0] if op_node.children else None
                rhs = children[1]
                # Extract column names from both sides
                for node in (lhs, rhs):
                    if node is not None:
                        col = self._extract_column_name(node)
                        if col:
                            table = self._extract_table_name(node)
                            self._add_pattern(IndexPattern(
                                column=col,
                                pattern=PatternType.COMPARISON,
                                table_hint=table,
                            ))

                # Check for IN operator - array membership
                if isinstance(op_node, Tree) and op_node.data == "relation_in":
                    if lhs is not None:
                        col = self._extract_column_name(lhs)
                        if col:
                            table = self._extract_table_name(lhs)
                            self._add_pattern(IndexPattern(
                                column=col,
                                pattern=PatternType.ARRAY_MEMBERSHIP,
                                table_hint=table,
                            ))
        self._visit_children(tree)

    # Relation prefix handlers
    def relation_eq(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_ne(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_lt(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_le(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_gt(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_ge(self, tree: Tree) -> None:
        self._visit_children(tree)

    def relation_in(self, tree: Tree) -> None:
        self._visit_children(tree)

    def member_dot(self, tree: Tree) -> None:
        """Detect JSON field access -> JSON_ACCESS pattern."""
        obj = tree.children[0]
        field_name = str(tree.children[1])

        table_name = self._get_root_ident(obj)
        if table_name:
            first_field = self._get_first_field(obj, field_name)
            if self._is_field_json(table_name, first_field):
                self._add_pattern(IndexPattern(
                    column=first_field,
                    pattern=PatternType.JSON_ACCESS,
                    table_hint=table_name,
                ))

        self._visit_children(tree)

    def member_dot_arg(self, tree: Tree) -> None:
        """Detect matches() -> REGEX_MATCH and comprehensions -> ARRAY/JSON_ARRAY_COMPREHENSION."""
        obj = tree.children[0]
        method_name = str(tree.children[1])

        if method_name == "matches":
            col = self._extract_column_name(obj)
            if col:
                table = self._extract_table_name(obj)
                self._add_pattern(IndexPattern(
                    column=col,
                    pattern=PatternType.REGEX_MATCH,
                    table_hint=table,
                ))

        if method_name in ("all", "exists", "exists_one", "map", "filter"):
            col = self._extract_column_name(obj)
            if col:
                table = self._extract_table_name(obj)
                # Determine if this is a JSON array comprehension
                root = self._get_root_ident(obj)
                first_field = self._get_first_field(obj, col) if root else col
                if root and self._is_field_json(root, first_field):
                    pattern_type = PatternType.JSON_ARRAY_COMPREHENSION
                else:
                    pattern_type = PatternType.ARRAY_COMPREHENSION
                self._add_pattern(IndexPattern(
                    column=first_field if root else col,
                    pattern=pattern_type,
                    table_hint=table,
                ))

        self._visit_children(tree)

    # --- Helper methods ---

    def _extract_column_name(self, tree: Tree | Token) -> str | None:
        """Extract a column/field name from a tree node."""
        node = tree
        while isinstance(node, Tree):
            if node.data == "ident":
                return str(node.children[0])
            if node.data == "member_dot":
                return str(node.children[1])
            if len(node.children) == 1:
                node = node.children[0]
            else:
                return None
        if isinstance(node, Token) and node.type == "IDENT":
            return str(node)
        return None

    def _extract_table_name(self, tree: Tree | Token) -> str:
        """Extract the root table name from a tree."""
        root = self._get_root_ident(tree)
        return root or ""

    def _get_root_ident(self, tree: Tree | Token) -> str | None:
        node = tree
        while isinstance(node, Tree):
            if node.data == "ident":
                return str(node.children[0])
            if node.data == "member_dot":
                node = node.children[0]
            elif len(node.children) >= 1:
                node = node.children[0]
            else:
                return None
        if isinstance(node, Token) and node.type == "IDENT":
            return str(node)
        return None

    def _get_first_field(self, obj: Tree | Token, fallback: str) -> str:
        node = obj
        while isinstance(node, Tree):
            if node.data == "member_dot":
                child = node.children[0]
                inner = child
                while isinstance(inner, Tree) and inner.data in ("member", "primary") and len(inner.children) == 1:
                    inner = inner.children[0]
                if isinstance(inner, Tree) and inner.data == "ident":
                    return str(node.children[1])
                node = node.children[0]
            elif len(node.children) == 1:
                node = node.children[0]
            else:
                break
        return fallback

    def _is_field_json(self, table_name: str, field_name: str) -> bool:
        schema = self._schemas.get(table_name)
        if not schema:
            return False
        field = schema.find_field(field_name)
        return field is not None and (field.is_json or field.is_jsonb)


def _pattern_priority(pattern: PatternType) -> int:
    """Higher value = more specialized index type."""
    priorities = {
        PatternType.COMPARISON: 1,
        PatternType.ARRAY_MEMBERSHIP: 2,
        PatternType.REGEX_MATCH: 3,
        PatternType.JSON_ACCESS: 3,
        PatternType.ARRAY_COMPREHENSION: 3,
        PatternType.JSON_ARRAY_COMPREHENSION: 3,
    }
    return priorities.get(pattern, 0)


def analyze_patterns(
    tree: Tree,
    advisor: IndexAdvisor,
    schemas: dict[str, Schema] | None = None,
) -> list[IndexRecommendation]:
    """Analyze a CEL parse tree for index-worthy patterns.

    Args:
        tree: Parsed CEL expression (lark.Tree).
        advisor: IndexAdvisor to generate recommendations.
        schemas: Optional table schemas.

    Returns:
        Deduplicated list of index recommendations.
    """
    analyzer = IndexAnalyzer(advisor, schemas)
    analyzer.visit(tree)

    recommendations: dict[str, IndexRecommendation] = {}
    for pattern in analyzer.patterns:
        rec = advisor.recommend_index(pattern)
        if rec is not None:
            key = rec.column
            existing = recommendations.get(key)
            if existing is None:
                recommendations[key] = rec
            elif _index_priority(rec.index_type) > _index_priority(existing.index_type):
                recommendations[key] = rec

    return list(recommendations.values())


def _index_priority(index_type: IndexType) -> int:
    """Higher value = more specialized."""
    priorities = {
        IndexType.BTREE: 1,
        IndexType.ART: 1,
        IndexType.CLUSTERING: 1,
        IndexType.GIN: 3,
        IndexType.GIST: 3,
        IndexType.SEARCH_INDEX: 3,
        IndexType.FULLTEXT: 2,
    }
    return priorities.get(index_type, 0)
