"""Core Converter class - Lark Interpreter subclass for CEL-to-SQL conversion."""

from __future__ import annotations

import re
from io import StringIO
from typing import Any

from lark import Token, Tree
from lark.visitors import Interpreter

from pycel2sql._constants import (
    DEFAULT_MAX_RECURSION_DEPTH,
    DEFAULT_MAX_SQL_OUTPUT_LENGTH,
    MAX_BYTE_ARRAY_LENGTH,
    MAX_COMPREHENSION_DEPTH,
)
from pycel2sql._errors import (
    ERR_MSG_SCHEMA_VALIDATION_FAILED,
    InvalidArgumentsError,
    InvalidByteArrayLengthError,
    InvalidDurationError,
    InvalidSchemaError,
    MaxComprehensionDepthExceededError,
    MaxDepthExceededError,
    MaxOutputLengthExceededError,
    UnsupportedExpressionError,
    UnsupportedOperationError,
)
from pycel2sql._operators import COMPARISON_OPERATORS, NULL_AWARE_OPS
from pycel2sql._utils import (
    escape_like_pattern,
    validate_field_name,
    validate_no_null_bytes,
)
from pycel2sql.dialect._base import Dialect
from pycel2sql.schema import Schema


def _strip_quotes(s: str) -> str:
    """Strip surrounding quotes from a CEL string literal token."""
    if s.startswith(('r"', "r'", 'R"', "R'")):
        s = s[1:]
    if s.startswith('"""') or s.startswith("'''"):
        return s[3:-3]
    if s.startswith('"') or s.startswith("'"):
        return s[1:-1]
    return s


def _is_string_token(token: Token) -> bool:
    return token.type in ("STRING_LIT", "MLSTRING_LIT")


def _is_int_token(token: Token) -> bool:
    return token.type == "INT_LIT"


def _is_uint_token(token: Token) -> bool:
    return token.type == "UINT_LIT"


def _is_float_token(token: Token) -> bool:
    return token.type == "FLOAT_LIT"


def _is_bool_token(token: Token) -> bool:
    return token.type == "BOOL_LIT"


def _is_null_token(token: Token) -> bool:
    return token.type == "NULL_LIT"


def _is_bytes_token(token: Token) -> bool:
    return token.type == "BYTES_LIT"


def _get_literal_token(tree: Tree) -> Token | None:
    """Extract the literal token from a deeply-nested expression tree.

    Walks through the precedence chain to find a literal at the bottom.
    """
    node: Tree | Token = tree
    while isinstance(node, Tree):
        if node.data == "literal":
            if node.children:
                tok = node.children[0]
                if isinstance(tok, Token):
                    return tok
            return None
        # Walk through single-child wrapper nodes
        if len(node.children) == 1:
            node = node.children[0]
        else:
            return None
    return None


def _tree_contains_string_literal(tree: Tree) -> bool:
    """Check if a tree contains any string literal at its leaves."""
    if isinstance(tree, Token):
        return _is_string_token(tree)
    if tree.data == "literal" and tree.children:
        tok = tree.children[0]
        return isinstance(tok, Token) and _is_string_token(tok)
    return any(
        _tree_contains_string_literal(child)
        for child in tree.children
        if isinstance(child, Tree)
    )


def _tree_is_list_literal(tree: Tree) -> bool:
    """Check if a tree is a list literal (for IN operator)."""
    node: Tree | Token = tree
    while isinstance(node, Tree):
        if node.data == "list_lit":
            return True
        if len(node.children) == 1:
            node = node.children[0]
        else:
            return False
    return False


def _unwrap_to_data(tree: Tree, target_data: str) -> Tree | None:
    """Unwrap single-child tree nodes to find a node with the given data."""
    node: Tree | Token = tree
    while isinstance(node, Tree):
        if node.data == target_data:
            return node
        if len(node.children) == 1:
            node = node.children[0]
        else:
            return None
    return None


class Converter(Interpreter):
    """Converts a CEL Lark parse tree into a SQL WHERE clause string."""

    def __init__(
        self,
        dialect: Dialect,
        schemas: dict[str, Schema] | None = None,
        max_depth: int = DEFAULT_MAX_RECURSION_DEPTH,
        max_output_length: int = DEFAULT_MAX_SQL_OUTPUT_LENGTH,
        parameterize: bool = False,
        validate_schema: bool = False,
    ) -> None:
        self._w = StringIO()
        self._dialect = dialect
        self._schemas = schemas or {}
        self._max_depth = max_depth
        self._max_output_length = max_output_length
        self._depth = 0
        self._comprehension_depth = 0
        self._parameterize = parameterize
        self._parameters: list[Any] = []
        self._param_count = 0
        self._comprehension_vars: set[str] = set()
        self._validate_schema = validate_schema
        if self._validate_schema and not self._schemas:
            raise InvalidSchemaError(
                ERR_MSG_SCHEMA_VALIDATION_FAILED,
                "validate_schema=True requires at least one schema to be provided",
            )

    @property
    def result(self) -> str:
        return self._w.getvalue()

    @property
    def parameters(self) -> list[Any]:
        return self._parameters

    def _check_limits(self) -> None:
        if self._depth > self._max_depth:
            raise MaxDepthExceededError(
                "maximum recursion depth exceeded",
                f"depth {self._depth} exceeds limit {self._max_depth}",
            )
        if self._w.tell() > self._max_output_length:
            raise MaxOutputLengthExceededError(
                "maximum SQL output length exceeded",
                f"output length exceeds limit {self._max_output_length}",
            )

    def _add_param(self, value: Any) -> int:
        """Add a parameter and return its 1-based index."""
        self._param_count += 1
        self._parameters.append(value)
        return self._param_count

    def _visit_child(self, tree: Tree) -> None:
        """Visit a child node, incrementing depth."""
        self._depth += 1
        try:
            self._check_limits()
            self.visit(tree)
        finally:
            self._depth -= 1

    # ---- Top-level entry ----

    def visit(self, tree: Tree) -> Any:
        """Override to handle Token children transparently."""
        if isinstance(tree, Token):
            # Bare tokens shouldn't appear at this level normally,
            # but handle gracefully
            self._w.write(str(tree))
            return None
        return super().visit(tree)

    # ---- expr: top-level, potentially ternary ----

    def expr(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 3:
            # Ternary: condition ? true_val : false_val
            self._w.write("CASE WHEN ")
            self._visit_child(children[0])
            self._w.write(" THEN ")
            self._visit_child(children[1])
            self._w.write(" ELSE ")
            self._visit_child(children[2])
            self._w.write(" END")
        elif len(children) == 1:
            self._visit_child(children[0])
        else:
            raise UnsupportedExpressionError(
                "unsupported expression structure",
                f"expr node has {len(children)} children",
            )

    # ---- Logical operators ----

    def conditionalor(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 2:
            self._visit_child(children[0])
            self._w.write(" OR ")
            self._visit_child(children[1])
        elif len(children) == 1:
            self._visit_child(children[0])
        else:
            raise UnsupportedExpressionError(
                "unsupported OR expression",
                f"conditionalor has {len(children)} children",
            )

    def conditionaland(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 2:
            self._visit_child(children[0])
            self._w.write(" AND ")
            self._visit_child(children[1])
        elif len(children) == 1:
            self._visit_child(children[0])
        else:
            raise UnsupportedExpressionError(
                "unsupported AND expression",
                f"conditionaland has {len(children)} children",
            )

    # ---- Comparison / relation ----

    def relation(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 1:
            self._visit_child(children[0])
            return

        if len(children) != 2:
            raise UnsupportedExpressionError(
                "unsupported relation expression",
                f"relation has {len(children)} children",
            )

        # children[0] is the operator prefix node (e.g. relation_eq)
        # children[1] is the right operand
        op_node = children[0]
        rhs = children[1]

        if not isinstance(op_node, Tree):
            raise UnsupportedExpressionError(
                "unsupported relation operator",
                f"expected Tree, got {type(op_node)}",
            )

        op_name = op_node.data
        lhs = op_node.children[0]  # left operand is inside the prefix node

        # Check for IN operator
        if op_name == "relation_in":
            self._visit_in(lhs, rhs)
            return

        # Check for NULL comparisons
        rhs_literal = _get_literal_token(rhs)
        lhs_literal = _get_literal_token(lhs)

        if op_name in NULL_AWARE_OPS:
            # NULL comparisons
            if rhs_literal and _is_null_token(rhs_literal):
                self._visit_child(lhs)
                self._w.write(" IS NULL" if op_name == "relation_eq" else " IS NOT NULL")
                return
            if lhs_literal and _is_null_token(lhs_literal):
                self._visit_child(rhs)
                self._w.write(" IS NULL" if op_name == "relation_eq" else " IS NOT NULL")
                return

            # BOOL comparisons
            if rhs_literal and _is_bool_token(rhs_literal):
                self._visit_child(lhs)
                bool_val = str(rhs_literal).lower() == "true"
                if op_name == "relation_eq":
                    self._w.write(" IS TRUE" if bool_val else " IS FALSE")
                else:
                    self._w.write(" IS NOT TRUE" if bool_val else " IS NOT FALSE")
                return
            if lhs_literal and _is_bool_token(lhs_literal):
                self._visit_child(rhs)
                bool_val = str(lhs_literal).lower() == "true"
                if op_name == "relation_eq":
                    self._w.write(" IS TRUE" if bool_val else " IS FALSE")
                else:
                    self._w.write(" IS NOT TRUE" if bool_val else " IS NOT FALSE")
                return

        sql_op = COMPARISON_OPERATORS.get(op_name)
        if sql_op is None:
            raise UnsupportedExpressionError(
                "unsupported comparison operator",
                f"unknown relation operator: {op_name}",
            )

        # Check if this is a JSON text extraction needing numeric cast
        # Only apply numeric cast when the other operand is numeric (not string)
        if op_name in ("relation_lt", "relation_le", "relation_gt", "relation_ge",
                        "relation_eq", "relation_ne"):
            rhs_is_numeric = self._is_numeric_literal(rhs)
            lhs_is_numeric = self._is_numeric_literal(lhs)
            if self._is_json_text_extraction(lhs) and rhs_is_numeric:
                self._dialect.write_cast_to_numeric(
                    self._w,
                    lambda: (self._w.write("("), self._visit_child(lhs), self._w.write(")")),
                )
                self._w.write(f" {sql_op} ")
                self._visit_child(rhs)
                return
            if self._is_json_text_extraction(rhs) and lhs_is_numeric:
                self._visit_child(lhs)
                self._w.write(f" {sql_op} ")
                self._dialect.write_cast_to_numeric(
                    self._w,
                    lambda: (self._w.write("("), self._visit_child(rhs), self._w.write(")")),
                )
                return

        self._visit_child(lhs)
        self._w.write(f" {sql_op} ")
        self._visit_child(rhs)

    # Operator prefix handlers - they just delegate to relation
    def relation_eq(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_ne(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_lt(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_le(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_gt(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_ge(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def relation_in(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    # ---- IN operator ----

    def _visit_in(self, lhs: Tree, rhs: Tree) -> None:
        """Handle the 'in' operator: x in [1,2,3] or x in arr."""
        if _tree_is_list_literal(rhs):
            # x in [1, 2, 3] -> x = ANY(ARRAY[1, 2, 3])
            self._dialect.write_array_membership(
                self._w,
                lambda: self._visit_child(lhs),
                lambda: self._visit_child(rhs),
            )
        else:
            # x in arr -> x = ANY(arr)
            self._dialect.write_array_membership(
                self._w,
                lambda: self._visit_child(lhs),
                lambda: self._visit_child(rhs),
            )

    # ---- Arithmetic ----

    def addition(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 1:
            self._visit_child(children[0])
            return

        if len(children) != 2:
            raise UnsupportedExpressionError(
                "unsupported addition expression",
                f"addition has {len(children)} children",
            )

        op_node = children[0]
        rhs = children[1]

        if not isinstance(op_node, Tree):
            raise UnsupportedExpressionError("unsupported addition operator")

        lhs = op_node.children[0]
        op_name = op_node.data

        if op_name == "addition_add":
            # Check for timestamp/duration arithmetic FIRST (before string concat,
            # since temporal functions like timestamp("...") contain string literals)
            if self._is_timestamp_or_duration_context(lhs, rhs):
                # Normalize order: timestamp/date first, duration/interval second
                if self._is_duration_expression(lhs) and not self._is_duration_expression(rhs):
                    self._dialect.write_timestamp_arithmetic(
                        self._w, "+",
                        lambda: self._visit_child(rhs),
                        lambda: self._visit_child(lhs),
                    )
                else:
                    self._dialect.write_timestamp_arithmetic(
                        self._w, "+",
                        lambda: self._visit_child(lhs),
                        lambda: self._visit_child(rhs),
                    )
                return
            # Check for string concatenation (only pure string contexts)
            if _tree_contains_string_literal(lhs) or _tree_contains_string_literal(rhs):
                self._dialect.write_string_concat(
                    self._w,
                    lambda: self._visit_child(lhs),
                    lambda: self._visit_child(rhs),
                )
                return
            # Check for list concatenation
            if _tree_is_list_literal(lhs) or _tree_is_list_literal(rhs):
                self._dialect.write_string_concat(
                    self._w,
                    lambda: self._visit_child(lhs),
                    lambda: self._visit_child(rhs),
                )
                return
            self._visit_child(lhs)
            self._w.write(" + ")
            self._visit_child(rhs)
        elif op_name == "addition_sub":
            # Check for timestamp arithmetic
            if self._is_timestamp_or_duration_context(lhs, rhs):
                self._dialect.write_timestamp_arithmetic(
                    self._w, "-",
                    lambda: self._visit_child(lhs),
                    lambda: self._visit_child(rhs),
                )
                return
            self._visit_child(lhs)
            self._w.write(" - ")
            self._visit_child(rhs)
        else:
            raise UnsupportedExpressionError(
                "unsupported addition operator",
                f"unknown addition operator: {op_name}",
            )

    def addition_add(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def addition_sub(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def multiplication(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 1:
            self._visit_child(children[0])
            return

        if len(children) != 2:
            raise UnsupportedExpressionError(
                "unsupported multiplication expression",
                f"multiplication has {len(children)} children",
            )

        op_node = children[0]
        rhs = children[1]

        if not isinstance(op_node, Tree):
            raise UnsupportedExpressionError("unsupported multiplication operator")

        lhs = op_node.children[0]
        op_name = op_node.data

        if op_name == "multiplication_mul":
            self._visit_child(lhs)
            self._w.write(" * ")
            self._visit_child(rhs)
        elif op_name == "multiplication_div":
            self._visit_child(lhs)
            self._w.write(" / ")
            self._visit_child(rhs)
        elif op_name == "multiplication_mod":
            self._w.write("MOD(")
            self._visit_child(lhs)
            self._w.write(", ")
            self._visit_child(rhs)
            self._w.write(")")
        else:
            raise UnsupportedExpressionError(
                "unsupported multiplication operator",
                f"unknown multiplication operator: {op_name}",
            )

    def multiplication_mul(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def multiplication_div(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    def multiplication_mod(self, tree: Tree) -> None:
        self._visit_child(tree.children[0])

    # ---- Unary ----

    def unary(self, tree: Tree) -> None:
        children = tree.children
        if len(children) == 1:
            self._visit_child(children[0])
            return

        if len(children) == 2:
            op_node = children[0]
            operand = children[1]
            if isinstance(op_node, Tree):
                if op_node.data == "unary_not":
                    self._w.write("NOT ")
                    self._visit_child(operand)
                    return
                elif op_node.data == "unary_neg":
                    self._w.write("-")
                    self._visit_child(operand)
                    return

        raise UnsupportedExpressionError(
            "unsupported unary expression",
            f"unary has {len(children)} children",
        )

    def unary_not(self, tree: Tree) -> None:
        pass  # Handled by unary()

    def unary_neg(self, tree: Tree) -> None:
        pass  # Handled by unary()

    # ---- Member access ----

    def member(self, tree: Tree) -> None:
        if len(tree.children) == 1:
            self._visit_child(tree.children[0])
        else:
            raise UnsupportedExpressionError(
                "unsupported member expression",
                f"member has {len(tree.children)} children",
            )

    def member_dot(self, tree: Tree) -> None:
        """Field access: a.b"""
        obj = tree.children[0]
        field_name = str(tree.children[1])

        # Check for JSON path
        table_name = self._get_root_ident(obj)

        # Schema validation (before JSON check and SQL writing)
        if table_name and not self._is_comprehension_var(table_name):
            first_field = self._get_first_field(obj, field_name)
            self._validate_field_in_schema(table_name, first_field)

        if table_name and self._is_field_json(table_name, self._get_first_field(obj, field_name)):
            self._build_json_path(tree)
            return

        self._visit_child(obj)
        self._w.write(".")
        validate_field_name(field_name)
        self._w.write(field_name)

    def member_dot_arg(self, tree: Tree) -> None:
        """Method call: a.method(args) or comprehension macro."""
        obj = tree.children[0]
        method_name = str(tree.children[1])
        args_node = tree.children[2] if len(tree.children) > 2 else None
        args = args_node.children if args_node is not None else []

        # Comprehension macros
        if method_name in ("all", "exists", "exists_one", "map", "filter"):
            self._visit_comprehension(obj, method_name, args)
            return

        # String methods
        if method_name == "contains":
            self._visit_contains(obj, args)
            return
        if method_name == "startsWith":
            self._visit_starts_with(obj, args)
            return
        if method_name == "endsWith":
            self._visit_ends_with(obj, args)
            return
        if method_name == "matches":
            self._visit_matches_method(obj, args)
            return
        if method_name == "size":
            self._visit_size_method(obj)
            return
        if method_name == "lowerAscii":
            self._w.write("LOWER(")
            self._visit_child(obj)
            self._w.write(")")
            return
        if method_name == "upperAscii":
            self._w.write("UPPER(")
            self._visit_child(obj)
            self._w.write(")")
            return
        if method_name == "trim":
            self._w.write("TRIM(")
            self._visit_child(obj)
            self._w.write(")")
            return
        if method_name == "charAt":
            self._visit_char_at(obj, args)
            return
        if method_name == "indexOf":
            self._visit_index_of(obj, args)
            return
        if method_name == "lastIndexOf":
            self._visit_last_index_of(obj, args)
            return
        if method_name == "substring":
            self._visit_substring(obj, args)
            return
        if method_name == "replace":
            self._visit_replace(obj, args)
            return
        if method_name == "reverse":
            self._w.write("REVERSE(")
            self._visit_child(obj)
            self._w.write(")")
            return
        if method_name == "split":
            self._visit_split(obj, args)
            return
        if method_name == "join":
            self._visit_join(obj, args)
            return
        if method_name == "format":
            self._visit_format(obj, args)
            return

        # Timestamp methods
        if method_name in (
            "getFullYear", "getMonth", "getDate", "getDayOfMonth",
            "getHours", "getMinutes", "getSeconds", "getMilliseconds",
            "getDayOfYear", "getDayOfWeek",
        ):
            self._visit_timestamp_extract(obj, method_name, args)
            return

        raise UnsupportedExpressionError(
            "unsupported method call",
            f"unknown method: {method_name}",
        )

    def member_index(self, tree: Tree) -> None:
        """Index access: a[0] or a["key"]."""
        obj = tree.children[0]
        index_expr = tree.children[1]

        # Check if this is a map key access (string index)
        index_literal = _get_literal_token(index_expr)
        if index_literal and _is_string_token(index_literal):
            raw_key = _strip_quotes(str(index_literal))
            validate_field_name(raw_key)
            self._visit_child(obj)
            self._w.write(f".{raw_key}")
            return

        # Array index access
        if index_literal and _is_int_token(index_literal):
            idx = int(str(index_literal))
            if idx < 0:
                raise InvalidArgumentsError(
                    "negative array index not supported",
                    f"array index {idx} is negative",
                )
            if idx > 2**31:
                raise InvalidArgumentsError(
                    "array index overflow",
                    f"array index {idx} is too large",
                )
            self._dialect.write_list_index_const(
                self._w,
                lambda: self._visit_child(obj),
                idx,
            )
            return

        # Dynamic index
        self._dialect.write_list_index(
            self._w,
            lambda: self._visit_child(obj),
            lambda: self._visit_child(index_expr),
        )

    def member_object(self, tree: Tree) -> None:
        raise UnsupportedExpressionError("object construction not supported in SQL conversion")

    # ---- Primary expressions ----

    def primary(self, tree: Tree) -> None:
        if len(tree.children) == 1:
            self._visit_child(tree.children[0])
        else:
            raise UnsupportedExpressionError(
                "unsupported primary expression",
                f"primary has {len(tree.children)} children",
            )

    def ident(self, tree: Tree) -> None:
        """Bare identifier."""
        name = str(tree.children[0])
        # Don't validate comprehension iteration variables
        if not self._is_comprehension_var(name):
            validate_field_name(name)
        self._w.write(name)

    def ident_arg(self, tree: Tree) -> None:
        """Function call: func(args)."""
        func_name = str(tree.children[0])
        args_node = tree.children[1] if len(tree.children) > 1 else None
        args = args_node.children if args_node is not None else []

        # has() function
        if func_name == "has":
            self._visit_has(args)
            return

        # size() function
        if func_name == "size":
            if len(args) == 1:
                self._visit_size_func(args[0])
            return

        # matches() function-style
        if func_name == "matches":
            if len(args) == 2:
                self._visit_matches_func(args[0], args[1])
                return

        # Type cast functions
        if func_name in ("bool", "bytes", "double", "int", "uint", "string"):
            self._visit_type_cast(func_name, args)
            return

        # timestamp() function
        if func_name == "timestamp":
            self._visit_timestamp_func(args)
            return

        # duration() function
        if func_name == "duration":
            self._visit_duration_func(args)
            return

        # interval() function
        if func_name == "interval":
            self._visit_interval_func(args)
            return

        # date/time/datetime functions
        if func_name in ("date", "time", "datetime"):
            self._visit_datetime_constructor(func_name, args)
            return

        # current_date, current_datetime
        if func_name in ("current_date", "current_datetime"):
            self._visit_current_datetime(func_name, args)
            return

        # Generic uppercase function
        self._w.write(func_name.upper())
        self._w.write("(")
        for i, arg in enumerate(args):
            if i > 0:
                self._w.write(", ")
            self._visit_child(arg)
        self._w.write(")")

    def dot_ident_arg(self, tree: Tree) -> None:
        func_name = str(tree.children[0])
        self._w.write(f".{func_name}(")
        if len(tree.children) > 1:
            args_node = tree.children[1]
            for i, arg in enumerate(args_node.children):
                if i > 0:
                    self._w.write(", ")
                self._visit_child(arg)
        self._w.write(")")

    def dot_ident(self, tree: Tree) -> None:
        name = str(tree.children[0])
        self._w.write(f".{name}")

    def paren_expr(self, tree: Tree) -> None:
        """Parenthesized expression."""
        self._w.write("(")
        self._visit_child(tree.children[0])
        self._w.write(")")

    # ---- Literals ----

    def literal(self, tree: Tree) -> None:
        token = tree.children[0]
        if not isinstance(token, Token):
            raise UnsupportedExpressionError("unexpected literal structure")

        if _is_null_token(token):
            self._w.write("NULL")
        elif _is_bool_token(token):
            self._w.write("TRUE" if str(token).lower() == "true" else "FALSE")
        elif _is_int_token(token):
            val = int(str(token), 0)
            if self._parameterize:
                idx = self._add_param(val)
                self._dialect.write_param_placeholder(self._w, idx)
            else:
                self._w.write(str(val))
        elif _is_uint_token(token):
            raw = str(token).rstrip("uU")
            val = int(raw, 0)
            if self._parameterize:
                idx = self._add_param(val)
                self._dialect.write_param_placeholder(self._w, idx)
            else:
                self._w.write(str(val))
        elif _is_float_token(token):
            val = float(str(token))
            if self._parameterize:
                idx = self._add_param(val)
                self._dialect.write_param_placeholder(self._w, idx)
            else:
                self._w.write(str(token))
        elif _is_string_token(token):
            raw = _strip_quotes(str(token))
            # Process escape sequences for non-raw strings
            if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
                raw = self._process_escapes(raw)
            validate_no_null_bytes(raw)
            if self._parameterize:
                idx = self._add_param(raw)
                self._dialect.write_param_placeholder(self._w, idx)
            else:
                self._dialect.write_string_literal(self._w, raw)
        elif _is_bytes_token(token):
            raw_str = str(token)
            # Strip b/B prefix and quotes
            inner = raw_str[1:]
            if inner.startswith('"""') or inner.startswith("'''"):
                inner = inner[3:-3]
            elif inner.startswith('"') or inner.startswith("'"):
                inner = inner[1:-1]
            byte_val = inner.encode("utf-8")
            if not self._parameterize and len(byte_val) > MAX_BYTE_ARRAY_LENGTH:
                raise InvalidByteArrayLengthError(
                    "byte array too long",
                    f"byte array length {len(byte_val)} exceeds limit {MAX_BYTE_ARRAY_LENGTH}",
                )
            if self._parameterize:
                idx = self._add_param(byte_val)
                self._dialect.write_param_placeholder(self._w, idx)
            else:
                self._dialect.write_bytes_literal(self._w, byte_val)
        else:
            raise UnsupportedExpressionError(
                "unsupported literal type",
                f"unknown token type: {token.type}",
            )

    def list_lit(self, tree: Tree) -> None:
        """List literal: [1, 2, 3] -> ARRAY[1, 2, 3]."""
        self._dialect.write_array_literal_open(self._w)
        if tree.children:
            exprlist = tree.children[0]
            if isinstance(exprlist, Tree) and exprlist.data == "exprlist":
                for i, child in enumerate(exprlist.children):
                    if i > 0:
                        self._w.write(", ")
                    self._visit_child(child)
        self._dialect.write_array_literal_close(self._w)

    def map_lit(self, tree: Tree) -> None:
        """Map literal: {"k": v} -> ROW(v) with .k access."""
        self._dialect.write_struct_open(self._w)
        if tree.children:
            mapinits = tree.children[0]
            if isinstance(mapinits, Tree) and mapinits.data == "mapinits":
                # mapinits children alternate: key, value, key, value...
                children = mapinits.children
                first = True
                for i in range(0, len(children), 2):
                    if not first:
                        self._w.write(", ")
                    first = False
                    # Only write the value, key is used for field access
                    self._visit_child(children[i + 1])
        self._dialect.write_struct_close(self._w)

    def exprlist(self, tree: Tree) -> None:
        for i, child in enumerate(tree.children):
            if i > 0:
                self._w.write(", ")
            self._visit_child(child)

    def mapinits(self, tree: Tree) -> None:
        children = tree.children
        first = True
        for i in range(0, len(children), 2):
            if not first:
                self._w.write(", ")
            first = False
            self._visit_child(children[i + 1])

    def fieldinits(self, tree: Tree) -> None:
        children = tree.children
        first = True
        for i in range(0, len(children), 2):
            if not first:
                self._w.write(", ")
            first = False
            self._visit_child(children[i + 1])

    # ---- String functions ----

    def _visit_contains(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("contains() requires exactly 1 argument")
        needle_token = _get_literal_token(args[0])
        if needle_token and _is_string_token(needle_token):
            raw = _strip_quotes(str(needle_token))
            if not str(needle_token).startswith(("r'", 'r"', "R'", 'R"')):
                raw = self._process_escapes(raw)
            validate_no_null_bytes(raw, "string literals")
        if self._parameterize:
            self._dialect.write_contains(
                self._w,
                lambda: self._visit_child(obj),
                lambda: self._visit_child(args[0]),
            )
        else:
            self._dialect.write_contains(
                self._w,
                lambda: self._visit_child(obj),
                lambda: self._visit_child(args[0]),
            )

    def _visit_starts_with(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("startsWith() requires exactly 1 argument")
        token = _get_literal_token(args[0])
        if not token or not _is_string_token(token):
            raise InvalidArgumentsError("startsWith() requires a string literal argument")
        raw = _strip_quotes(str(token))
        if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
            raw = self._process_escapes(raw)
        validate_no_null_bytes(raw, "LIKE patterns")
        escaped = escape_like_pattern(raw)
        self._visit_child(obj)
        self._w.write(f" LIKE '{escaped}%'")
        self._dialect.write_like_escape(self._w)

    def _visit_ends_with(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("endsWith() requires exactly 1 argument")
        token = _get_literal_token(args[0])
        if not token or not _is_string_token(token):
            raise InvalidArgumentsError("endsWith() requires a string literal argument")
        raw = _strip_quotes(str(token))
        if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
            raw = self._process_escapes(raw)
        validate_no_null_bytes(raw, "LIKE patterns")
        escaped = escape_like_pattern(raw)
        self._visit_child(obj)
        self._w.write(f" LIKE '%{escaped}'")
        self._dialect.write_like_escape(self._w)

    def _visit_matches_method(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("matches() requires exactly 1 argument")
        token = _get_literal_token(args[0])
        if not token or not _is_string_token(token):
            raise InvalidArgumentsError("matches() requires a string literal argument")
        raw = _strip_quotes(str(token))
        if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
            raw = self._process_escapes(raw)
        converted_pattern, case_insensitive = self._dialect.convert_regex(raw)
        self._dialect.write_regex_match(
            self._w,
            lambda: self._visit_child(obj),
            converted_pattern,
            case_insensitive,
        )

    def _visit_matches_func(self, target: Tree, pattern_expr: Tree) -> None:
        token = _get_literal_token(pattern_expr)
        if not token or not _is_string_token(token):
            raise InvalidArgumentsError("matches() requires a string literal pattern")
        raw = _strip_quotes(str(token))
        if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
            raw = self._process_escapes(raw)
        converted_pattern, case_insensitive = self._dialect.convert_regex(raw)
        self._dialect.write_regex_match(
            self._w,
            lambda: self._visit_child(target),
            converted_pattern,
            case_insensitive,
        )

    def _visit_size_method(self, obj: Tree) -> None:
        """size() as a method call on an object."""
        if self._is_array_expression(obj):
            self._dialect.write_array_length(
                self._w, 1, lambda: self._visit_child(obj)
            )
            return
        self._w.write("LENGTH(")
        self._visit_child(obj)
        self._w.write(")")

    def _visit_size_func(self, arg: Tree) -> None:
        """size(x) function call."""
        if self._is_array_expression(arg):
            self._dialect.write_array_length(
                self._w, 1, lambda: self._visit_child(arg)
            )
            return
        self._w.write("LENGTH(")
        self._visit_child(arg)
        self._w.write(")")

    def _visit_char_at(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("charAt() requires exactly 1 argument")
        idx_literal = _get_literal_token(args[0])
        self._w.write("SUBSTRING(")
        self._visit_child(obj)
        self._w.write(", ")
        if idx_literal and _is_int_token(idx_literal):
            idx = int(str(idx_literal))
            self._w.write(str(idx + 1))
        else:
            self._visit_child(args[0])
            self._w.write(" + 1")
        self._w.write(", 1)")

    def _visit_index_of(self, obj: Tree, args: list) -> None:
        if len(args) < 1 or len(args) > 2:
            raise InvalidArgumentsError("indexOf() requires 1 or 2 arguments")

        if len(args) == 1:
            # indexOf(needle) -> CASE WHEN POSITION(needle IN str) > 0 THEN POSITION(needle IN str) - 1 ELSE -1 END
            self._w.write("CASE WHEN POSITION(")
            self._visit_child(args[0])
            self._w.write(" IN ")
            self._visit_child(obj)
            self._w.write(") > 0 THEN POSITION(")
            self._visit_child(args[0])
            self._w.write(" IN ")
            self._visit_child(obj)
            self._w.write(") - 1 ELSE -1 END")
        else:
            # indexOf(needle, offset) -> complex POSITION with SUBSTRING
            offset_literal = _get_literal_token(args[1])
            if offset_literal and _is_int_token(offset_literal):
                offset = int(str(offset_literal))
                self._w.write("CASE WHEN POSITION(")
                self._visit_child(args[0])
                self._w.write(" IN SUBSTRING(")
                self._visit_child(obj)
                self._w.write(f", {offset + 1}))")
                self._w.write(" > 0 THEN POSITION(")
                self._visit_child(args[0])
                self._w.write(" IN SUBSTRING(")
                self._visit_child(obj)
                self._w.write(f", {offset + 1}))")
                self._w.write(f" + {offset} - 1 ELSE -1 END")
            else:
                self._w.write("CASE WHEN POSITION(")
                self._visit_child(args[0])
                self._w.write(" IN SUBSTRING(")
                self._visit_child(obj)
                self._w.write(", ")
                self._visit_child(args[1])
                self._w.write(" + 1))")
                self._w.write(" > 0 THEN POSITION(")
                self._visit_child(args[0])
                self._w.write(" IN SUBSTRING(")
                self._visit_child(obj)
                self._w.write(", ")
                self._visit_child(args[1])
                self._w.write(" + 1))")
                self._w.write(" + ")
                self._visit_child(args[1])
                self._w.write(" - 1 ELSE -1 END")

    def _visit_last_index_of(self, obj: Tree, args: list) -> None:
        if len(args) < 1:
            raise InvalidArgumentsError("lastIndexOf() requires at least 1 argument")
        # lastIndexOf(needle) using REVERSE
        self._w.write("CASE WHEN POSITION(REVERSE(")
        self._visit_child(args[0])
        self._w.write(") IN REVERSE(")
        self._visit_child(obj)
        self._w.write(")) > 0 THEN LENGTH(")
        self._visit_child(obj)
        self._w.write(") - POSITION(REVERSE(")
        self._visit_child(args[0])
        self._w.write(") IN REVERSE(")
        self._visit_child(obj)
        self._w.write(")) - LENGTH(")
        self._visit_child(args[0])
        self._w.write(") + 1 ELSE -1 END")

    def _visit_substring(self, obj: Tree, args: list) -> None:
        if len(args) < 1 or len(args) > 2:
            raise InvalidArgumentsError("substring() requires 1 or 2 arguments")

        self._w.write("SUBSTRING(")
        self._visit_child(obj)
        self._w.write(", ")

        start_literal = _get_literal_token(args[0])

        if len(args) == 1:
            # substring(start) - from start to end
            if start_literal and _is_int_token(start_literal):
                start = int(str(start_literal))
                self._w.write(str(start + 1))
            else:
                self._visit_child(args[0])
                self._w.write(" + 1")
            self._w.write(")")
        else:
            # substring(start, end) - from start to end
            end_literal = _get_literal_token(args[1])
            if start_literal and _is_int_token(start_literal):
                start = int(str(start_literal))
                self._w.write(str(start + 1))
                self._w.write(", ")
                if end_literal and _is_int_token(end_literal):
                    end = int(str(end_literal))
                    self._w.write(str(end - start))
                else:
                    self._visit_child(args[1])
                    self._w.write(f" - ({start})")
            else:
                self._visit_child(args[0])
                self._w.write(" + 1, ")
                self._visit_child(args[1])
                self._w.write(" - (")
                self._visit_child(args[0])
                self._w.write(")")
            self._w.write(")")

    def _visit_replace(self, obj: Tree, args: list) -> None:
        if len(args) < 2 or len(args) > 3:
            raise InvalidArgumentsError("replace() requires 2 or 3 arguments")

        if len(args) == 3:
            # Check limit
            limit_token = _get_literal_token(args[2])
            if limit_token and _is_int_token(limit_token):
                limit = int(str(limit_token))
                if limit != -1:
                    raise UnsupportedOperationError(
                        "replace() with limit != -1 is not supported",
                        f"replace() limit={limit} not supported in SQL",
                    )

        self._w.write("REPLACE(")
        self._visit_child(obj)
        self._w.write(", ")
        self._visit_child(args[0])
        self._w.write(", ")
        self._visit_child(args[1])
        self._w.write(")")

    def _visit_split(self, obj: Tree, args: list) -> None:
        if len(args) < 1 or len(args) > 2:
            raise InvalidArgumentsError("split() requires 1 or 2 arguments")

        if len(args) == 1:
            # split(delimiter)
            self._dialect.write_split(
                self._w,
                lambda: self._visit_child(obj),
                lambda: self._visit_child(args[0]),
            )
            return

        # split(delimiter, limit)
        limit_token = _get_literal_token(args[1])
        if limit_token and _is_int_token(limit_token):
            limit = int(str(limit_token))
            if limit == -1:
                # Unlimited split
                self._dialect.write_split(
                    self._w,
                    lambda: self._visit_child(obj),
                    lambda: self._visit_child(args[0]),
                )
            elif limit == 0:
                # Empty array
                self._dialect.write_empty_typed_array(self._w, "text")
            elif limit == 1:
                # No split - return single-element array
                self._dialect.write_array_literal_open(self._w)
                self._visit_child(obj)
                self._dialect.write_array_literal_close(self._w)
            elif limit < -1:
                raise UnsupportedOperationError(
                    "split() with negative limit other than -1 is not supported",
                    f"split() limit={limit} not supported",
                )
            else:
                self._dialect.write_split_with_limit(
                    self._w,
                    lambda: self._visit_child(obj),
                    lambda: self._visit_child(args[0]),
                    limit,
                )
        else:
            raise InvalidArgumentsError("split() limit must be an integer literal")

    def _visit_join(self, obj: Tree, args: list) -> None:
        if len(args) > 1:
            raise InvalidArgumentsError("join() requires 0 or 1 arguments")
        if len(args) == 0:
            self._dialect.write_join(
                self._w,
                lambda: self._visit_child(obj),
                lambda: self._dialect.write_string_literal(self._w, ""),
            )
        else:
            self._dialect.write_join(
                self._w,
                lambda: self._visit_child(obj),
                lambda: self._visit_child(args[0]),
            )

    def _visit_format(self, obj: Tree, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("format() requires exactly 1 argument (the arg list)")

        # Get the format string
        fmt_token = _get_literal_token(obj)
        if not fmt_token or not _is_string_token(fmt_token):
            raise InvalidArgumentsError("format() requires a string literal format")

        raw_fmt = _strip_quotes(str(fmt_token))
        if not str(fmt_token).startswith(("r'", 'r"', "R'", 'R"')):
            raw_fmt = self._process_escapes(raw_fmt)

        # Supported format specifiers that map to SQL %s
        _SUPPORTED_SPECIFIERS = {"s", "d", "f", "o", "e", "E", "g", "G"}

        # Check for unsupported format specifiers
        for m in re.finditer(r"%([a-zA-Z])", raw_fmt):
            spec = m.group(1)
            if spec not in _SUPPORTED_SPECIFIERS:
                raise UnsupportedOperationError(
                    f"unsupported format specifier %{spec}",
                    f"format specifier %{spec} cannot be converted to SQL",
                )

        # Convert %d, %f etc. to %s for SQL FORMAT()
        sql_fmt = re.sub(r"%([dfoFeEgG])", "%s", raw_fmt)

        # Get the argument list
        arg_list = args[0]
        list_node = _unwrap_to_data(arg_list, "list_lit")

        self._w.write("FORMAT(")
        self._dialect.write_string_literal(self._w, sql_fmt)

        if list_node is not None and list_node.children:
            exprlist = list_node.children[0]
            if isinstance(exprlist, Tree) and exprlist.data == "exprlist":
                for child in exprlist.children:
                    self._w.write(", ")
                    self._visit_child(child)
        self._w.write(")")

    # ---- has() function ----

    def _visit_has(self, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("has() requires exactly 1 argument")
        arg = args[0]
        # has(a.b) -> a.b IS NOT NULL (or JSON key existence check)
        member_dot = _unwrap_to_data(arg, "member_dot")
        if member_dot:
            table_name = self._get_root_ident(member_dot.children[0])
            field_name = str(member_dot.children[1])
            # Check if the parent object (not the field itself) is a JSON field
            # e.g., has(usr.metadata.key) -> usr.metadata ? 'key'
            # but has(usr.metadata) -> usr.metadata IS NOT NULL
            parent_dot = _unwrap_to_data(member_dot.children[0], "member_dot")
            if parent_dot:
                parent_table = self._get_root_ident(parent_dot.children[0])
                parent_field = str(parent_dot.children[1])
                if parent_table and self._is_field_json(parent_table, parent_field):
                    self._dialect.write_json_existence(
                        self._w,
                        self._is_field_jsonb(parent_table, parent_field),
                        field_name,
                        lambda: self._visit_child(member_dot.children[0]),
                    )
                    return
            if table_name and self._is_nested_json_field(member_dot):
                # Nested JSON field existence
                self._build_json_path(member_dot)
                self._w.write(" IS NOT NULL")
                return
        self._visit_child(arg)
        self._w.write(" IS NOT NULL")

    # ---- Type casting ----

    def _visit_type_cast(self, type_name: str, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError(f"{type_name}() requires exactly 1 argument")

        # Special case: int(timestamp) -> EXTRACT(EPOCH FROM ts)::bigint
        if type_name == "int":
            root = self._get_root_ident(args[0])
            if root and self._is_timestamp_field(root, args[0]):
                self._dialect.write_epoch_extract(
                    self._w, lambda: self._visit_child(args[0])
                )
                return

        self._w.write("CAST(")
        self._visit_child(args[0])
        self._w.write(" AS ")
        self._dialect.write_type_name(self._w, type_name)
        self._w.write(")")

    # ---- Timestamp functions ----

    def _visit_timestamp_func(self, args: list) -> None:
        if len(args) == 1:
            # timestamp("2021-01-01T00:00:00Z") -> CAST('...' AS TIMESTAMP WITH TIME ZONE)
            self._dialect.write_timestamp_cast(
                self._w, lambda: self._visit_child(args[0])
            )
        elif len(args) == 2:
            # timestamp(datetime_expr, timezone) -> expr AT TIME ZONE tz
            self._visit_child(args[0])
            self._w.write(" AT TIME ZONE ")
            self._visit_child(args[1])
        else:
            raise InvalidArgumentsError("timestamp() requires 1 or 2 arguments")

    def _visit_duration_func(self, args: list) -> None:
        if len(args) != 1:
            raise InvalidArgumentsError("duration() requires exactly 1 argument")
        token = _get_literal_token(args[0])
        if not token or not _is_string_token(token):
            raise InvalidArgumentsError("duration() requires a string literal argument")
        raw = _strip_quotes(str(token))
        if not str(token).startswith(("r'", 'r"', "R'", 'R"')):
            raw = self._process_escapes(raw)
        value, unit = self._parse_duration(raw)
        self._dialect.write_duration(self._w, value, unit)

    def _visit_interval_func(self, args: list) -> None:
        if len(args) != 2:
            raise InvalidArgumentsError("interval() requires exactly 2 arguments")
        # interval(value, UNIT) - UNIT is an identifier
        unit_token = _get_literal_token(args[1])
        if unit_token:
            unit = str(unit_token)
        else:
            # Try to get the identifier directly
            ident_node = _unwrap_to_data(args[1], "ident")
            if ident_node:
                unit = str(ident_node.children[0])
            else:
                raise InvalidArgumentsError("interval() requires a unit identifier")
        self._dialect.write_interval(
            self._w,
            lambda: self._visit_child(args[0]),
            unit,
        )

    def _visit_datetime_constructor(self, func_name: str, args: list) -> None:
        """Handle date(), time(), datetime() constructors."""
        self._w.write(func_name.upper())
        self._w.write("(")
        for i, arg in enumerate(args):
            if i > 0:
                self._w.write(", ")
            self._visit_child(arg)
        self._w.write(")")

    def _visit_current_datetime(self, func_name: str, args: list) -> None:
        self._w.write(func_name.upper())
        self._w.write("(")
        for i, arg in enumerate(args):
            if i > 0:
                self._w.write(", ")
            self._visit_child(arg)
        self._w.write(")")

    def _visit_timestamp_extract(self, obj: Tree, method_name: str, args: list) -> None:
        """Handle timestamp extraction methods: getFullYear(), getMonth(), etc."""
        part_map = {
            "getFullYear": "YEAR",
            "getMonth": "MONTH",
            "getDate": "DAY",
            "getDayOfMonth": "DAY",
            "getHours": "HOUR",
            "getMinutes": "MINUTE",
            "getSeconds": "SECOND",
            "getMilliseconds": "MILLISECONDS",
            "getDayOfYear": "DOY",
            "getDayOfWeek": "DOW",
        }
        part = part_map.get(method_name)
        if not part:
            raise UnsupportedExpressionError(f"unsupported timestamp method: {method_name}")

        # Check for timezone argument
        write_tz = None
        if args:
            def write_tz():
                return self._visit_child(args[0])

        self._dialect.write_extract(
            self._w, part, lambda: self._visit_child(obj), write_tz
        )

        # Post-extraction adjustments for 0-indexed values
        if method_name == "getMonth":
            self._w.write(" - 1")
        elif method_name == "getDayOfMonth":
            self._w.write(" - 1")
        elif method_name == "getDayOfYear":
            self._w.write(" - 1")

    def _parse_duration(self, duration_str: str) -> tuple[int, str]:
        """Parse a Go-style duration string into (value, SQL_UNIT)."""
        total_ns = 0
        remaining = duration_str

        patterns = [
            (r"(\d+)h", 3_600_000_000_000),
            (r"(\d+)m(?!s)", 60_000_000_000),
            (r"(\d+)s", 1_000_000_000),
            (r"(\d+)ms", 1_000_000),
            (r"(\d+)us|(\d+)µs", 1_000),
            (r"(\d+)ns", 1),
        ]

        for pattern, ns_per_unit in patterns:
            m = re.search(pattern, remaining)
            if m:
                val = int(m.group(1) if m.group(1) else m.group(2))
                total_ns += val * ns_per_unit

        if total_ns == 0:
            raise InvalidDurationError(
                "invalid duration value",
                f"cannot parse duration: {duration_str}",
            )

        # Find the best unit
        if total_ns % 3_600_000_000_000 == 0:
            return total_ns // 3_600_000_000_000, "HOUR"
        if total_ns % 60_000_000_000 == 0:
            return total_ns // 60_000_000_000, "MINUTE"
        if total_ns % 1_000_000_000 == 0:
            return total_ns // 1_000_000_000, "SECOND"
        if total_ns % 1_000_000 == 0:
            return total_ns // 1_000_000, "MILLISECOND"
        if total_ns % 1_000 == 0:
            return total_ns // 1_000, "MICROSECOND"
        return total_ns, "NANOSECOND"

    # ---- Comprehensions ----

    def _visit_comprehension(self, source: Tree, macro_name: str, args: list) -> None:
        """Handle comprehension macros: all, exists, exists_one, map, filter."""
        if self._comprehension_depth >= MAX_COMPREHENSION_DEPTH:
            raise MaxComprehensionDepthExceededError(
                "comprehension nesting depth exceeded",
                f"depth {self._comprehension_depth} exceeds limit {MAX_COMPREHENSION_DEPTH}",
            )

        self._comprehension_depth += 1
        try:
            if macro_name == "all":
                self._visit_comp_all(source, args)
            elif macro_name == "exists":
                self._visit_comp_exists(source, args)
            elif macro_name == "exists_one":
                self._visit_comp_exists_one(source, args)
            elif macro_name == "map":
                if len(args) == 3:
                    self._visit_comp_map_filter(source, args)
                else:
                    self._visit_comp_map(source, args)
            elif macro_name == "filter":
                self._visit_comp_filter(source, args)
            else:
                raise UnsupportedExpressionError(f"unsupported comprehension: {macro_name}")
        finally:
            self._comprehension_depth -= 1

    def _write_unnest_source(self, source: Tree, iter_var: str) -> None:
        """Write the UNNEST(source) AS var clause."""
        self._dialect.write_unnest(self._w, lambda: self._visit_child(source))
        self._w.write(f" AS {iter_var}")

    def _visit_comp_all(self, source: Tree, args: list) -> None:
        """all(x, pred) -> NOT EXISTS (SELECT 1 FROM UNNEST(src) AS x WHERE NOT (pred))"""
        if len(args) != 2:
            raise InvalidArgumentsError("all() requires exactly 2 arguments")
        iter_var = self._get_ident_name(args[0])
        pred = args[1]
        self._comprehension_vars.add(iter_var)
        try:
            self._w.write("NOT EXISTS (SELECT 1 FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(" WHERE NOT (")
            self._visit_child(pred)
            self._w.write("))")
        finally:
            self._comprehension_vars.discard(iter_var)

    def _visit_comp_exists(self, source: Tree, args: list) -> None:
        """exists(x, pred) -> EXISTS (SELECT 1 FROM UNNEST(src) AS x WHERE pred)"""
        if len(args) != 2:
            raise InvalidArgumentsError("exists() requires exactly 2 arguments")
        iter_var = self._get_ident_name(args[0])
        pred = args[1]
        self._comprehension_vars.add(iter_var)
        try:
            self._w.write("EXISTS (SELECT 1 FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(" WHERE ")
            self._visit_child(pred)
            self._w.write(")")
        finally:
            self._comprehension_vars.discard(iter_var)

    def _visit_comp_exists_one(self, source: Tree, args: list) -> None:
        """exists_one(x, pred) -> (SELECT COUNT(*) FROM UNNEST(src) AS x WHERE pred) = 1"""
        if len(args) != 2:
            raise InvalidArgumentsError("exists_one() requires exactly 2 arguments")
        iter_var = self._get_ident_name(args[0])
        pred = args[1]
        self._comprehension_vars.add(iter_var)
        try:
            self._w.write("(SELECT COUNT(*) FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(" WHERE ")
            self._visit_child(pred)
            self._w.write(") = 1")
        finally:
            self._comprehension_vars.discard(iter_var)

    def _visit_comp_map(self, source: Tree, args: list) -> None:
        """map(x, transform) -> ARRAY(SELECT transform FROM UNNEST(src) AS x)"""
        if len(args) != 2:
            raise InvalidArgumentsError("map() requires exactly 2 arguments")
        iter_var = self._get_ident_name(args[0])
        transform = args[1]
        self._comprehension_vars.add(iter_var)
        try:
            self._dialect.write_array_subquery_open(self._w)
            self._visit_child(transform)
            self._dialect.write_array_subquery_expr_close(self._w)
            self._w.write(" FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(")")
        finally:
            self._comprehension_vars.discard(iter_var)

    def _visit_comp_map_filter(self, source: Tree, args: list) -> None:
        """map(x, filter, transform) -> ARRAY(SELECT transform FROM UNNEST(src) AS x WHERE filter)"""
        iter_var = self._get_ident_name(args[0])
        filter_pred = args[1]
        transform = args[2]
        self._comprehension_vars.add(iter_var)
        try:
            self._dialect.write_array_subquery_open(self._w)
            self._visit_child(transform)
            self._dialect.write_array_subquery_expr_close(self._w)
            self._w.write(" FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(" WHERE ")
            self._visit_child(filter_pred)
            self._w.write(")")
        finally:
            self._comprehension_vars.discard(iter_var)

    def _visit_comp_filter(self, source: Tree, args: list) -> None:
        """filter(x, pred) -> ARRAY(SELECT x FROM UNNEST(src) AS x WHERE pred)"""
        if len(args) != 2:
            raise InvalidArgumentsError("filter() requires exactly 2 arguments")
        iter_var = self._get_ident_name(args[0])
        pred = args[1]
        self._comprehension_vars.add(iter_var)
        try:
            self._dialect.write_array_subquery_open(self._w)
            self._w.write(iter_var)
            self._dialect.write_array_subquery_expr_close(self._w)
            self._w.write(" FROM ")
            self._write_unnest_source(source, iter_var)
            self._w.write(" WHERE ")
            self._visit_child(pred)
            self._w.write(")")
        finally:
            self._comprehension_vars.discard(iter_var)

    # ---- JSON support ----

    def _is_field_json(self, table_name: str, field_name: str) -> bool:
        schema = self._schemas.get(table_name)
        if not schema:
            return False
        field = schema.find_field(field_name)
        return field is not None and (field.is_json or field.is_jsonb)

    def _is_field_jsonb(self, table_name: str, field_name: str) -> bool:
        schema = self._schemas.get(table_name)
        if not schema:
            return False
        field = schema.find_field(field_name)
        return field is not None and field.is_jsonb

    def _is_nested_json_field(self, tree: Tree) -> bool:
        """Check if a member_dot chain involves a JSON field."""
        if tree.data != "member_dot":
            return False
        obj = tree.children[0]
        table_name = self._get_root_ident(obj)
        if not table_name:
            return False
        first_field = self._get_first_field(obj, str(tree.children[1]))
        return self._is_field_json(table_name, first_field)

    def _build_json_path(self, tree: Tree) -> None:
        """Build a JSON path expression from a member_dot chain."""
        # Collect the chain: table.json_field.path1.path2...
        parts: list[str] = []
        node = tree
        while isinstance(node, Tree) and node.data == "member_dot":
            parts.append(str(node.children[1]))
            node = node.children[0]
            # Unwrap single-child wrappers
            while isinstance(node, Tree) and node.data in ("member", "primary") and len(node.children) == 1:
                node = node.children[0]

        parts.reverse()

        # node should now be the root ident
        # First part after root is the JSON column, rest are path segments
        if len(parts) < 2:
            # Simple field access
            self._visit_child(node)
            self._w.write(f".{parts[0]}")
            return

        root_node = node
        json_col = parts[0]

        # Callback that writes "root.json_column"
        def write_base() -> None:
            self._visit_child(root_node)
            self._w.write(f".{json_col}")

        # Chain through path segments with real callbacks
        current_base = write_base
        for i, part in enumerate(parts[1:]):
            is_final = i == len(parts) - 2

            if is_final:
                self._dialect.write_json_field_access(
                    self._w, current_base, part, True,
                )
            else:
                # Build intermediate callback for nested access
                def make_base(pb: Any = current_base, cp: str = part) -> Any:
                    def intermediate() -> None:
                        self._dialect.write_json_field_access(
                            self._w, pb, cp, False,
                        )
                    return intermediate
                current_base = make_base()

    def _is_json_text_extraction(self, tree: Tree) -> bool:
        """Check if a tree represents a JSON text extraction (->>)."""
        node = tree
        while isinstance(node, Tree) and node.data in (
            "expr", "conditionalor", "conditionaland", "relation",
            "addition", "multiplication", "unary", "member", "primary",
        ) and len(node.children) == 1:
            node = node.children[0]
        if not isinstance(node, Tree) or node.data != "member_dot":
            return False
        return self._is_nested_json_field(node)

    # ---- Schema helpers ----

    def _validate_field_in_schema(self, table_name: str, field_name: str) -> None:
        """Validate that a field exists in the schema for a table.

        No-op if validate_schema is False.
        """
        if not self._validate_schema:
            return
        schema = self._schemas.get(table_name)
        if schema is None:
            raise InvalidSchemaError(
                ERR_MSG_SCHEMA_VALIDATION_FAILED,
                f"table '{table_name}' not found in schemas",
            )
        if schema.find_field(field_name) is None:
            raise InvalidSchemaError(
                ERR_MSG_SCHEMA_VALIDATION_FAILED,
                f"field '{field_name}' not found in schema for '{table_name}'",
            )

    def _is_member_dot_array_field(self, tree: Tree) -> bool:
        """Check if a tree represents an array field via schema."""
        node = tree
        while isinstance(node, Tree) and node.data in (
            "expr", "conditionalor", "conditionaland", "relation",
            "addition", "multiplication", "unary", "member", "primary",
        ) and len(node.children) == 1:
            node = node.children[0]
        if isinstance(node, Tree) and node.data == "member_dot":
            table = self._get_root_ident(node.children[0])
            field = str(node.children[1])
            if table:
                return self._is_field_array(table, field)
        if isinstance(node, Tree) and node.data == "ident":
            name = str(node.children[0])
            # Check if it's a known array variable
            for schema in self._schemas.values():
                f = schema.find_field(name)
                if f and f.repeated:
                    return True
        return False

    def _is_field_array(self, table_name: str, field_name: str) -> bool:
        schema = self._schemas.get(table_name)
        if not schema:
            return False
        field = schema.find_field(field_name)
        return field is not None and field.repeated

    def _is_timestamp_field(self, root_name: str, tree: Tree) -> bool:
        """Check if a field is a timestamp type based on schema or naming."""
        _TIMESTAMP_NAMES = {"created_at", "updated_at", "timestamp", "ts"}
        # Check the root identifier name directly
        if root_name in _TIMESTAMP_NAMES:
            return True
        # Also walk the tree to find the leaf ident
        node = tree
        while isinstance(node, Tree) and len(node.children) == 1:
            node = node.children[0]
        if isinstance(node, Tree) and node.data == "ident":
            name = str(node.children[0])
            return name in _TIMESTAMP_NAMES
        return False

    @staticmethod
    def _is_numeric_literal(tree: Tree) -> bool:
        """Check if a tree is a numeric literal (int or float)."""
        tok = _get_literal_token(tree)
        if tok is None:
            return False
        return _is_int_token(tok) or _is_float_token(tok) or _is_uint_token(tok)

    def _is_array_expression(self, tree: Tree) -> bool:
        """Check if a tree produces an array result.

        Detects: schema array fields, list literals, split(), filter(), map(),
        empty typed arrays, and similar array-producing expressions.
        """
        node = tree
        while isinstance(node, Tree) and node.data in (
            "expr", "conditionalor", "conditionaland", "relation",
            "addition", "multiplication", "unary", "member", "primary",
        ) and len(node.children) == 1:
            node = node.children[0]

        if not isinstance(node, Tree):
            return False

        # List literal
        if node.data == "list_lit":
            return True

        # Method calls that produce arrays
        if node.data == "member_dot_arg":
            method_name = str(node.children[1]) if len(node.children) > 1 else ""
            return method_name in ("split", "filter", "map")

        # Schema-based array field detection
        if node.data == "member_dot":
            table = self._get_root_ident(node.children[0])
            field = str(node.children[1])
            if table and self._is_field_array(table, field):
                return True

        # Bare ident that's a known array
        if node.data == "ident":
            name = str(node.children[0])
            for schema in self._schemas.values():
                f = schema.find_field(name)
                if f and f.repeated:
                    return True

        return False

    # ---- Utility helpers ----

    def _get_root_ident(self, tree: Tree | Token) -> str | None:
        """Get the root identifier name from a tree."""
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
        """Get the first field name in a member_dot chain."""
        node = obj
        while isinstance(node, Tree):
            if node.data == "member_dot":
                # If the child of member_dot is the root ident, then the field is children[1]
                child = node.children[0]
                self._get_root_ident(child)
                inner_child = child
                while isinstance(inner_child, Tree) and inner_child.data in ("member", "primary") and len(inner_child.children) == 1:
                    inner_child = inner_child.children[0]
                if isinstance(inner_child, Tree) and inner_child.data == "ident":
                    return str(node.children[1])
                node = node.children[0]
            elif len(node.children) == 1:
                node = node.children[0]
            else:
                break
        return fallback

    def _get_ident_name(self, tree: Tree) -> str:
        """Extract identifier name from a tree node."""
        node = tree
        while isinstance(node, Tree):
            if node.data == "ident":
                return str(node.children[0])
            if len(node.children) == 1:
                node = node.children[0]
            else:
                raise InvalidArgumentsError(
                    "expected identifier",
                    f"cannot extract identifier from {tree.data}",
                )
        if isinstance(node, Token) and node.type == "IDENT":
            return str(node)
        raise InvalidArgumentsError(
            "expected identifier",
            "cannot extract identifier from node",
        )

    def _is_comprehension_var(self, name: str) -> bool:
        return name in self._comprehension_vars

    def _is_duration_expression(self, tree: Tree | Token) -> bool:
        """Check if a tree is specifically a duration/interval expression."""
        if isinstance(tree, Token):
            return False
        if tree.data == "ident_arg":
            func_name = str(tree.children[0])
            return func_name in ("duration", "interval")
        return any(
            self._is_duration_expression(child)
            for child in tree.children
            if isinstance(child, (Tree, Token))
            and not (isinstance(child, Tree) and child.data == "exprlist")
        )

    def _is_timestamp_or_duration_context(self, lhs: Tree, rhs: Tree) -> bool:
        """Detect if this is a timestamp/duration arithmetic context.

        We detect this by looking for duration() or interval() or timestamp() calls,
        or known timestamp field names in the expression trees.
        """
        return self._tree_has_temporal(lhs) or self._tree_has_temporal(rhs)

    def _tree_has_temporal(self, tree: Tree | Token) -> bool:
        """Check if a tree contains temporal expressions (duration, interval, timestamp, etc.)."""
        if isinstance(tree, Token):
            return False
        if tree.data == "ident_arg":
            func_name = str(tree.children[0])
            return func_name in (
                "duration", "interval", "timestamp", "date", "time",
                "datetime", "current_date", "current_datetime",
            )
        if tree.data == "ident":
            name = str(tree.children[0])
            return name in (
                "created_at", "updated_at", "birthday", "fixed_time",
                "scheduled_at",
            )
        return any(
            self._tree_has_temporal(child)
            for child in tree.children
            if isinstance(child, (Tree, Token))
        )

    @staticmethod
    def _process_escapes(s: str) -> str:
        """Process CEL string escape sequences."""
        result = []
        i = 0
        while i < len(s):
            if s[i] == "\\" and i + 1 < len(s):
                nxt = s[i + 1]
                if nxt == "n":
                    result.append("\n")
                    i += 2
                elif nxt == "t":
                    result.append("\t")
                    i += 2
                elif nxt == "r":
                    result.append("\r")
                    i += 2
                elif nxt == "\\":
                    result.append("\\")
                    i += 2
                elif nxt == "'":
                    result.append("'")
                    i += 2
                elif nxt == '"':
                    result.append('"')
                    i += 2
                elif nxt == "0":
                    result.append("\0")
                    i += 2
                elif nxt == "x" and i + 3 < len(s):
                    hex_val = s[i + 2 : i + 4]
                    try:
                        result.append(chr(int(hex_val, 16)))
                        i += 4
                    except ValueError:
                        result.append(s[i])
                        i += 1
                elif nxt == "u" and i + 5 < len(s):
                    hex_val = s[i + 2 : i + 6]
                    try:
                        result.append(chr(int(hex_val, 16)))
                        i += 6
                    except ValueError:
                        result.append(s[i])
                        i += 1
                else:
                    result.append(s[i])
                    result.append(nxt)
                    i += 2
            else:
                result.append(s[i])
                i += 1
        return "".join(result)
