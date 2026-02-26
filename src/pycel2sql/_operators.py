"""CEL-to-SQL operator mappings."""

# Lark relation rule name -> SQL operator
COMPARISON_OPERATORS: dict[str, str] = {
    "relation_eq": "=",
    "relation_ne": "!=",
    "relation_lt": "<",
    "relation_le": "<=",
    "relation_gt": ">",
    "relation_ge": ">=",
}

# SQL operators that need special NULL/BOOL handling
NULL_AWARE_OPS = {"relation_eq", "relation_ne"}

# Numeric comparison operator rule names
NUMERIC_COMPARISON_OPS = {
    "relation_eq",
    "relation_ne",
    "relation_lt",
    "relation_le",
    "relation_gt",
    "relation_ge",
}
