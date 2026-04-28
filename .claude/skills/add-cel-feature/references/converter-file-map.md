# Converter File Map

A map of which CEL surface area each section of `src/pycel2sql/_converter.py` owns. Line numbers are approximate (the file evolves) — search by method name to find the current location.

## Contents

- Top-level entry
- Logical operators
- Comparison operators
- `in` operator
- Arithmetic
- Unary
- Member access (dot / index / dot-arg)
- Identifiers and free-function calls
- Literals
- Compound literals (lists, maps)
- String methods (contains, startsWith, endsWith, etc.)
- Size, has, type cast
- Timestamp / duration
- Comprehensions

## Top-level entry

| Method | Owns |
|---|---|
| `expr` | `?:` ternary; otherwise pass-through to single child. |
| `visit` | Override of Lark `Interpreter.visit` to handle bare `Token` children. |
| `_visit_child` | Depth-tracking visitor; every internal recursion goes through this. |

## Logical operators

| Method | Owns |
|---|---|
| `conditionalor` | `lhs \|\| rhs` → `lhs OR rhs`. |
| `conditionaland` | `lhs && rhs` → `lhs AND rhs`. |

## Comparison operators

The `relation` method dispatches to operator-specific helpers. Each `relation_<op>` method handles null-aware emission (`IS NULL` / `IS NOT NULL` / `IS TRUE` / etc.) and JSON-text-extraction numeric coercion.

| Method | Owns |
|---|---|
| `relation` | Dispatches to per-operator helpers. |
| `relation_eq`, `relation_ne` | `==`, `!=`. Handles null/bool comparisons specially. |
| `relation_lt`, `relation_le`, `relation_gt`, `relation_ge` | `<`, `<=`, `>`, `>=`. |
| `relation_in` | `x in [list]` / `x in arr`; routes through `_visit_in` and `Dialect.write_array_membership`. |

## `in` operator

| Method | Owns |
|---|---|
| `_visit_in` | Routes `x in arr` to `Dialect.write_array_membership`. Currently does not yet route JSON-array membership through the dedicated `Dialect.write_json_array_membership` method (those abstract methods exist in the ABC but no call site invokes them yet). |

## Arithmetic

| Method | Owns |
|---|---|
| `addition` | `+`, `-` dispatch. **Critical**: timestamp-arithmetic check happens BEFORE string-concat check (temporal functions contain string literals; CLAUDE.md notes this). |
| `addition_add`, `addition_sub` | Trivial pass-throughs (the work is in `addition`). |
| `multiplication` | `*`, `/`, `%` dispatch. `%` becomes `MOD(a, b)` (universal SQL). |
| `multiplication_mul`, `multiplication_div`, `multiplication_mod` | Trivial pass-throughs. |

## Unary

| Method | Owns |
|---|---|
| `unary`, `unary_not`, `unary_neg` | `!x` → `NOT x`; `-x` → `-x`. |

## Member access

The most-trafficked area. Three entry points:

| Method | Owns |
|---|---|
| `member` | Pass-through. |
| `member_dot` | `obj.field` access. Routes through `_emit_json_path` for JSON-path access (schema-declared JSON or `json_variables`); otherwise emits plain `.field`. Applies `validate_field_name`. |
| `member_dot_arg` | `obj.method(args)` — the **method-call dispatcher**. The long if/elif chain checks `method_name` and routes to the appropriate `_visit_<feature>` helper. New string/timestamp/comprehension methods land here. |
| `member_index` | `obj[idx]` and `obj["key"]`. String literal index → JSON path (when root is a `json_variable`) or `.key` plain access. Integer index → `Dialect.write_list_index_const`. Dynamic index → `Dialect.write_list_index`. |
| `member_object` | Type construction `T{f: v}` (rare). |

When adding a new CEL **method** like `string.toLowerCase()`, the entry point is the `member_dot_arg` if/elif chain.

## Identifiers and free-function calls

| Method | Owns |
|---|---|
| `primary` | Pass-through. |
| `ident` | Bare identifier emission. Applies `column_aliases` (PR #8) before `validate_field_name`. |
| `ident_arg` | `func(args)` — the **free-function-call dispatcher**. Long if/elif chain on `func_name`. Owns `has`, `size`, `matches`, `int`/`uint`/`double`/`string`/`bool`/`bytes`/`timestamp`/`duration` casts, `now()`, `getInterval()`, `dyn()`. |
| `dot_ident_arg`, `dot_ident` | `.name` (rare CEL syntax). |
| `paren_expr` | `(expr)` parenthesisation. |

When adding a new CEL **free function** like `format(...)` or `has(...)`, the entry point is the `ident_arg` if/elif chain.

## Literals

| Method | Owns |
|---|---|
| `literal` | All scalar literals — strings, ints, uints, floats, bools, null, bytes. Routes through dialect-specific `write_string_literal` / `write_bytes_literal`. Parameterized mode replaces literals with placeholders via `_add_param`. |

## Compound literals

| Method | Owns |
|---|---|
| `list_lit` | `[1, 2, 3]` array literals — calls `Dialect.write_array_literal_open` / `write_array_literal_close`. |
| `map_lit` | `{a: 1, b: 2}` map/struct literals — calls `Dialect.write_struct_open` / `write_struct_close`. |
| `exprlist` | Comma-separated expression list (used inside list/map/format args). |
| `mapinits` | Map initialiser list. |
| `fieldinits` | Struct initialiser list. |

## String methods

Each method lives in a `_visit_<name>` helper. Most route through a `Dialect.write_<name>` method to absorb dialect divergence.

| Helper | CEL surface | Notes |
|---|---|---|
| `_visit_contains` | `s.contains(needle)` | → `Dialect.write_contains`. |
| `_visit_starts_with` | `s.startsWith(prefix)` | → `LIKE 'prefix%'` + dialect-specific ESCAPE clause. |
| `_visit_ends_with` | `s.endsWith(suffix)` | → `LIKE '%suffix'`. |
| `_visit_matches_method` | `s.matches(pattern)` | → `Dialect.convert_regex` + `write_regex_match`. |
| `_visit_matches_func` | `matches(s, pattern)` | Same as above, function form. |
| `_visit_char_at` | `s.charAt(idx)` | → `SUBSTRING(s, idx + 1, 1)`. |
| `_visit_index_of` | `s.indexOf(n)` / `s.indexOf(n, off)` | Inline `POSITION(...)` (universal). |
| `_visit_last_index_of` | `s.lastIndexOf(n)` | Inline. |
| `_visit_substring` | `s.substring(start)` / `s.substring(start, end)` | `SUBSTRING(...)`. |
| `_visit_replace` | `s.replace(old, new)` | `REPLACE(...)`. |
| `_visit_split` | `s.split(d)` / `s.split(d, n)` | → `Dialect.write_split` / `write_split_with_limit`. |
| `_visit_join` | `arr.join(d)` | → `Dialect.write_join`. |
| `_visit_format` | `s.format([args])` | → `Dialect.write_format` (added in PR #8). The `%d`/`%f` → `%s` normalization happens unconditionally before dispatch. |

## Size, has, type cast

| Helper | CEL surface | Notes |
|---|---|---|
| `_visit_size_method` | `x.size()` | Routes to `Dialect.write_array_length` for arrays, plain `LENGTH(x)` for strings. |
| `_visit_size_func` | `size(x)` | Same as above, function form. |
| `_visit_has` | `has(a.b)` / `has(jsonvar.k)` | Routes to `Dialect.write_json_existence` for JSON paths; otherwise emits `... IS NOT NULL`. The `json_variables` audit (PR #8) added the JSON-variable branch. |
| `_visit_type_cast` | `int(x)`, `uint(x)`, `double(x)`, `string(x)`, `bool(x)`, `bytes(x)` | Special-case for `int(timestamp)` → `Dialect.write_epoch_extract`. Other casts emit `CAST(x AS <dialect_type>)` via `Dialect.write_type_name`. |

## Timestamp / duration

| Helper | CEL surface | Notes |
|---|---|---|
| `_visit_timestamp_func` | `timestamp("...")` | → `Dialect.write_timestamp_cast`. |
| `_visit_duration_func` | `duration("...")` | Parses Go-style duration string (`24h`, `30m`, `1500ms`). Limits: only h/m/s/ms/us/ns recognised. → `Dialect.write_duration`. |
| `_visit_interval_func` | `interval(value, unit)` | Dynamic-value variant. → `Dialect.write_interval`. |
| `_visit_datetime_constructor` | `date(...)`, `time(...)`, etc. | Constructor-style date/time helpers. |
| `_visit_current_datetime` | `now()`, `today()` | → `CURRENT_TIMESTAMP` / `CURRENT_DATE` (universal). |
| `_visit_timestamp_extract` | `t.getFullYear()`, `t.getMonth()`, `t.getDayOfWeek()`, etc. | Routes to `Dialect.write_extract`. **Critical**: DOW maps differ (Sunday=0 vs Sunday=1) — handled per-dialect in `write_extract`. |

## Comprehensions

| Helper | CEL surface |
|---|---|
| `_visit_comprehension` | Top-level dispatch on macro name. |
| `_visit_comp_all` | `list.all(v, pred)` |
| `_visit_comp_exists` | `list.exists(v, pred)` |
| `_visit_comp_exists_one` | `list.exists_one(v, pred)` |
| `_visit_comp_map` | `list.map(v, transform)` |
| `_visit_comp_map_filter` | `list.map(v, pred, transform)` |
| `_visit_comp_filter` | `list.filter(v, pred)` |

All comprehension helpers use the `Dialect.write_unnest` + `write_array_subquery_open` + `write_array_subquery_expr_close` triple. Spark's variant (`(SELECT collect_list(...))`) needs the close to emit `)` because `collect_list` is an aggregator wrapper; other dialects close with empty.

## Common patterns

When adding a new CEL feature, the workflow is almost always:

1. Find the entry point in `member_dot_arg` (for methods) or `ident_arg` (for free functions).
2. Add an `elif method_name == "<name>": self._visit_<name>(obj, args); return`.
3. Add a `_visit_<name>` helper near the existing string-method helpers (or wherever fits semantically).
4. Decide universal vs dialect-divergent (see SKILL.md "New method or inline?").
5. Add a parametrized test in the appropriate file under `tests/`.
6. If a new `Dialect` `@abstractmethod` is added, update every dialect file AND the dialect-method-checklist in the `add-sql-dialect` skill.
