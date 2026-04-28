# Dialect Method Checklist

Every abstract method on the `Dialect` ABC (`src/pycel2sql/dialect/_base.py`), grouped by category, with one-line "what to emit" guidance per method drawn from the six existing implementations.

## Contents

- Literals
- Operators
- Type casting
- Arrays
- JSON
- Timestamps
- String functions
- Comprehensions
- Regex
- Struct
- Validation
- Capabilities

## Literals

| Method | What to emit | Examples |
|---|---|---|
| `write_string_literal(w, value)` | Single-quoted string with `''` escaping (or `\\'` for BigQuery). | Postgres/DuckDB/MySQL: `'foo''bar'`. BigQuery: `'foo\'bar'`. |
| `write_bytes_literal(w, value)` | Hex-encoded byte literal in the engine's preferred form. | Postgres: `'\\x...'`. SQLite/Spark: `X'...'`. BigQuery: `b"..."` form. |
| `write_param_placeholder(w, param_index)` | Numbered or positional placeholder. | Postgres/DuckDB: `$N`. BigQuery: `@pN`. MySQL/SQLite/Spark: `?` (index ignored). |

## Operators

| Method | What to emit |
|---|---|
| `write_string_concat(w, write_lhs, write_rhs)` | Engine's concat form. Postgres/DuckDB: `lhs \|\| rhs`. MySQL: `CONCAT(lhs, rhs)`. SQLite: `lhs \|\| rhs`. BigQuery: `CONCAT(lhs, rhs)`. Spark: `concat(lhs, rhs)`. |
| `write_regex_match(w, write_target, pattern, case_insensitive)` | Operator or function call. Postgres: `target ~ 'p'` / `~* 'p'`. DuckDB: `regexp_matches(target, 'p')`. MySQL: `target REGEXP 'p'`. BigQuery: `REGEXP_CONTAINS(target, 'p')`. Spark: `target RLIKE 'p'`. SQLite: raises (no portable regex). |
| `write_like_escape(w)` | The trailing `ESCAPE` clause for `LIKE`. Postgres/DuckDB: ` ESCAPE '\\'`. SQLite: ` ESCAPE '\\'`. MySQL: ` ESCAPE '\\\\'`. BigQuery: empty (no ESCAPE supported). Spark: ` ESCAPE '\\\\'`. |
| `write_array_membership(w, write_elem, write_array)` | `elem` membership in array. Postgres: `elem = ANY(array)`. DuckDB: `elem = ANY(array)`. BigQuery: `elem IN UNNEST(array)`. Spark: `array_contains(array, elem)` — note arg-order swap. MySQL/SQLite: emit through JSON-array path (no native arrays). |

## Type casting

| Method | What to emit |
|---|---|
| `write_cast_to_numeric(w, write_expr)` | Force string→number coercion. Postgres: `expr::numeric`. DuckDB: `expr::DOUBLE`. BigQuery: `CAST(expr AS FLOAT64)`. MySQL/SQLite/Spark: `expr + 0` (arithmetic coercion). |
| `write_type_name(w, cel_type_name)` | Engine type name for explicit casts. Postgres: lowercase (`bigint`, `double precision`). MySQL: uppercase (`SIGNED`, `DOUBLE`). BigQuery: `BIGNUMERIC`/`FLOAT64`. Spark: `BIGINT`/`DOUBLE`/`STRING`. |
| `write_epoch_extract(w, write_expr)` | `int(timestamp)` → epoch seconds. Postgres: `EXTRACT(EPOCH FROM expr)::bigint`. DuckDB: `EXTRACT(EPOCH FROM expr)::BIGINT`. MySQL: `UNIX_TIMESTAMP(expr)`. BigQuery: `UNIX_SECONDS(expr)`. Spark: `UNIX_TIMESTAMP(expr)`. SQLite: `CAST(strftime('%s', expr) AS INTEGER)`. |
| `write_timestamp_cast(w, write_expr)` | `timestamp(string)`. Postgres/DuckDB: `CAST(expr AS TIMESTAMPTZ)`. MySQL: `CAST(expr AS DATETIME)`. BigQuery: `CAST(expr AS TIMESTAMP)`. Spark: `CAST(expr AS TIMESTAMP)`. SQLite: `datetime(expr)`. |

## Arrays

| Method | What to emit |
|---|---|
| `write_array_literal_open(w)` / `write_array_literal_close(w)` | Open/close array literal. Postgres: `ARRAY[` / `]`. DuckDB/BigQuery: `[` / `]`. Spark: `array(` / `)`. MySQL: `JSON_ARRAY(` / `)`. SQLite: `json_array(` / `)`. |
| `write_array_length(w, dimension, write_expr)` | Length, NULL-safe. Wrap in `COALESCE(..., 0)` — every existing dialect does this. Multi-dim raises `UnsupportedDialectFeatureError` for engines without portable multi-dim length (Spark). |
| `write_list_index(w, write_array, write_index)` | Dynamic index. 1-indexed engines (Postgres, DuckDB, MySQL, SQLite): emit `arr[idx + 1]`. 0-indexed (BigQuery): `arr[OFFSET(idx)]`. Spark: `arr[idx]` (0-indexed direct). |
| `write_list_index_const(w, write_array, index)` | Constant-int index — same shapes as `write_list_index` with the integer baked in. |
| `write_empty_typed_array(w, type_name)` | Empty typed array literal for `split(s, d, 0)` etc. Postgres: `ARRAY[]::<type>[]`. DuckDB: `[]::<type>[]`. BigQuery: `ARRAY<<type>>[]`. Spark: `CAST(array() AS ARRAY<<type>>)`. |

## JSON

| Method | What to emit |
|---|---|
| `write_json_field_access(w, write_base, field_name, is_final)` | Access a JSON field. Postgres/DuckDB: `base->'field'` (intermediate) / `base->>'field'` (final). MySQL: `base->>'$.field'` (always text). BigQuery: `JSON_QUERY(base, '$.field')` / `JSON_VALUE(base, '$.field')`. Spark: `get_json_object(base, '$.field')` (single function for both). SQLite: `json_extract(base, '$.field')`. |
| `write_json_existence(w, is_jsonb, field_name, write_base)` | `has(base.field)`. Postgres JSONB: `base ? 'field'`. Postgres JSON: `base->>'field' IS NOT NULL`. Others: `<extract> IS NOT NULL`. |
| `write_json_array_elements(w, is_jsonb, as_text, write_expr)` | Set-returning expression for `FROM <here>` in comprehensions. Postgres: `jsonb_array_elements_text(expr)`. DuckDB: `json_each(expr)` style. BigQuery: `UNNEST(JSON_QUERY_ARRAY(expr))`. Spark: `EXPLODE(from_json(expr, 'ARRAY<STRING>'))`. SQLite: `json_each(expr)`. |
| `write_json_array_length(w, write_expr)` | NULL-safe length of a JSON array column. **Wrap in `COALESCE(..., 0)`** — every dialect does this; the BigQuery wrap was added in PR #8 to match. |
| `write_json_array_membership(w, json_func, write_expr)` | RHS for `lhs = <subquery>` in comprehensions. SQLite: `(SELECT value FROM json_each(expr))`. Spark: raises (no portable boolean-predicate form available without candidate element). |
| `write_nested_json_array_membership(w, write_expr)` | Same as above but for nested chains. |

## Timestamps

| Method | What to emit |
|---|---|
| `write_duration(w, value, unit)` | Constant duration literal. Postgres/DuckDB: `INTERVAL 'N unit'`. MySQL/SQLite: dialect-specific INTERVAL syntax. Spark: `INTERVAL N unit`. BigQuery: `INTERVAL N unit`. |
| `write_interval(w, write_value, unit)` | Dynamic-value INTERVAL. Same shapes as above with the value coming from a callback. |
| `write_extract(w, part, write_expr, write_tz)` | `EXTRACT(part FROM expr)`. **DOW special case**: Sunday=1 (BigQuery, Spark) vs Sunday=0 (Postgres convention). Adjust with `(dayofweek(expr) - 1)` (Spark) or modulo arithmetic (BigQuery). |
| `write_timestamp_arithmetic(w, op, write_ts, write_dur)` | `timestamp +/- duration`. Postgres/DuckDB: `ts op dur`. BigQuery: `TIMESTAMP_ADD(ts, dur)` / `TIMESTAMP_SUB(...)`. MySQL: `DATE_ADD(...)` / `DATE_SUB(...)`. SQLite: `datetime(ts, '<sign>N unit')`. Spark: `ts op dur`. |

## String functions

| Method | What to emit |
|---|---|
| `write_contains(w, write_haystack, write_needle)` | `haystack.contains(needle)` → boolean. Postgres: `POSITION(needle IN haystack) > 0`. DuckDB: `CONTAINS(haystack, needle)`. MySQL: `LOCATE(needle, haystack) > 0`. BigQuery: `STRPOS(haystack, needle) > 0`. Spark: `LOCATE(needle, haystack) > 0`. SQLite: `INSTR(haystack, needle) > 0`. |
| `write_split(w, write_str, write_delim)` | Split into array. Postgres: `STRING_TO_ARRAY(s, d)`. DuckDB: `STRING_SPLIT(s, d)`. BigQuery: `SPLIT(s, d)`. Spark: `split(s, d)`. MySQL: `JSON_ARRAY(s)` (cannot split into a SQL array; emits a single-element JSON array). SQLite: raises. |
| `write_split_with_limit(w, write_str, write_delim, limit)` | 3-arg split. Spark/Postgres-style: `split(s, d, limit)` or 2-arg + slice. BigQuery: `SPLIT(...)` with `WHERE OFFSET < limit`. |
| `write_join(w, write_array, write_delim)` | Array → string. Postgres/DuckDB: `ARRAY_TO_STRING(arr, delim, '')`. BigQuery: `ARRAY_TO_STRING(arr, delim)`. Spark: `array_join(arr, delim)`. MySQL: `JSON_UNQUOTE(arr)` (no-op fallback). SQLite: raises. |
| `write_format(w, fmt_string, write_args)` | `string.format([args])`. Postgres/BigQuery: `FORMAT('fmt', ...)`. SQLite/DuckDB: `printf('fmt', ...)`. Spark: `format_string('fmt', ...)`. MySQL: raises `UnsupportedDialectFeatureError` (no equivalent). |

## Comprehensions

| Method | What to emit |
|---|---|
| `write_unnest(w, write_source)` | Set-returning expression for `FROM <here>`. Postgres/DuckDB/BigQuery: `UNNEST(source)`. Spark: `EXPLODE(source)`. MySQL: `JSON_TABLE(source, '$[*]' COLUMNS(...))`. SQLite: `json_each(source)`. |
| `write_array_subquery_open(w)` | Opens an `ARRAY(SELECT ...)` wrapper for `map()` / `filter()`. Postgres/DuckDB: `ARRAY(SELECT `. BigQuery: `ARRAY(SELECT `. Spark: `(SELECT collect_list(` (different — `collect_list` aggregator). MySQL/SQLite: subquery scaffolding. |
| `write_array_subquery_expr_close(w)` | Closes the inner expression before the FROM clause. Postgres/DuckDB: `` (no-op). Spark: `)` (closes `collect_list`). |

## Regex

| Method | What to emit |
|---|---|
| `convert_regex(re2_pattern)` | Validate RE2 pattern + convert to engine-native form. Returns `(pattern, case_insensitive)`. Postgres/DuckDB/Spark: passthrough after ReDoS validators. MySQL: convert to MySQL POSIX form. SQLite: not called (regex unsupported). |

## Struct

| Method | What to emit |
|---|---|
| `write_struct_open(w)` / `write_struct_close(w)` | Struct/record literal opener and closer. Postgres: `ROW(` / `)`. DuckDB: `{` / `}` (struct literal). BigQuery: `STRUCT(` / `)`. Spark: `struct(` / `)`. MySQL/SQLite: `JSON_OBJECT(` / `)` or similar. |

## Validation

| Method | What to emit |
|---|---|
| `max_identifier_length()` | Engine's identifier length limit. Postgres/MySQL: 63/64. BigQuery: 1024. Spark: 128. SQLite: no limit (returns 0). |
| `validate_field_name(name)` | Raise `InvalidFieldNameError` for invalid names. Should check empty, length, regex, reserved-keyword set. |

## Capabilities

| Method | What to emit |
|---|---|
| `supports_native_arrays()` | True for Postgres/DuckDB/BigQuery/Spark; False for MySQL/SQLite (use JSON arrays). |
| `supports_jsonb()` | True for Postgres only (JSONB-specific behaviour like `?` operator). False everywhere else. |
