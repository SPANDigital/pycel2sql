# Go → Python Idioms

Mapping table for porting upstream cel2sql Go code into pycel2sql Python. Distilled from the actual edits in PR #8 (Spark dialect + observe-fork options + BigQuery COALESCE fix).

## Contents

- Closures and callbacks
- Errors and panics
- Interfaces, structs, and ABCs
- Generics
- Strings, bytes, and rune handling
- Maps and sets
- Time and durations
- Reflection and type tags

## Closures and callbacks

Go's `func() error` closures correspond directly to Python's `Callable[[], None]` (pycel2sql doesn't use Python's exception-by-return pattern; errors propagate via raised exceptions).

| Go | Python | Notes |
|---|---|---|
| `func(w *strings.Builder) { ... }` | `lambda: ...` | The shared StringIO buffer (`self._w`) plays the role of `*strings.Builder`. |
| `WriteFunc func() error` | `WriteFunc = Callable[[], None]` | Defined in `dialect/_base.py`. Errors propagate via raised `ConversionError`, not return values. |
| `c.write("FORMAT(")` (Go method on converter) | `self._dialect.write_format(self._w, fmt, write_args)` | Python uses an explicit dialect parameter rather than method receivership; the `Dialect` ABC dispatches polymorphically. |

## Errors and panics

Go's sentinel-error pattern (`var ErrFoo = errors.New("foo")` + `errors.Is(err, ErrFoo)`) translates to typed exception subclasses in pycel2sql.

| Go | Python |
|---|---|
| `var ErrUnsupportedFeature = errors.New("unsupported")` | `class UnsupportedDialectFeatureError(ConversionError): ...` (in `_errors.py`) |
| `return fmt.Errorf("%w: ...", ErrFoo)` | `raise UnsupportedDialectFeatureError(user_msg, internal_msg)` |
| `errors.Is(err, ErrFoo)` | `isinstance(exc, UnsupportedDialectFeatureError)` |
| `panic("invariant violated")` | `raise AssertionError(...)` (rare; pycel2sql avoids panic-style errors) |

The dual-messaging pattern (`user_message` vs `internal_details`) is intentional — see `_errors.py:ConversionError`. The user-visible message is sanitized to prevent CWE-209 information disclosure; the internal message has the full diagnostic detail.

Don't port the upstream's 16 sentinel errors literally. pycel2sql uses ~16 typed subclasses on a single base class — the surface is the same; the implementation is more idiomatic for Python.

## Interfaces, structs, and ABCs

| Go | Python |
|---|---|
| `type Dialect interface { ... }` | `class Dialect(ABC): @abstractmethod def ...` |
| `type DuckDBDialect struct{}` | `class DuckDBDialect(Dialect): ...` |
| `var _ Dialect = (*DuckDBDialect)(nil)` (compile-time interface check) | `Dialect`'s `@abstractmethod`s are checked at instantiation; `DuckDBDialect()` raises `TypeError: Can't instantiate abstract class` if any are missing. CI's `tests/conftest.py::ALL_DIALECTS` instantiates every dialect. |
| `func (d *DuckDBDialect) WriteX(w, write)` | `def write_x(self, w: StringIO, write: WriteFunc) -> None:` |

Go method names are `PascalCase`; pycel2sql uses `snake_case`. The mapping is mechanical: `WriteJSONFieldAccess` → `write_json_field_access`.

Go struct fields (e.g. `c.dialect dialect.Dialect`) become instance attributes (`self._dialect: Dialect`). Underscore prefix denotes "internal" (Python's loose convention).

## Generics

Go 1.18+ generics rarely appear in cel2sql. When they do (e.g. typed slices via `[]T`), Python uses `list[T]` or `Sequence[T]`. `TypeVar` is rare in this codebase — most "generic" Go shapes become typed `list` / `dict` annotations.

## Strings, bytes, and rune handling

| Go | Python |
|---|---|
| `[]byte` | `bytes` |
| `string` | `str` |
| `strconv.Quote(s)` | `repr(s)` (rough — pycel2sql uses `_utils.escape_like_pattern` and dialect-specific `write_string_literal` for SQL string escaping) |
| `strings.Builder` | `io.StringIO` (via `self._w` on the Converter) |
| `strings.HasPrefix(s, p)` | `s.startswith(p)` |
| `strings.Contains(s, sub)` | `sub in s` |
| Hex byte format `%x` | `bytes.hex()` |

## Maps and sets

| Go | Python |
|---|---|
| `map[string]bool{"a": true}` | `set[str] = {"a"}` (or `frozenset` if immutable, e.g. `_json_variables`) |
| `map[string]string` | `dict[str, str]` |
| `for k, v := range m { ... }` | `for k, v in m.items(): ...` |
| `_, ok := m[k]; if ok { ... }` | `if k in m: ...` |

The `frozenset` distinction matters in pycel2sql: per-call options like `json_variables` accept any iterable but normalise to `frozenset` internally so they're hashable and unmodifiable.

## Time and durations

| Go | Python |
|---|---|
| `time.Duration` (nanoseconds) | `datetime.timedelta` (usually unused in Converter — duration parsing happens at the string level, not via timedelta) |
| Go-style duration string `"24h"` | Parsed by `_visit_duration_func`'s pattern matching — see `_converter.py:_parse_duration`. Only h/m/s/ms/us/ns recognised. |

Note: pycel2sql doesn't parse durations into `timedelta` and re-render — it emits the raw value/unit pair into the SQL `INTERVAL` literal.

## Reflection and type tags

| Go | Python |
|---|---|
| `reflect.TypeOf(x)` | `type(x)` |
| Struct tags `\`json:"foo"\`` | `dataclasses.field(metadata=...)` (rare; pycel2sql doesn't serialize) |
| `interface{}` (any value) | `Any` from `typing` |

Reflection is rare in pycel2sql — if the upstream uses heavy reflection, the port likely needs a different shape (often a `dict` lookup or an explicit `isinstance` chain).

## Common port shapes

Three patterns recur:

1. **New `Dialect` method**: add `@abstractmethod` to `_base.py`, implement in all six dialects, add the `write_args: list[WriteFunc]` callback shape if needed. Mirrors Go's `Dialect.WriteX(w, write_args ...func() error)`.
2. **New `Converter` visit branch**: add an `elif` to `member_dot_arg` (method calls) or `ident_arg` (free functions), add a `_visit_<name>` helper. Mirrors Go's `case "<name>":` in the visitor.
3. **New `ConvertOption`**: add a kwarg to `convert()` / `convert_parameterized()` / `analyze()` in `__init__.py`, thread through `Converter.__init__`, store as `self._<name>`. Mirrors Go's functional options (`func WithX(...) ConvertOption`).
