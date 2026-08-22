"""MySQL dialect implementation."""

from __future__ import annotations

from io import StringIO

from pycel2sql._errors import UnsupportedDialectFeatureError
from pycel2sql._utils import convert_re2_to_mysql, validate_sql_field_name
from pycel2sql.dialect._base import Dialect, WriteFunc

# MySQL reserved keywords
_MYSQL_RESERVED: set[str] = {
    "accessible", "add", "all", "alter", "analyze", "and", "as", "asc",
    "asensitive", "before", "between", "bigint", "binary", "blob", "both",
    "by", "call", "cascade", "case", "change", "char", "character", "check",
    "collate", "column", "condition", "constraint", "continue", "convert",
    "create", "cross", "cube", "cume_dist", "current_date", "current_time",
    "current_timestamp", "current_user", "cursor", "database", "databases",
    "day_hour", "day_microsecond", "day_minute", "day_second", "dec",
    "decimal", "declare", "default", "delayed", "delete", "dense_rank",
    "desc", "describe", "deterministic", "distinct", "distinctrow", "div",
    "double", "drop", "dual", "each", "else", "elseif", "empty",
    "enclosed", "escaped", "except", "exists", "exit", "explain", "false",
    "fetch", "float", "float4", "float8", "for", "force", "foreign",
    "from", "fulltext", "function", "generated", "get", "grant", "group",
    "grouping", "groups", "having", "high_priority", "hour_microsecond",
    "hour_minute", "hour_second", "if", "ignore", "in", "index", "infile",
    "inner", "inout", "insensitive", "insert", "int", "int1", "int2",
    "int3", "int4", "int8", "integer", "interval", "into", "io_after_gtids",
    "io_before_gtids", "is", "iterate", "join", "json_table", "key",
    "keys", "kill", "lag", "last_value", "lateral", "lead", "leading",
    "leave", "left", "like", "limit", "linear", "lines", "load",
    "localtime", "localtimestamp", "lock", "long", "longblob", "longtext",
    "loop", "low_priority", "master_bind", "master_ssl_verify_server_cert",
    "match", "maxvalue", "mediumblob", "mediumint", "mediumtext", "member",
    "merge", "middleint", "minute_microsecond", "minute_second", "mod",
    "modifies", "natural", "not", "no_write_to_binlog", "null",
    "numeric", "of", "on", "optimize", "optimizer_costs", "option",
    "optionally", "or", "order", "out", "outer", "outfile", "over",
    "partition", "percent_rank", "primary", "procedure", "purge",
    "range", "rank", "read", "reads", "read_write", "real", "recursive",
    "references", "regexp", "release", "rename", "repeat", "replace",
    "require", "resignal", "restrict", "return", "revoke", "right",
    "rlike", "row", "rows", "row_number", "schema", "schemas",
    "second_microsecond", "select", "sensitive", "separator", "set",
    "show", "signal", "smallint", "spatial", "specific", "sql",
    "sqlexception", "sqlstate", "sqlwarning", "sql_big_result",
    "sql_calc_found_rows", "sql_small_result", "ssl", "starting",
    "stored", "straight_join", "system", "table", "terminated", "then",
    "tinyblob", "tinyint", "tinytext", "to", "trailing", "trigger",
    "true", "undo", "union", "unique", "unlock", "unsigned", "update",
    "usage", "use", "using", "utc_date", "utc_time", "utc_timestamp",
    "values", "varbinary", "varchar", "varcharacter", "varying", "virtual",
    "when", "where", "while", "window", "with", "write", "xor",
    "year_month", "zerofill",
}

# CEL type name -> MySQL type name
_TYPE_MAP: dict[str, str] = {
    "bool": "UNSIGNED",
    "bytes": "BINARY",
    "double": "DECIMAL",
    "int": "SIGNED",
    "uint": "UNSIGNED",
    "string": "CHAR",
    "timestamp": "DATETIME",
}


class MySQLDialect(Dialect):
    """MySQL dialect for CEL-to-SQL conversion."""

    TYPE_MAP = _TYPE_MAP

    # --- Literals ---
    # write_string_literal: inherits the base default (single-quote escaping).

    def write_bytes_literal(self, w: StringIO, value: bytes) -> None:
        hex_str = value.hex().upper()
        w.write(f"X'{hex_str}'")

    def write_param_placeholder(self, w: StringIO, param_index: int) -> None:
        w.write("?")

    # --- Operators ---

    def write_string_concat(
        self, w: StringIO, write_lhs: WriteFunc, write_rhs: WriteFunc
    ) -> None:
        w.write("CONCAT(")
        write_lhs()
        w.write(", ")
        write_rhs()
        w.write(")")

    def write_regex_match(
        self, w: StringIO, write_target: WriteFunc, pattern: str, case_insensitive: bool
    ) -> None:
        write_target()
        w.write(" REGEXP ")
        escaped = pattern.replace("'", "''")
        w.write(f"'{escaped}'")

    def write_like_escape(self, w: StringIO) -> None:
        w.write(" ESCAPE '\\\\'")

    def write_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        w.write("JSON_CONTAINS(")
        write_array()
        w.write(", JSON_EXTRACT(JSON_ARRAY(")
        write_elem()
        w.write("), '$[0]'))")

    # --- Type Casting ---

    def write_cast_to_numeric(self, w: StringIO, write_expr: WriteFunc) -> None:
        write_expr()
        w.write(" + 0")

    def write_epoch_extract(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("UNIX_TIMESTAMP(")
        write_expr()
        w.write(")")

    def write_timestamp_cast(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("CAST(")
        write_expr()
        w.write(" AS DATETIME)")

    # --- Arrays ---

    def write_array_literal_open(self, w: StringIO) -> None:
        w.write("JSON_ARRAY(")

    def write_array_literal_close(self, w: StringIO) -> None:
        w.write(")")

    def write_array_length(
        self, w: StringIO, dimension: int, write_expr: WriteFunc
    ) -> None:
        w.write("COALESCE(JSON_LENGTH(")
        write_expr()
        w.write("), 0)")

    def write_list_index(
        self, w: StringIO, write_array: WriteFunc, write_index: WriteFunc
    ) -> None:
        w.write("JSON_EXTRACT(")
        write_array()
        w.write(", CONCAT('$[', ")
        write_index()
        w.write(", ']'))")

    def write_list_index_const(
        self, w: StringIO, write_array: WriteFunc, index: int
    ) -> None:
        w.write("JSON_EXTRACT(")
        write_array()
        w.write(f", '$[{index}]')")

    def write_empty_typed_array(self, w: StringIO, type_name: str) -> None:
        w.write("JSON_ARRAY()")

    # --- JSON ---

    def write_json_field_access(
        self, w: StringIO, write_base: WriteFunc, field_name: str, is_final: bool
    ) -> None:
        write_base()
        escaped = field_name.replace("'", "''")
        if is_final:
            w.write(f"->>'$.{escaped}'")
        else:
            w.write(f"->'$.{escaped}'")

    def write_json_existence(
        self, w: StringIO, is_jsonb: bool, field_name: str, write_base: WriteFunc
    ) -> None:
        escaped = field_name.replace("'", "''")
        w.write("JSON_CONTAINS_PATH(")
        write_base()
        w.write(f", 'one', '$.{escaped}')")

    def write_json_array_elements(
        self, w: StringIO, is_jsonb: bool, as_text: bool, write_expr: WriteFunc
    ) -> None:
        w.write("JSON_TABLE(")
        write_expr()
        w.write(", '$[*]' COLUMNS(value TEXT PATH '$'))")

    def write_json_array_length(self, w: StringIO, write_expr: WriteFunc) -> None:
        w.write("COALESCE(JSON_LENGTH(")
        write_expr()
        w.write("), 0)")

    def write_json_array_membership(
        self, w: StringIO, json_func: str, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        w.write("JSON_OVERLAPS(JSON_ARRAY(")
        write_elem()
        w.write("), ")
        write_array()
        w.write(")")

    def write_nested_json_array_membership(
        self, w: StringIO, write_elem: WriteFunc, write_array: WriteFunc
    ) -> None:
        w.write("JSON_OVERLAPS(JSON_ARRAY(")
        write_elem()
        w.write("), ")
        write_array()
        w.write(")")

    # --- Timestamps ---
    # write_duration / write_interval: inherit the base `INTERVAL <n> <unit>` defaults.

    def write_extract(
        self,
        w: StringIO,
        part: str,
        write_expr: WriteFunc,
        write_tz: WriteFunc | None,
    ) -> None:
        if part == "DOW":
            # MySQL DAYOFWEEK: 1=Sunday; convert to CEL ISO: 0=Monday
            w.write("(DAYOFWEEK(")
            write_expr()
            w.write(") + 5) % 7")
            return
        w.write(f"EXTRACT({part} FROM ")
        write_expr()
        w.write(")")

    # write_timestamp_arithmetic: inherits the base infix `<ts> <op> <dur>` default.

    # --- String Functions ---

    def write_contains(
        self, w: StringIO, write_haystack: WriteFunc, write_needle: WriteFunc
    ) -> None:
        w.write("LOCATE(")
        write_needle()
        w.write(", ")
        write_haystack()
        w.write(") > 0")

    def write_split(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc
    ) -> None:
        # MySQL doesn't have a native split; simplified implementation
        w.write("JSON_ARRAY(")
        write_str()
        w.write(")")

    def write_split_with_limit(
        self, w: StringIO, write_str: WriteFunc, write_delim: WriteFunc, limit: int
    ) -> None:
        w.write("JSON_ARRAY(")
        write_str()
        w.write(")")

    def write_join(
        self, w: StringIO, write_array: WriteFunc, write_delim: WriteFunc
    ) -> None:
        w.write("JSON_UNQUOTE(")
        write_array()
        w.write(")")

    def write_format(
        self, w: StringIO, fmt_string: str, write_args: list[WriteFunc]
    ) -> None:
        raise UnsupportedDialectFeatureError(
            "format() is not supported in MySQL",
            "MySQL has no equivalent to FORMAT/printf for templated strings",
        )

    # --- Comprehensions ---

    def write_unnest(self, w: StringIO, write_source: WriteFunc) -> None:
        w.write("JSON_TABLE(")
        write_source()
        w.write(", '$[*]' COLUMNS(value TEXT PATH '$'))")

    def write_comprehension_source(
        self, w: StringIO, write_source: WriteFunc, iter_var: str
    ) -> None:
        # JSON_TABLE is table-valued, so the value column is renamed to
        # iter_var through a derived table (JSON_TABLE itself needs an alias).
        w.write(f"(SELECT value AS {iter_var} FROM ")
        self.write_unnest(w, write_source)
        w.write(" AS jt) AS _t")

    def write_comprehension_exists(self, w: StringIO, write_body: WriteFunc) -> None:
        # Not EXISTS: the MySQL 8.x optimizer turns a correlated EXISTS into a
        # semijoin and loses the correlation to JSON_TABLE, silently matching
        # nothing (works from 9.x). COUNT comparisons are never transformed.
        w.write("(SELECT COUNT(*) FROM ")
        write_body()
        w.write(") > 0")

    def write_comprehension_not_exists(self, w: StringIO, write_body: WriteFunc) -> None:
        w.write("(SELECT COUNT(*) FROM ")
        write_body()
        w.write(") = 0")

    def write_array_subquery_open(self, w: StringIO) -> None:
        w.write("(SELECT JSON_ARRAYAGG(")

    def write_array_subquery_expr_close(self, w: StringIO) -> None:
        w.write(")")  # Close JSON_ARRAYAGG(

    # --- Regex ---

    def convert_regex(self, re2_pattern: str) -> tuple[str, bool]:
        return convert_re2_to_mysql(re2_pattern)

    # --- Struct ---

    def write_struct_open(self, w: StringIO) -> None:
        w.write("ROW(")

    # write_struct_close: inherits the base `)` default.

    # --- Validation ---

    def max_identifier_length(self) -> int:
        return 64

    def validate_field_name(self, name: str) -> None:
        validate_sql_field_name(
            name,
            reserved=_MYSQL_RESERVED,
            keyword_label="MySQL",
            max_length=64,
        )

    # --- Capabilities ---

    def supports_native_arrays(self) -> bool:
        return False

    def supports_jsonb(self) -> bool:
        return False
