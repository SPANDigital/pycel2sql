"""BigQuery schema introspection."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pycel2sql._errors import IntrospectionError
from pycel2sql.schema import FieldSchema, Schema


@runtime_checkable
class BQSchemaField(Protocol):
    """Minimal protocol for BigQuery SchemaField."""

    @property
    def name(self) -> str: ...
    @property
    def field_type(self) -> str: ...
    @property
    def mode(self) -> str: ...


@runtime_checkable
class BQTable(Protocol):
    """Minimal protocol for BigQuery Table."""

    @property
    def schema(self) -> list[BQSchemaField]: ...


class BQClient(Protocol):
    """Minimal protocol for BigQuery Client."""

    def get_table(self, table_ref: str) -> BQTable: ...


def introspect_bigquery(
    client: BQClient,
    *,
    table_names: list[str],
    dataset: str | None = None,
) -> dict[str, Schema]:
    """Introspect BigQuery table schemas.

    Table names can be fully qualified (``"dataset.table"``) or plain
    (``"table"``), in which case the ``dataset`` parameter is required.

    Dict keys use the short table name (no dataset prefix).

    Args:
        client: A BigQuery ``Client``.
        table_names: Tables to introspect.
        dataset: Default dataset for unqualified table names.

    Returns:
        Mapping of table name to :class:`~pycel2sql.schema.Schema`.

    Raises:
        IntrospectionError: If a table is not found or ``dataset`` is
            missing for an unqualified table name.
    """
    if not table_names:
        return {}

    result: dict[str, Schema] = {}

    for name in table_names:
        if "." in name:
            table_ref = name
            short_name = name.rsplit(".", 1)[1]
        elif dataset is not None:
            table_ref = f"{dataset}.{name}"
            short_name = name
        else:
            raise IntrospectionError(
                f"dataset required for unqualified table name: {name!r}",
                internal_details=(
                    f"table {name!r} has no dataset prefix and no "
                    f"default dataset was provided"
                ),
            )

        try:
            table = client.get_table(table_ref)
        except Exception as exc:
            raise IntrospectionError(
                f"table not found: {name!r}",
                internal_details=f"get_table({table_ref!r}) failed: {exc}",
                wrapped=exc,
            ) from exc

        fields = [_map_field(f) for f in table.schema]
        result[short_name] = Schema(fields)

    return result


def _map_field(field: Any) -> FieldSchema:
    is_json = field.field_type == "JSON"
    repeated = field.mode == "REPEATED"
    return FieldSchema(name=field.name, is_json=is_json, repeated=repeated)
