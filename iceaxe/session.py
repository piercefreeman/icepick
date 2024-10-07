from collections import defaultdict
from contextlib import asynccontextmanager
from typing import (
    Any,
    Literal,
    ParamSpec,
    Sequence,
    Type,
    TypeVar,
    cast,
    overload,
)

import asyncpg
from mountaineer import CoreDependencies, Depends
from mountaineer.database import DatabaseConfig

from envelope.db.base import TableBase
from envelope.db.queries import (
    QueryBuilder,
    QueryIdentifier,
    is_base_table,
    is_column,
    is_literal,
)
from envelope.logging import LOGGER

P = ParamSpec("P")
T = TypeVar("T")


class DBConnection:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn
        self.obj_to_primary_key: dict[str, str | None] = {}
        self.in_transaction = False

    @asynccontextmanager
    async def transaction(self):
        self.in_transaction = True
        async with self.conn.transaction():
            try:
                yield
            finally:
                self.in_transaction = False

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["SELECT"]]) -> list[T]: ...

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["INSERT"]]) -> None: ...

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["UPDATE"]]) -> None: ...

    async def exec(
        self,
        query: QueryBuilder[T, Literal["SELECT"]]
        | QueryBuilder[T, Literal["INSERT"]]
        | QueryBuilder[T, Literal["UPDATE"]],
    ) -> list[T] | None:
        sql_text, variables = query.build()
        LOGGER.debug(f"Executing query: {sql_text} with variables: {variables}")
        values = await self.conn.fetch(sql_text, *variables)

        if query.query_type == "SELECT":
            # We now need to cast any desired model as the models
            # instead of a blob of fields
            # Field selections should already be in the proper type
            result_all: list[Any] = []

            for value in values:
                # Go through the fields and cast them to the proper type
                result_value: list[Any] = []
                for select_raw in query.select_raw:
                    if is_base_table(select_raw):
                        result_value.append(
                            select_raw(
                                **{
                                    field: value[field]
                                    for field, info in select_raw.model_fields.items()
                                    if not info.exclude
                                }
                            )
                        )
                    elif is_column(select_raw):
                        result_value.append(value[select_raw.key])
                    elif is_literal(select_raw):
                        # result_value.append(value[])
                        # TODO: We need to recover this somehow
                        pass

                if len(result_value) == 1:
                    result_all.append(result_value[0])
                else:
                    result_all.append(tuple(result_value))

            return cast(list[T], result_all)

        return None

    async def insert(self, objects: Sequence[TableBase]):
        if not objects:
            return

        objects_by_class: defaultdict[Type[TableBase], list[TableBase]] = defaultdict(
            list
        )
        for obj in objects:
            objects_by_class[obj.__class__].append(obj)

        for model, model_objects in objects_by_class.items():
            # We let the DB handle autoincrement keys
            auto_increment_keys = [
                field
                for field, info in model.model_fields.items()
                if info.autoincrement
            ]

            table_name = QueryIdentifier(model.get_table_name())
            fields = [
                field
                for field, info in model.model_fields.items()
                if not info.exclude
                and not info.autoincrement
                and field not in auto_increment_keys
            ]
            field_string = ", ".join(f'"{field}"' for field in fields)
            primary_key = self._get_primary_key(model)

            placeholders = ", ".join(f"${i}" for i in range(1, len(fields) + 1))
            query = f"INSERT INTO {table_name} ({field_string}) VALUES ({placeholders})"
            if primary_key:
                query += f" RETURNING {primary_key}"

            async with self._ensure_transaction():
                for obj in model_objects:
                    obj_values = obj.model_dump()
                    values = [obj_values[field] for field in fields]
                    result = await self.conn.fetchrow(query, *values)

                    if primary_key and result:
                        setattr(obj, primary_key, result[primary_key])
                    obj.clear_modified_attributes()

    async def update(self, objects: Sequence[TableBase]):
        if not objects:
            return
        async with self._ensure_transaction():
            for obj in objects:
                modified_attrs = {
                    k: v
                    for k, v in obj.get_modified_attributes().items()
                    if not obj.model_fields[k].exclude
                }
                if not modified_attrs:
                    continue

                model = obj.__class__
                table_name = QueryIdentifier(model.get_table_name())
                primary_key = self._get_primary_key(model)

                if not primary_key:
                    raise ValueError(
                        f"Model {model} has no primary key, required to UPDATE with ORM objects"
                    )

                set_clause = ", ".join(
                    f"{QueryIdentifier(key)} = ${i}"
                    for i, key in enumerate(modified_attrs.keys(), start=2)
                )
                query = f"UPDATE {table_name} SET {set_clause} WHERE id = $1"
                values = [getattr(obj, primary_key)] + list(modified_attrs.values())
                await self.conn.execute(query, *values)
                obj.clear_modified_attributes()

    def _get_primary_key(self, obj: Type[TableBase]) -> str | None:
        table_name = obj.get_table_name()
        if table_name not in self.obj_to_primary_key:
            primary_key = [
                field for field, info in obj.model_fields.items() if info.primary_key
            ]
            self.obj_to_primary_key[table_name] = (
                primary_key[0] if primary_key else None
            )
        return self.obj_to_primary_key[table_name]

    @asynccontextmanager
    async def _ensure_transaction(self):
        """
        For database modifications like INSERT and UPDATE, we need to ensure that we're within
        a transaction so. This helper function will allow us to create a transaction only if
        the client has not already created one.

        """
        if not self.in_transaction:
            async with self.transaction():
                yield
        else:
            yield


async def get_db_connection(
    config: DatabaseConfig = Depends(
        CoreDependencies.get_config_with_type(DatabaseConfig)
    ),
) -> DBConnection:
    conn = await asyncpg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
    )
    return DBConnection(conn)
