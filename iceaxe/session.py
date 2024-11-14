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
from typing_extensions import TypeVarTuple

from iceaxe.base import TableBase
from iceaxe.logging import LOGGER
from iceaxe.queries import (
    QueryBuilder,
    QueryIdentifier,
    is_base_table,
    is_column,
    is_function_metadata,
)
from iceaxe.session_optimized import optimize_exec_casting

P = ParamSpec("P")
T = TypeVar("T")
Ts = TypeVarTuple("Ts")


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

    @overload
    async def exec(self, query: QueryBuilder[T, Literal["DELETE"]]) -> None: ...

    async def exec(
        self,
        query: QueryBuilder[T, Literal["SELECT"]]
        | QueryBuilder[T, Literal["INSERT"]]
        | QueryBuilder[T, Literal["UPDATE"]]
        | QueryBuilder[T, Literal["DELETE"]],
    ) -> list[T] | None:
        sql_text, variables = query.build()
        LOGGER.debug(f"Executing query: {sql_text} with variables: {variables}")
        values = await self.conn.fetch(sql_text, *variables)

        if query.query_type == "SELECT":
            # We now need to cast any desired model as the models
            # instead of a blob of fields
            # Field selections should already be in the proper type
            result_all: list[Any] = []

            # Pre-cache the select types, so we don't have to the runtime inspection of types
            # for each value
            select_types = [
                (
                    is_base_table(select_raw),
                    is_column(select_raw),
                    is_function_metadata(select_raw),
                )
                for select_raw in query.select_raw
            ]

            result_all = optimize_exec_casting(values, query.select_raw, select_types)
            return cast(list[T], result_all)

        return None

    async def insert(self, objects: Sequence[TableBase]):
        if not objects:
            return

        for model, model_objects in self._aggregate_models_by_table(objects):
            table_name = QueryIdentifier(model.get_table_name())
            fields = {
                field: info
                for field, info in model.model_fields.items()
                if (not info.exclude and not info.autoincrement)
            }
            field_string = ", ".join(f'"{field}"' for field in fields)
            primary_key = self._get_primary_key(model)

            placeholders = ", ".join(f"${i}" for i in range(1, len(fields) + 1))
            query = f"INSERT INTO {table_name} ({field_string}) VALUES ({placeholders})"
            if primary_key:
                query += f" RETURNING {primary_key}"

            async with self._ensure_transaction():
                for obj in model_objects:
                    obj_values = obj.model_dump()
                    values = [
                        info.to_db_value(obj_values[field])
                        for field, info in fields.items()
                    ]
                    result = await self.conn.fetchrow(query, *values)

                    if primary_key and result:
                        setattr(obj, primary_key, result[primary_key])
                    obj.clear_modified_attributes()

    @overload
    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: tuple[T, *Ts],
    ) -> list[tuple[T, *Ts]]: ...

    @overload
    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: None,
    ) -> None: ...

    async def upsert(
        self,
        objects: Sequence[TableBase],
        *,
        conflict_fields: tuple[Any, ...],
        update_fields: tuple[Any, ...] | None = None,
        returning_fields: tuple[T, *Ts] | None = None,
    ) -> list[tuple[T, *Ts]] | None:
        """
        Performs an upsert (INSERT ... ON CONFLICT DO UPDATE) operation for the given objects.

        :param objects: Sequence of TableBase objects to upsert
        :param conflict_fields: Fields to check for conflicts (ON CONFLICT)
        :param update_fields: Fields to update on conflict. If None, updates all non-excluded fields
        :param returning_fields: Fields to return after the operation. If None, returns nothing

        :return List of dictionaries containing the returned fields if returning_fields is specified

        """
        if not objects:
            return None

        # Evaluate column types
        conflict_fields_cols = [field for field in conflict_fields if is_column(field)]
        update_fields_cols = [
            field for field in update_fields or [] if is_column(field)
        ]
        returning_fields_cols = [
            field for field in returning_fields or [] if is_column(field)
        ]

        results: list[tuple[T, *Ts]] = []
        async with self._ensure_transaction():
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                fields = {
                    field: info
                    for field, info in model.model_fields.items()
                    if (not info.exclude and not info.autoincrement)
                }

                field_string = ", ".join(f'"{field}"' for field in fields)
                placeholders = ", ".join(f"${i}" for i in range(1, len(fields) + 1))
                query = (
                    f"INSERT INTO {table_name} ({field_string}) VALUES ({placeholders})"
                )
                if conflict_fields_cols:
                    conflict_field_string = ", ".join(
                        f'"{field.key}"' for field in conflict_fields_cols
                    )
                    query += f" ON CONFLICT ({conflict_field_string})"

                    if update_fields_cols:
                        set_values = ", ".join(
                            f'"{field.key}" = EXCLUDED."{field.key}"'
                            for field in update_fields_cols
                        )
                        query += f" DO UPDATE SET {set_values}"
                    else:
                        query += " DO NOTHING"

                if returning_fields_cols:
                    returning_string = ", ".join(
                        f'"{field.key}"' for field in returning_fields_cols
                    )
                    query += f" RETURNING {returning_string}"

                # Execute for each object
                for obj in model_objects:
                    obj_values = obj.model_dump()
                    values = [
                        info.to_db_value(obj_values[field])
                        for field, info in fields.items()
                    ]

                    if returning_fields_cols:
                        result = await self.conn.fetchrow(query, *values)
                        if result:
                            results.append(
                                tuple(
                                    [
                                        result[field.key]
                                        for field in returning_fields_cols
                                    ]
                                )
                            )
                    else:
                        await self.conn.execute(query, *values)

                    obj.clear_modified_attributes()

        return results if returning_fields_cols else None

    async def update(self, objects: Sequence[TableBase]):
        if not objects:
            return

        async with self._ensure_transaction():
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                primary_key = self._get_primary_key(model)

                if not primary_key:
                    raise ValueError(
                        f"Model {model} has no primary key, required to UPDATE with ORM objects"
                    )

                primary_key_name = QueryIdentifier(primary_key)

                for obj in model_objects:
                    modified_attrs = {
                        k: v
                        for k, v in obj.get_modified_attributes().items()
                        if not obj.model_fields[k].exclude
                    }
                    if not modified_attrs:
                        continue

                    set_clause = ", ".join(
                        f"{QueryIdentifier(key)} = ${i}"
                        for i, key in enumerate(modified_attrs.keys(), start=2)
                    )

                    query = f"UPDATE {table_name} SET {set_clause} WHERE {primary_key_name} = $1"
                    values = [getattr(obj, primary_key)] + list(modified_attrs.values())
                    await self.conn.execute(query, *values)
                    obj.clear_modified_attributes()

    async def delete(self, objects: Sequence[TableBase]):
        async with self._ensure_transaction():
            for model, model_objects in self._aggregate_models_by_table(objects):
                table_name = QueryIdentifier(model.get_table_name())
                primary_key = self._get_primary_key(model)

                if not primary_key:
                    raise ValueError(
                        f"Model {model} has no primary key, required to UPDATE with ORM objects"
                    )

                primary_key_name = QueryIdentifier(primary_key)

                for obj in model_objects:
                    query = f"DELETE FROM {table_name} WHERE {primary_key_name} = $1"
                    await self.conn.execute(query, getattr(obj, primary_key))

    async def refresh(self, objects: Sequence[TableBase]):
        for model, model_objects in self._aggregate_models_by_table(objects):
            table_name = QueryIdentifier(model.get_table_name())
            primary_key = self._get_primary_key(model)
            fields = [
                field for field, info in model.model_fields.items() if not info.exclude
            ]

            if not primary_key:
                raise ValueError(
                    f"Model {model} has no primary key, required to UPDATE with ORM objects"
                )

            primary_key_name = QueryIdentifier(primary_key)
            object_ids = {getattr(obj, primary_key) for obj in model_objects}

            query = f"SELECT * FROM {table_name} WHERE {primary_key_name} = ANY($1)"
            results = {
                result[primary_key]: result
                for result in await self.conn.fetch(query, list(object_ids))
            }

            # Update the objects in-place
            for obj in model_objects:
                obj_id = getattr(obj, primary_key)
                if obj_id in results:
                    # Update field-by-field
                    for field in fields:
                        setattr(obj, field, results[obj_id][field])
                else:
                    LOGGER.error(
                        f"Object {obj} with primary key {obj_id} not found in database"
                    )

    def _aggregate_models_by_table(self, objects: Sequence[TableBase]):
        objects_by_class: defaultdict[Type[TableBase], list[TableBase]] = defaultdict(
            list
        )
        for obj in objects:
            objects_by_class[obj.__class__].append(obj)

        return objects_by_class.items()

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
