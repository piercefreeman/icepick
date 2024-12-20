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

TableType = TypeVar("TableType", bound=TableBase)


class DBConnection:
    """
    Core class for all ORM actions against a PostgreSQL database. Provides high-level methods
    for executing queries and managing database transactions.

    The DBConnection wraps an asyncpg Connection and provides ORM functionality for:
    - Executing SELECT/INSERT/UPDATE/DELETE queries
    - Managing transactions
    - Inserting, updating, and deleting model instances
    - Refreshing model instances from the database

    ```python {{sticky: True}}
    # Create a connection
    conn = DBConnection(
        await asyncpg.connect(
            host="localhost",
            port=5432,
            user="db_user",
            password="yoursecretpassword",
            database="your_db",
        )
    )

    # Use with models
    class User(TableBase):
        id: int = Field(primary_key=True)
        name: str
        email: str

    # Insert data
    user = User(name="Alice", email="alice@example.com")
    await conn.insert([user])

    # Query data
    users = await conn.exec(
        select(User)
        .where(User.name == "Alice")
    )

    # Update data
    user.email = "newemail@example.com"
    await conn.update([user])
    ```
    """

    def __init__(self, conn: asyncpg.Connection):
        """
        Initialize a new database connection wrapper.

        :param conn: An asyncpg Connection instance to wrap
        """
        self.conn = conn
        self.obj_to_primary_key: dict[str, str | None] = {}
        self.in_transaction = False

    @asynccontextmanager
    async def transaction(self):
        """
        Context manager for managing database transactions. Ensures that a series of database
        operations are executed atomically.

        ```python {{sticky: True}}
        async with conn.transaction():
            # All operations here are executed in a transaction
            user = User(name="Alice", email="alice@example.com")
            await conn.insert([user])

            post = Post(title="Hello", user_id=user.id)
            await conn.insert([post])

            # If any operation fails, all changes are rolled back
        ```
        """
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
        """
        Execute a query built with QueryBuilder and return the results.

        ```python {{sticky: True}}
        # Select query
        users = await conn.exec(
            select(User)
            .where(User.age >= 18)
            .order_by(User.name)
        )

        # Select with joins and aggregates
        results = await conn.exec(
            select((User.name, func.count(Order.id)))
            .join(Order, Order.user_id == User.id)
            .group_by(User.name)
            .having(func.count(Order.id) > 5)
        )

        # Delete query
        await conn.exec(
            delete(User)
            .where(User.is_active == False)
        )
        ```

        :param query: A QueryBuilder instance representing the query to execute
        :return: For SELECT queries, returns a list of results. For other queries, returns None

        """
        sql_text, variables = query.build()
        LOGGER.debug(f"Executing query: {sql_text} with variables: {variables}")
        values = await self.conn.fetch(sql_text, *variables)

        if query._query_type == "SELECT":
            # Pre-cache the select types for better performance
            select_types = [
                (
                    is_base_table(select_raw),
                    is_column(select_raw),
                    is_function_metadata(select_raw),
                )
                for select_raw in query._select_raw
            ]

            result_all = optimize_exec_casting(values, query._select_raw, select_types)
            return cast(list[T], result_all)

        return None

    async def insert(self, objects: Sequence[TableBase]):
        """
        Insert one or more model instances into the database. If the model has an auto-incrementing
        primary key, it will be populated on the instances after insertion.

        ```python {{sticky: True}}
        # Insert a single object
        user = User(name="Alice", email="alice@example.com")
        await conn.insert([user])
        print(user.id)  # Auto-populated primary key

        # Insert multiple objects
        users = [
            User(name="Bob", email="bob@example.com"),
            User(name="Charlie", email="charlie@example.com")
        ]
        await conn.insert(users)
        ```

        :param objects: A sequence of TableBase instances to insert

        """
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
        This is useful when you want to insert records but update them if they already exist.

        ```python {{sticky: True}}
        # Simple upsert based on email
        users = [
            User(email="alice@example.com", name="Alice"),
            User(email="bob@example.com", name="Bob")
        ]
        await conn.upsert(
            users,
            conflict_fields=(User.email,),
            update_fields=(User.name,)
        )

        # Upsert with returning values
        results = await conn.upsert(
            users,
            conflict_fields=(User.email,),
            update_fields=(User.name,),
            returning_fields=(User.id, User.email)
        )
        for user_id, email in results:
            print(f"Upserted user {email} with ID {user_id}")
        ```

        :param objects: Sequence of TableBase objects to upsert
        :param conflict_fields: Fields to check for conflicts (ON CONFLICT)
        :param update_fields: Fields to update on conflict. If None, updates all non-excluded fields
        :param returning_fields: Fields to return after the operation. If None, returns nothing
        :return: List of tuples containing the returned fields if returning_fields is specified

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
        """
        Update one or more model instances in the database. Only modified attributes will be updated.

        ```python {{sticky: True}}
        # Update a single object
        user = await conn.exec(select(User).where(User.id == 1))
        user.name = "New Name"
        await conn.update([user])

        # Update multiple objects
        users = await conn.exec(select(User).where(User.age < 18))
        for user in users:
            user.is_minor = True
        await conn.update(users)
        ```

        :param objects: A sequence of TableBase instances to update

        """
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
        """
        Delete one or more model instances from the database.

        ```python {{sticky: True}}
        # Delete a single object
        user = await conn.exec(select(User).where(User.id == 1))
        await conn.delete([user])

        # Delete multiple objects
        inactive_users = await conn.exec(
            select(User).where(User.last_login < datetime.now() - timedelta(days=90))
        )
        await conn.delete(inactive_users)
        ```

        :param objects: A sequence of TableBase instances to delete

        """
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
        """
        Refresh one or more model instances from the database, updating their attributes
        with the current database values.

        ```python {{sticky: True}}
        # Refresh a single object
        user = await conn.exec(select(User).where(User.id == 1))
        # ... some time passes, database might have changed
        await conn.refresh([user])  # User now has current database values

        # Refresh multiple objects
        users = await conn.exec(select(User).where(User.department == "Sales"))
        # ... after some time
        await conn.refresh(users)  # All users now have current values
        ```

        :param objects: A sequence of TableBase instances to refresh

        """
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

    async def get(
        self, model: Type[TableType], primary_key_value: Any
    ) -> TableType | None:
        """
        Retrieve a single model instance by its primary key value.

        This method provides a convenient way to fetch a single record from the database using its primary key.
        It automatically constructs and executes a SELECT query with a WHERE clause matching the primary key.

        ```python {{sticky: True}}
        class User(TableBase):
            id: int = Field(primary_key=True)
            name: str
            email: str

        # Fetch a user by ID
        user = await db_connection.get(User, 1)
        if user:
            print(f"Found user: {user.name}")
        else:
            print("User not found")
        ```

        :param model: The model class to query (must be a subclass of TableBase)
        :param primary_key_value: The value of the primary key to look up
        :return: The model instance if found, None if no record matches the primary key
        :raises ValueError: If the model has no primary key defined

        """
        primary_key = self._get_primary_key(model)
        if not primary_key:
            raise ValueError(
                f"Model {model} has no primary key, required to GET with ORM objects"
            )

        query_builder = QueryBuilder()
        query = query_builder.select(model).where(
            getattr(model, primary_key) == primary_key_value
        )
        results = await self.exec(query)
        return results[0] if results else None

    def _aggregate_models_by_table(self, objects: Sequence[TableBase]):
        """
        Group model instances by their table class for batch operations.

        :param objects: Sequence of TableBase instances to group
        :return: Iterator of (model_class, list_of_instances) pairs
        """
        objects_by_class: defaultdict[Type[TableBase], list[TableBase]] = defaultdict(
            list
        )
        for obj in objects:
            objects_by_class[obj.__class__].append(obj)

        return objects_by_class.items()

    def _get_primary_key(self, obj: Type[TableBase]) -> str | None:
        """
        Get the primary key field name for a model class, with caching.

        :param obj: The model class to get the primary key for
        :return: The name of the primary key field, or None if no primary key exists
        """
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
        Context manager that ensures operations are executed within a transaction.
        If no transaction is active, creates a new one for the duration of the context.
        If a transaction is already active, uses the existing transaction.
        """
        if not self.in_transaction:
            async with self.transaction():
                yield
        else:
            yield
