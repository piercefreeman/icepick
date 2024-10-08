from datetime import date, datetime, time, timedelta
from enum import Enum, IntEnum, StrEnum
from inspect import isgenerator
from typing import Any, Generator, Sequence, Type
from uuid import UUID

from graphlib import TopologicalSorter
from pydantic_core import PydanticUndefined

from iceaxe.base import (
    INTERNAL_TABLE_FIELDS,
    DBFieldInfo,
    IndexConstraint,
    TableBase,
    UniqueConstraint,
)
from iceaxe.migrations.actions import (
    CheckConstraint,
    ColumnType,
    ConstraintType,
    DatabaseActions,
    ForeignKeyConstraint,
)
from iceaxe.migrations.db_stubs import (
    DBColumn,
    DBConstraint,
    DBObject,
    DBObjectPointer,
    DBTable,
    DBType,
    DBTypePointer,
)
from iceaxe.migrations.generics import (
    has_null_type,
    is_type_compatible,
    remove_null_type,
)
from iceaxe.postgres import PostgresDateTime, PostgresTime


class DatabaseMemorySerializer:
    """
    Serialize the in-memory database representations into a format that can be
    compared to the database definitions on disk.

    """

    def __init__(self):
        # Construct the directed acyclic graph of the in-memory database objects
        # that indicate what order items should be fulfilled in
        self.db_dag = []

        self.database_handler = DatabaseHandler()

    def delegate(self, tables: list[Type[TableBase]]):
        """
        Find the most specific relevant handler. For instance, if a subclass
        is a registered handler, we should use that instead of the superclass
        If multiple are found we throw, since we can't determine which one to use
        for the resolution.

        """
        yield from self.database_handler.convert(tables)

    def order_db_objects(
        self,
        db_objects: Sequence[tuple[DBObject, Sequence[DBObject | DBObjectPointer]]],
    ):
        """
        Resolve the order that the database objects should be created or modified
        by normalizing pointers/full objects and performing a sort of their defined
        DAG dependencies in the migration graph.

        """
        # First, go through and create a representative object for each of
        # the representation names
        db_objects_by_name: dict[str, DBObject] = {}
        for db_object, _ in db_objects:
            # Only perform this mapping for objects that are not pointers
            if isinstance(db_object, DBObjectPointer):
                continue

            # If the object is already in the dictionary, try to merge the two
            # different values. Otherwise this indicates that there is a conflicting
            # name with a different definition which we don't allow
            if db_object.representation() in db_objects_by_name:
                current_obj = db_objects_by_name[db_object.representation()]
                db_objects_by_name[db_object.representation()] = current_obj.merge(
                    db_object
                )
            else:
                db_objects_by_name[db_object.representation()] = db_object

        # Make sure all the pointers can be resolved by full objects
        # Otherwise we want a verbose error that gives more context
        for _, dependencies in db_objects:
            for dep in dependencies:
                if isinstance(dep, DBObjectPointer):
                    if dep.representation() not in db_objects_by_name:
                        raise ValueError(
                            f"Pointer {dep.representation()} not found in the defined database objects"
                        )

        # Map the potentially different objects to the same object
        graph_edges = {
            db_objects_by_name[obj.representation()]: [
                db_objects_by_name[dep.representation()] for dep in dependencies
            ]
            for obj, dependencies in db_objects
        }

        # Construct the directed acyclic graph
        ts = TopologicalSorter(graph_edges)
        return {obj: i for i, obj in enumerate(ts.static_order())}

    async def build_actions(
        self,
        actor: DatabaseActions,
        previous: list[DBObject],
        previous_ordering: dict[DBObject, int],
        next: list[DBObject],
        next_ordering: dict[DBObject, int],
    ):
        # Arrange each object by their representation so we can determine
        # the state of each
        previous_by_name = {obj.representation(): obj for obj in previous}
        next_by_name = {obj.representation(): obj for obj in next}

        previous_ordering_by_name = {
            obj.representation(): order for obj, order in previous_ordering.items()
        }
        next_ordering_by_name = {
            obj.representation(): order for obj, order in next_ordering.items()
        }

        # Verification that the ordering dictionaries align with the objects
        for ordering, objects in [
            (previous_ordering_by_name, previous_by_name),
            (next_ordering_by_name, next_by_name),
        ]:
            if set(ordering.keys()) != set(objects.keys()):
                unique_keys = (set(ordering.keys()) - set(objects.keys())) | (
                    set(objects.keys()) - set(ordering.keys())
                )
                raise ValueError(
                    f"Ordering dictionary keys must be the same as the objects in the list: {unique_keys}"
                )

        # Sort the objects by the order that they should be created in. Only create one object
        # for each representation value, in case we were passed duplicate objects.
        previous = sorted(
            previous_by_name.values(),
            key=lambda obj: previous_ordering_by_name[obj.representation()],
        )
        next = sorted(
            next_by_name.values(),
            key=lambda obj: next_ordering_by_name[obj.representation()],
        )

        for next_obj in next:
            previous_obj = previous_by_name.get(next_obj.representation())

            if previous_obj is None and next_obj is not None:
                await next_obj.create(actor)
            elif previous_obj is not None and next_obj is not None:
                # Only migrate if they're actually different
                if previous_obj != next_obj:
                    await next_obj.migrate(previous_obj, actor)

        # For all of the items that were in the previous state but not in the
        # next state, we should delete them
        to_delete = [
            previous_obj
            for previous_obj in previous
            if previous_obj.representation() not in next_by_name
        ]
        # We use the reversed representation to destroy objects with more dependencies
        # before the dependencies themselves
        to_delete.reverse()
        for previous_obj in to_delete:
            await previous_obj.destroy(actor)

        return actor.dry_run_actions


# Cast a wide net for all the enum types so str doesn't capture str enums
ALL_ENUM_TYPES = Type[Enum | StrEnum | IntEnum]

PRIMITIVE_TYPES = int | float | str | bool | bytes | UUID
PRIMITIVE_WRAPPER_TYPES = list[PRIMITIVE_TYPES] | PRIMITIVE_TYPES

JSON_WRAPPER_FALLBACK = list[Any] | dict[Any, Any]


class TypeDeclarationResponse(DBObject):
    # Not really a db object, but we need to fulfill the yield contract
    # They'll be filtered out later
    primitive_type: ColumnType | None = None
    custom_type: DBType | None = None
    is_list: bool = False

    def representation(self) -> str:
        raise NotImplementedError()

    def create(self, actor: DatabaseActions):
        raise NotImplementedError()

    def destroy(self, actor: DatabaseActions):
        raise NotImplementedError()

    def migrate(self, previous, actor: DatabaseActions):
        raise NotImplementedError()


NodeYieldType = DBObject | tuple[DBObject, Sequence[DBObject]]


class DatabaseHandler:
    def __init__(self):
        self.python_to_sql = {
            int: ColumnType.INTEGER,
            float: ColumnType.DOUBLE_PRECISION,
            str: ColumnType.VARCHAR,
            bool: ColumnType.BOOLEAN,
            bytes: ColumnType.BYTEA,
            UUID: ColumnType.UUID,
            Any: ColumnType.JSON,
        }

    def convert(self, tables: list[Type[TableBase]]):
        for model in sorted(tables, key=lambda model: model.get_table_name()):
            yield from self.convert_table(model)

    def convert_table(self, table: Type[TableBase]):
        # Handle the table itself
        table_nodes = self._yield_nodes(DBTable(table_name=table.get_table_name()))
        yield from table_nodes

        # Handle the columns
        all_column_nodes: list[tuple[DBObject, list[DBObject]]] = []
        for field_name, field in table.model_fields.items():
            # Only create user-columns
            if field_name in INTERNAL_TABLE_FIELDS:
                continue

            column_nodes = self._yield_nodes(
                self.convert_column(field_name, field, table), dependencies=table_nodes
            )
            yield from column_nodes
            all_column_nodes += column_nodes

        # Primary keys must be handled after the columns are created, since multiple
        # columns can be primary keys but only one constraint can be created
        primary_keys = [
            (key, info) for key, info in table.model_fields.items() if info.primary_key
        ]
        yield from self._yield_nodes(
            self.handle_primary_keys(primary_keys, table), dependencies=all_column_nodes
        )

        if table.table_args != PydanticUndefined:
            for constraint in table.table_args:
                yield from self._yield_nodes(
                    self.handle_multiple_constraints(constraint, table),
                    dependencies=all_column_nodes,
                )

    def convert_column(self, key: str, info: DBFieldInfo, table: Type[TableBase]):
        if info.annotation is None:
            raise ValueError(f"Annotation must be provided for {table.__name__}.{key}")

        is_nullable = has_null_type(info.annotation)

        # If we need to create enums or other db-backed types, we need to do that before
        # the column itself
        db_annotation = self.handle_column_type(key, info, table)
        column_type: DBTypePointer | ColumnType
        column_dependencies: list[tuple[DBObject, list[DBObject]]] = []
        if db_annotation.custom_type:
            dependencies = self._yield_nodes(db_annotation.custom_type)
            column_dependencies += dependencies
            yield from dependencies

            column_type = DBTypePointer(name=db_annotation.custom_type.name)
        elif db_annotation.primitive_type:
            column_type = db_annotation.primitive_type
        else:
            raise ValueError("Column type must be provided")

        # We need to create the column itself once types have been created
        yield from self._yield_nodes(
            DBColumn(
                table_name=table.get_table_name(),
                column_name=key,
                column_type=column_type,
                column_is_list=db_annotation.is_list,
                nullable=is_nullable,
            ),
            dependencies=column_dependencies,
        )

    def handle_column_type(self, key: str, info: DBFieldInfo, table: Type[TableBase]):
        if info.annotation is None:
            raise ValueError(f"Annotation must be provided for {table.__name__}.{key}")

        annotation = remove_null_type(info.annotation)

        if is_type_compatible(annotation, ALL_ENUM_TYPES):
            return TypeDeclarationResponse(
                custom_type=DBType(
                    name=annotation.__name__.lower(),  # type: ignore
                    values=frozenset([value.name for value in annotation]),  # type: ignore
                    reference_columns=frozenset({(table.get_table_name(), key)}),
                ),
            )
        elif is_type_compatible(annotation, PRIMITIVE_WRAPPER_TYPES):
            for primitive, json_type in self.python_to_sql.items():
                if annotation == primitive or annotation == list[primitive]:  # type: ignore
                    return TypeDeclarationResponse(
                        primitive_type=json_type,
                        is_list=(annotation == list[primitive]),  # type: ignore
                    )
        elif is_type_compatible(annotation, datetime):
            if isinstance(info.postgres_config, PostgresDateTime):
                return TypeDeclarationResponse(
                    primitive_type=(
                        ColumnType.TIMESTAMP_WITH_TIME_ZONE
                        if info.postgres_config.timezone
                        else ColumnType.TIMESTAMP
                    )
                )
            # Assume no timezone if not specified
            return TypeDeclarationResponse(
                primitive_type=ColumnType.TIMESTAMP,
            )
        elif is_type_compatible(annotation, date):
            return TypeDeclarationResponse(
                primitive_type=ColumnType.DATE,
            )
        elif is_type_compatible(annotation, time):
            if isinstance(info.postgres_config, PostgresTime):
                return TypeDeclarationResponse(
                    primitive_type=(
                        ColumnType.TIME_WITH_TIME_ZONE
                        if info.postgres_config.timezone
                        else ColumnType.TIME
                    ),
                )
            return TypeDeclarationResponse(
                primitive_type=ColumnType.TIME,
            )
        elif is_type_compatible(annotation, timedelta):
            return TypeDeclarationResponse(
                primitive_type=ColumnType.INTERVAL,
            )
        elif is_type_compatible(annotation, JSON_WRAPPER_FALLBACK):
            return TypeDeclarationResponse(
                primitive_type=ColumnType.JSON,
            )

        raise ValueError(f"Unsupported column type: {annotation}")

    def handle_single_constraints(
        self, key: str, info: DBFieldInfo, table: Type[TableBase]
    ):
        def _build_constraint(
            constraint_type: ConstraintType,
            *,
            foreign_key_constraint: ForeignKeyConstraint | None = None,
            check_constraint: CheckConstraint | None = None,
        ):
            return DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=constraint_type,
                columns=frozenset([key]),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    [key],
                    constraint_type,
                ),
                foreign_key_constraint=foreign_key_constraint,
                check_constraint=check_constraint,
            )

        if info.unique:
            yield from self._yield_nodes(_build_constraint(ConstraintType.UNIQUE))

        if info.foreign_key:
            target_table, target_column = info.foreign_key.rsplit(".", 1)
            yield from self._yield_nodes(
                _build_constraint(
                    ConstraintType.FOREIGN_KEY,
                    foreign_key_constraint=ForeignKeyConstraint(
                        target_table=target_table,
                        target_columns=frozenset({target_column}),
                    ),
                )
            )

        if info.index:
            yield from self._yield_nodes(_build_constraint(ConstraintType.INDEX))

        if info.check_expression:
            yield from self._yield_nodes(
                _build_constraint(
                    ConstraintType.CHECK,
                    check_constraint=CheckConstraint(
                        check_condition=info.check_expression,
                    ),
                )
            )

    def handle_multiple_constraints(
        self, constraint: UniqueConstraint | IndexConstraint, table: Type[TableBase]
    ):
        columns: list[str]
        constraint_type: ConstraintType

        if isinstance(constraint, UniqueConstraint):
            constraint_type = ConstraintType.UNIQUE
            columns = constraint.columns
        elif isinstance(constraint, IndexConstraint):
            constraint_type = ConstraintType.INDEX
            columns = constraint.columns
        else:
            raise ValueError(f"Unsupported constraint type: {constraint}")

        yield from self._yield_nodes(
            DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=constraint_type,
                columns=frozenset(columns),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    columns,
                    constraint_type,
                ),
            )
        )

    def handle_primary_keys(
        self, keys: list[tuple[str, DBFieldInfo]], table: Type[TableBase]
    ):
        if not keys:
            return

        columns = [key for key, _ in keys]
        yield from self._yield_nodes(
            DBConstraint(
                table_name=table.get_table_name(),
                constraint_type=ConstraintType.PRIMARY_KEY,
                columns=frozenset(columns),
                constraint_name=DBConstraint.new_constraint_name(
                    table.get_table_name(),
                    columns,
                    ConstraintType.PRIMARY_KEY,
                ),
            )
        )

    def _yield_nodes(
        self,
        child: NodeYieldType | Generator[NodeYieldType, None, None],
        dependencies: Sequence[NodeYieldType] | None = None,
    ) -> list[tuple[DBObject, list[DBObject]]]:
        """
        Given potentially nested nodes, merge them into a flat list of nodes
        with dependencies.

        """

        def _format_dependencies(dependencies: Sequence[NodeYieldType]):
            all_dependencies: list[DBObject] = []

            for value in dependencies:
                if isinstance(value, DBObject):
                    all_dependencies.append(value)
                elif isinstance(value, tuple):
                    node, existing_dependencies = value
                    all_dependencies.append(node)
                    all_dependencies += existing_dependencies
                else:
                    raise ValueError(f"Unsupported dependency type: {value}")

            # Sorting isn't required for the DAG but is useful for testing determinism
            return sorted(
                set(all_dependencies),
                key=lambda x: x.representation(),
            )

        results: list[tuple[DBObject, list[DBObject]]] = []

        if isinstance(child, DBObject):
            # No dependencies list is provided, let's yield a new one
            results.append((child, _format_dependencies(dependencies or [])))
        elif isinstance(child, tuple):
            node, existing_dependencies = child

            all_dependencies: list[NodeYieldType] = []
            all_dependencies += dependencies or []
            all_dependencies += existing_dependencies

            results.append(
                (
                    node,
                    _format_dependencies(all_dependencies),
                )
            )
        elif isgenerator(child):
            for node in child:
                results += self._yield_nodes(node, dependencies)
        else:
            raise ValueError(f"Unsupported node type: {child}")

        return results
