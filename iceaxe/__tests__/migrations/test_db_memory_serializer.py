from enum import Enum
from unittest.mock import ANY
from uuid import UUID

import pytest

from iceaxe import Field, TableBase
from iceaxe.migrations.actions import (
    ColumnType,
    ConstraintType,
    DatabaseActions,
    DryRunAction,
    DryRunComment,
)
from iceaxe.migrations.db_memory_serializer import DatabaseMemorySerializer
from iceaxe.migrations.db_stubs import (
    DBColumn,
    DBConstraint,
    DBTable,
    DBType,
    DBTypePointer,
)


@pytest.mark.asyncio
async def test_from_scratch_migration():
    """
    Test a migration from scratch.

    """

    class OldValues(Enum):
        A = "A"

    class ModelA(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunComment(
            text="\n" "NEW TABLE: modela\n",
        ),
        DryRunAction(
            fn=actor.add_table,
            kwargs={
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "table_name": "modela",
                "column_name": "id",
            },
        ),
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "oldvalues",
                "values": [
                    "A",
                ],
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "was_nullable",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "animal",
                "custom_data_type": "oldvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "table_name": "modela",
                "column_name": "animal",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": [
                    "id",
                ],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "modela_pkey",
                "table_name": "modela",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_diff_migration():
    """
    Test the diff migration between two schemas.

    """

    class OldValues(Enum):
        A = "A"

    class NewValues(Enum):
        A = "A"
        B = "B"

    class ModelA(TableBase):
        id: int = Field(primary_key=True)
        animal: OldValues
        was_nullable: str | None

    class ModelANew(TableBase):
        table_name = "modela"
        id: int = Field(primary_key=True)
        name: str
        animal: NewValues
        was_nullable: str

    actor = DatabaseActions()
    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([ModelA]))
    db_objects_previous = [obj for obj, _ in db_objects]
    previous_ordering = migrator.order_db_objects(db_objects)

    db_objects_new = list(migrator.delegate([ModelANew]))
    db_objects_next = [obj for obj, _ in db_objects_new]
    next_ordering = migrator.order_db_objects(db_objects_new)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, db_objects_previous, previous_ordering, db_objects_next, next_ordering
    )
    assert actions == [
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "name",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "table_name": "modela",
                "column_name": "name",
            },
        ),
        DryRunAction(
            fn=actor.add_type,
            kwargs={
                "type_name": "newvalues",
                "values": [
                    "A",
                    "B",
                ],
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={
                "column_name": "was_nullable",
                "table_name": "modela",
            },
        ),
        DryRunComment(
            text=ANY,
        ),
        DryRunAction(
            fn=actor.modify_column_type,
            kwargs={
                "column_name": "animal",
                "custom_data_type": "newvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "modela",
            },
        ),
        DryRunAction(
            fn=actor.drop_type,
            kwargs={
                "type_name": "oldvalues",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_duplicate_enum_migration():
    """
    Test that the shared reference to an enum across multiple tables results in only
    one migration action to define the type.

    """

    class EnumValues(Enum):
        A = "A"
        B = "B"

    class Model1(TableBase):
        id: int = Field(primary_key=True)
        value: EnumValues

    class Model2(TableBase):
        id: int = Field(primary_key=True)
        value: EnumValues

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([Model1, Model2]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunComment(text="\nNEW TABLE: model1\n"),
        DryRunAction(fn=actor.add_table, kwargs={"table_name": "model1"}),
        DryRunComment(text="\nNEW TABLE: model2\n"),
        DryRunAction(fn=actor.add_table, kwargs={"table_name": "model2"}),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null, kwargs={"column_name": "id", "table_name": "model1"}
        ),
        DryRunAction(
            fn=actor.add_type, kwargs={"type_name": "enumvalues", "values": ["A", "B"]}
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null, kwargs={"column_name": "id", "table_name": "model2"}
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": "enumvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value", "table_name": "model1"},
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": "enumvalues",
                "explicit_data_is_list": False,
                "explicit_data_type": None,
                "table_name": "model2",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value", "table_name": "model2"},
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": ["id"],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model1_pkey",
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": ["id"],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model2_pkey",
                "table_name": "model2",
            },
        ),
    ]


@pytest.mark.asyncio
async def test_required_db_default():
    """
    Even if we have a default value in Python, we should still force the content
    to have a value at the db level.

    """

    class Model1(TableBase):
        id: int = Field(primary_key=True)
        value: str = "ABC"
        value2: str = Field(default="ABC")

    migrator = DatabaseMemorySerializer()

    db_objects = list(migrator.delegate([Model1]))
    next_ordering = migrator.order_db_objects(db_objects)

    actor = DatabaseActions()
    actions = await migrator.build_actions(
        actor, [], {}, [obj for obj, _ in db_objects], next_ordering
    )

    assert actions == [
        DryRunComment(text="\nNEW TABLE: model1\n"),
        DryRunAction(fn=actor.add_table, kwargs={"table_name": "model1"}),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "id",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.INTEGER,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null, kwargs={"column_name": "id", "table_name": "model1"}
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value", "table_name": "model1"},
        ),
        DryRunAction(
            fn=actor.add_column,
            kwargs={
                "column_name": "value2",
                "custom_data_type": None,
                "explicit_data_is_list": False,
                "explicit_data_type": ColumnType.VARCHAR,
                "table_name": "model1",
            },
        ),
        DryRunAction(
            fn=actor.add_not_null,
            kwargs={"column_name": "value2", "table_name": "model1"},
        ),
        DryRunAction(
            fn=actor.add_constraint,
            kwargs={
                "columns": ["id"],
                "constraint": ConstraintType.PRIMARY_KEY,
                "constraint_args": None,
                "constraint_name": "model1_pkey",
                "table_name": "model1",
            },
        ),
    ]


def test_multiple_primary_keys(clear_all_database_objects):
    """
    Support models defined with multiple primary keys. This should
    result in a composite constraint, which has different handling internally
    than most other field-constraints that are isolated to the field itself.

    """

    class ExampleModel(TableBase):
        value_a: UUID = Field(primary_key=True)
        value_b: UUID = Field(primary_key=True)

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExampleModel]))
    assert db_objects == [
        (
            DBTable(table_name="examplemodel"),
            [],
        ),
        (
            DBColumn(
                table_name="examplemodel",
                column_name="value_a",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel"),
            ],
        ),
        (
            DBColumn(
                table_name="examplemodel",
                column_name="value_b",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [
                DBTable(table_name="examplemodel"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel",
                constraint_name="examplemodel_pkey",
                columns=frozenset({"value_a", "value_b"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
            ),
            [
                DBTable(table_name="examplemodel"),
                DBColumn(
                    table_name="examplemodel",
                    column_name="value_a",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel",
                    column_name="value_b",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]


def test_enum_column_assignment(clear_all_database_objects):
    """
    Enum values will just yield the current column that they are assigned to even if they
    are assigned to multiple columns. It's up to the full memory serializer to combine them
    so we can properly track how we can migrate existing enum/column pairs to the
    new values.

    """

    class CommonEnum(Enum):
        A = "a"
        B = "b"

    class ExampleModel1(TableBase):
        id: UUID = Field(primary_key=True)
        value: CommonEnum

    class ExampleModel2(TableBase):
        id: UUID = Field(primary_key=True)
        value: CommonEnum

    migrator = DatabaseMemorySerializer()
    db_objects = list(migrator.delegate([ExampleModel1, ExampleModel2]))
    assert db_objects == [
        (DBTable(table_name="examplemodel1"), []),
        (
            DBColumn(
                table_name="examplemodel1",
                column_name="id",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [DBTable(table_name="examplemodel1")],
        ),
        (
            DBType(
                name="commonenum",
                values=frozenset({"B", "A"}),
                # This is the important part where we track the reference columns
                reference_columns=frozenset({("examplemodel1", "value")}),
            ),
            [DBTable(table_name="examplemodel1")],
        ),
        (
            DBColumn(
                table_name="examplemodel1",
                column_name="value",
                column_type=DBTypePointer(name="commonenum"),
                column_is_list=False,
                nullable=False,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"B", "A"}),
                    reference_columns=frozenset({("examplemodel1", "value")}),
                ),
                DBTable(table_name="examplemodel1"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel1",
                constraint_name="examplemodel1_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"A", "B"}),
                    reference_columns=frozenset({("examplemodel1", "value")}),
                ),
                DBTable(table_name="examplemodel1"),
                DBColumn(
                    table_name="examplemodel1",
                    column_name="id",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel1",
                    column_name="value",
                    column_type=DBTypePointer(name="commonenum"),
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
        (DBTable(table_name="examplemodel2"), []),
        (
            DBColumn(
                table_name="examplemodel2",
                column_name="id",
                column_type=ColumnType.UUID,
                column_is_list=False,
                nullable=False,
            ),
            [DBTable(table_name="examplemodel2")],
        ),
        (
            DBType(
                name="commonenum",
                values=frozenset({"B", "A"}),
                # This is the important part where we track the reference columns
                reference_columns=frozenset({("examplemodel2", "value")}),
            ),
            [DBTable(table_name="examplemodel2")],
        ),
        (
            DBColumn(
                table_name="examplemodel2",
                column_name="value",
                column_type=DBTypePointer(name="commonenum"),
                column_is_list=False,
                nullable=False,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"B", "A"}),
                    reference_columns=frozenset({("examplemodel2", "value")}),
                ),
                DBTable(table_name="examplemodel2"),
            ],
        ),
        (
            DBConstraint(
                table_name="examplemodel2",
                constraint_name="examplemodel2_pkey",
                columns=frozenset({"id"}),
                constraint_type=ConstraintType.PRIMARY_KEY,
                foreign_key_constraint=None,
                check_constraint=None,
            ),
            [
                DBType(
                    name="commonenum",
                    values=frozenset({"B", "A"}),
                    reference_columns=frozenset({("examplemodel2", "value")}),
                ),
                DBTable(table_name="examplemodel2"),
                DBColumn(
                    table_name="examplemodel2",
                    column_name="id",
                    column_type=ColumnType.UUID,
                    column_is_list=False,
                    nullable=False,
                ),
                DBColumn(
                    table_name="examplemodel2",
                    column_name="value",
                    column_type=DBTypePointer(name="commonenum"),
                    column_is_list=False,
                    nullable=False,
                ),
            ],
        ),
    ]
