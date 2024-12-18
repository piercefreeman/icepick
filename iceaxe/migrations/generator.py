from collections import defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime
from enum import Enum
from inspect import ismodule
from json import dumps as json_dumps
from time import time
from types import ModuleType
from typing import Any, Callable, Sequence, Type

from pydantic import BaseModel

from iceaxe.migrations.migration import MigrationRevisionBase
from iceaxe.migrations.migrator import Migrator
from iceaxe.schemas.actions import DatabaseActions, DryRunAction, DryRunComment
from iceaxe.schemas.db_memory_serializer import DatabaseMemorySerializer
from iceaxe.schemas.db_stubs import DBObject, DBObjectPointer

MIGRATION_TEMPLATE = """
{header_imports}

class MigrationRevision(MigrationRevisionBase):
    \"""
    Migration auto-generated on {timestamp}.

    Context: {user_message}

    \"""
    up_revision: str = {rev}
    down_revision: str | None = {prev_rev}

    async def up(self, migrator: Migrator):
{up_code}

    async def down(self, migrator: Migrator):
{down_code}
"""


class MigrationGenerator:
    """
    Generates Python migration files for database schema changes.
    This class handles the automatic generation of migration code by comparing
    the current database state with the desired schema defined in code.

    The generator creates both 'up' and 'down' migration methods, allowing for
    bidirectional schema changes. It automatically handles:
    - Table creation and deletion
    - Column additions, modifications, and removals
    - Constraint management
    - Type creation and updates
    - Import tracking for required dependencies

    ```python {{sticky: True}}
    # Generate a new migration
    generator = MigrationGenerator()
    code, revision = await generator.new_migration(
        down_objects=current_db_state,
        up_objects=desired_schema,
        down_revision="previous_migration_id",
        user_message="Add user preferences table"
    )

    # The generated code will look like:
    '''
    class MigrationRevision(MigrationRevisionBase):
        up_revision: str = "20240101120000"
        down_revision: str | None = "previous_migration_id"

        async def up(self, migrator: Migrator):
            await migrator.actor.add_table(...)
            await migrator.actor.add_column(...)

        async def down(self, migrator: Migrator):
            await migrator.actor.drop_table(...)
    '''
    """

    def __init__(self):
        """
        Initialize a new MigrationGenerator instance.
        Sets up the import tracking system and database serializer.
        """
        self.import_tracker: defaultdict[str, set[str]] = defaultdict(set)
        self.serializer = DatabaseMemorySerializer()

    async def new_migration(
        self,
        down_objects_with_dependencies: Sequence[
            tuple[DBObject, Sequence[DBObject | DBObjectPointer]]
        ],
        up_objects_with_dependencies: Sequence[
            tuple[DBObject, Sequence[DBObject | DBObjectPointer]]
        ],
        down_revision: str | None,
        user_message: str | None,
    ) -> tuple[str, str]:
        """
        Generate a new migration file by comparing two database states.

        :param down_objects_with_dependencies: Current database state with object dependencies
        :param up_objects_with_dependencies: Desired database state with object dependencies
        :param down_revision: ID of the previous migration this one builds upon
        :param user_message: Optional description of the migration's purpose
        :return: A tuple of (generated migration code, new revision ID)

        ```python {{sticky: True}}
        # Generate migration for schema change
        generator = MigrationGenerator()
        code, revision = await generator.new_migration(
            down_objects_with_dependencies=[(
                DBTable(table_name="users"),
                []
            )],
            up_objects_with_dependencies=[(
                DBTable(
                    table_name="users",
                    columns=[DBColumn(name="email", type=ColumnType.VARCHAR)]
                ),
                []
            )],
            down_revision="20240101",
            user_message="Add email column to users table"
        )
        ```
        """
        self.import_tracker.clear()
        revision = str(int(time()))

        # Import requirements for every file. We need to explicitly provide the location
        # to the dependencies, since this is a synthetic module and not an actual class where
        # we can track the module.
        self.track_import(Migrator)
        self.track_import(MigrationRevisionBase)

        next_objects = [obj for obj, _ in up_objects_with_dependencies]
        previous_objects = [obj for obj, _ in down_objects_with_dependencies]

        next_objects_ordering = self.serializer.order_db_objects(
            up_objects_with_dependencies
        )
        previous_objects_ordering = self.serializer.order_db_objects(
            down_objects_with_dependencies
        )

        # Convert to their respective DBObjects, with dependencies
        up_actor = DatabaseActions()
        up_actions = await self.serializer.build_actions(
            up_actor,
            previous_objects,
            previous_objects_ordering,
            next_objects,
            next_objects_ordering,
        )
        up_code = self.actions_to_code(up_actions)

        down_actor = DatabaseActions()
        down_actions = await self.serializer.build_actions(
            down_actor,
            next_objects,
            next_objects_ordering,
            previous_objects,
            previous_objects_ordering,
        )
        down_code = self.actions_to_code(down_actions)

        imports: list[str] = []
        for module, classes in self.import_tracker.items():
            if classes:
                classes_list = ", ".join(sorted(classes))
                imports.append(f"from {module} import {classes_list}")

        code = MIGRATION_TEMPLATE.strip().format(
            migrator_import=DatabaseMemorySerializer.__module__,
            rev=json_dumps(revision),
            prev_rev=json_dumps(down_revision) if down_revision else "None",
            up_code=self.indent_code(up_code, 2),
            down_code=self.indent_code(down_code, 2),
            header_imports="\n".join(imports),
            timestamp=datetime.now().isoformat(),
            user_message=user_message or "None",
        )

        return code, revision

    def actions_to_code(self, actions: list[DryRunAction | DryRunComment]) -> list[str]:
        """
        Convert a list of database actions into executable Python code.
        Handles both actual database operations and comments.

        :param actions: List of actions to convert to code
        :return: List of Python code lines

        ```python {{sticky: True}}
        generator = MigrationGenerator()
        code_lines = generator.actions_to_code([
            DryRunAction(
                fn=actor.add_column,
                kwargs={
                    "table_name": "users",
                    "column_name": "email",
                    "explicit_data_type": ColumnType.VARCHAR
                }
            ),
            DryRunComment(
                text="Add email verification field",
                previous_line=False
            )
        ])
        # Results in:
        # ['# Add email verification field',
        #  'await migrator.actor.add_column(table_name="users", column_name="email", explicit_data_type=ColumnType.VARCHAR)']
        ```
        """
        code_lines: list[str] = []

        for action in actions:
            if isinstance(action, DryRunAction):
                # All the actions should be callables attached to the migrator
                migrator_signature = action.fn.__name__

                # Format the kwargs as python native types since the code has to be executable
                kwargs = ", ".join(
                    [f"{k}={self.format_arg(v)}" for k, v in action.kwargs.items()]
                )

                # Format the dependencies
                code_lines.append(
                    f"await migrator.actor.{migrator_signature}({kwargs})"
                )
            elif isinstance(action, DryRunComment):
                if action.previous_line:
                    # Create a comment that's on the same line
                    previous_line = code_lines.pop()
                    new_comment = action.text.replace("\n", " ")
                    code_lines.append(f"{previous_line}  # {new_comment}")
                else:
                    comment_lines = action.text.split("\n")
                    for line in comment_lines:
                        code_lines.append(f"# {line}")
            else:
                raise ValueError(f"Unknown action type: {action}")

        if not code_lines:
            code_lines.append("pass")

        return code_lines

    def format_arg(self, value: Any) -> str:
        """
        Format a Python value as a valid code string, handling proper escaping
        and import tracking for complex types.

        This method supports formatting of:
        - Enums (with automatic import tracking)
        - Basic types (bool, str, int, float)
        - Collections (list, set, frozenset, tuple, dict)
        - Pydantic models and dataclasses
        - None values

        :param value: The value to format as code
        :return: A string representation of the value as valid Python code
        :raises ValueError: If the value type is not supported
        :raises TypeError: If a BaseModel/dataclass value is a class instead of an instance

        ```python {{sticky: True}}
        generator = MigrationGenerator()

        # Format different types of values
        generator.format_arg(SomeEnum.VALUE)  # -> "SomeEnum.VALUE"
        generator.format_arg("hello")         # -> '"hello"'
        generator.format_arg([1, 2, 3])       # -> "[1, 2, 3]"
        generator.format_arg({"a": 1})        # -> '{"a": 1}'
        generator.format_arg(
            UserModel(name="John")
        )                                     # -> 'UserModel(name="John")'
        ```
        """
        if isinstance(value, Enum):
            self.track_import(value.__class__)
            class_name = value.__class__.__name__
            return f"{class_name}.{value.name}"
        elif isinstance(value, bool):
            return "True" if value else "False"
        elif isinstance(value, (str, int, float)):
            # JSON dumps is used here for proper string escaping
            return json_dumps(value)
        elif isinstance(value, list):
            return f"[{', '.join([self.format_arg(v) for v in value])}]"
        elif isinstance(value, frozenset):
            # Sorting values isn't necessary for client code, but useful for test stability over time
            return f"frozenset({{{', '.join([self.format_arg(v) for v in sorted(value)])}}})"
        elif isinstance(value, set):
            return f"{{{', '.join([self.format_arg(v) for v in sorted(value)])}}}"
        elif isinstance(value, tuple):
            tuple_values = f"{', '.join([self.format_arg(v) for v in value])}"
            if len(value) == 1:
                # Trailing comma is necessary for single element tuples
                return f"({tuple_values},)"
            else:
                return f"({tuple_values})"
        elif isinstance(value, dict):
            return (
                "{"
                + ", ".join(
                    [
                        f"{self.format_arg(k)}: {self.format_arg(v)}"
                        for k, v in value.items()
                    ]
                )
                + "}"
            )
        elif isinstance(value, BaseModel) or is_dataclass(value):
            if isinstance(value, BaseModel):
                model_dict = value.model_dump()
            elif is_dataclass(value) and not isinstance(value, type):
                # Currently incorrect typehinting in pyright for isinstance(value, type)
                # Still results in a type[DataclassInstance] possible type. This can remove
                # the following 3 type ignores when fixed.
                # https://github.com/microsoft/pyright/issues/8963
                model_dict = asdict(value)  # type: ignore
            else:
                raise TypeError(
                    "Value must be a BaseModel instance or a dataclass instance."
                )

            self.track_import(value.__class__)  # type: ignore

            code = f"{value.__class__.__name__}("  # type: ignore
            code += ", ".join(
                [
                    f"{k}={self.format_arg(v)}"
                    for k, v in model_dict.items()
                    if v is not None
                ]
            )
            code += ")"
            return code
        elif value is None:
            return "None"
        else:
            raise ValueError(f"Unknown argument type: {value} ({type(value)})")

    def track_import(
        self,
        value: Type[Any] | Callable | ModuleType,
        explicit: str | None = None,
    ):
        """
        Track required imports for the generated migration file.
        Manages the import statements needed for types and functions used in the migration.

        :param value: The class, function, or module to import
        :param explicit: Optional explicit import path override
        :raises ValueError: If explicit import is required for a module but not provided

        ```python {{sticky: True}}
        generator = MigrationGenerator()

        # Track class import
        generator.track_import(UserModel)
        # -> Will add "from app.models import UserModel"

        # Track with explicit path
        generator.track_import(
            some_module,
            explicit="package.module.function"
        )
        # -> Will add "from package.module import function"
        ```
        """
        if ismodule(value):
            # We require an explicit import for modules
            if not explicit:
                raise ValueError("Explicit import required for modules")

        if explicit:
            module, class_name = explicit.rsplit(".", 1)
        else:
            module = value.__module__
            class_name = value.__name__

        self.import_tracker[module].add(class_name)

    def indent_code(self, code: list[str], indent: int) -> str:
        """
        Indent lines of code by a specified number of levels.
        Each level is 4 spaces.

        :param code: List of code lines to indent
        :param indent: Number of indentation levels
        :return: The indented code as a single string

        ```python {{sticky: True}}
        generator = MigrationGenerator()
        code = generator.indent_code(
            ["def example():", "return True"],
            indent=1
        )
        # Results in:
        # "    def example():\n        return True"
        ```
        """
        return "\n".join([f"{' ' * 4 * indent}{line}" for line in code])
