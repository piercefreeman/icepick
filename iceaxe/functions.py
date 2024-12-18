from __future__ import annotations

from typing import Any, TypeVar, cast

from iceaxe.base import (
    DBFieldClassDefinition,
)
from iceaxe.comparison import ComparisonBase
from iceaxe.queries_str import QueryLiteral
from iceaxe.typing import is_column, is_function_metadata

T = TypeVar("T")


class FunctionMetadata(ComparisonBase):
    """
    Represents metadata for SQL aggregate functions and other SQL function operations.
    This class bridges the gap between Python function calls and their SQL representations,
    maintaining type information and original field references.

    ```python {{sticky: True}}
    # Internal representation of function calls:
    metadata = FunctionMetadata(
        literal=QueryLiteral("count(users.id)"),
        original_field=User.id,
        local_name="user_count"
    )
    # Used in query: SELECT count(users.id) AS user_count
    ```
    """

    literal: QueryLiteral
    """
    The SQL representation of the function call
    """

    original_field: DBFieldClassDefinition
    """
    The database field this function operates on
    """

    local_name: str | None = None
    """
    Optional alias for the function result in the query
    """

    def __init__(
        self,
        literal: QueryLiteral,
        original_field: DBFieldClassDefinition,
        local_name: str | None = None,
    ):
        self.literal = literal
        self.original_field = original_field
        self.local_name = local_name

    def to_query(self):
        """
        Converts the function metadata to its SQL representation.

        :return: A tuple of the SQL literal and an empty list of variables
        """
        return self.literal, []


class FunctionBuilder:
    """
    Builder class for SQL aggregate functions and other SQL operations.
    Provides a Pythonic interface for creating SQL function calls with proper type hints.

    This class is typically accessed through the global `func` instance:
    ```python {{sticky: True}}
    from iceaxe import func

    # In a query:
    query = select((
        User.name,
        func.count(User.id),
        func.max(User.age)
    ))
    ```
    """

    def count(self, field: Any) -> int:
        """
        Creates a COUNT aggregate function call.

        :param field: The field to count. Can be a column or another function result
        :return: A function metadata object that resolves to an integer count

        ```python {{sticky: True}}
        # Count all users
        total = await conn.execute(select(func.count(User.id)))

        # Count distinct values
        unique = await conn.execute(
            select(func.count(func.distinct(User.status)))
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"count({metadata.literal})")
        return cast(int, metadata)

    def distinct(self, field: T) -> T:
        """
        Creates a DISTINCT function call that removes duplicate values.

        :param field: The field to get distinct values from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get distinct status values
        statuses = await conn.execute(select(func.distinct(User.status)))

        # Count distinct values
        unique_count = await conn.execute(
            select(func.count(func.distinct(User.status)))
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"distinct {metadata.literal}")
        return cast(T, metadata)

    def sum(self, field: T) -> T:
        """
        Creates a SUM aggregate function call.

        :param field: The numeric field to sum
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get total of all salaries
        total = await conn.execute(select(func.sum(Employee.salary)))

        # Sum with grouping
        by_dept = await conn.execute(
            select((Department.name, func.sum(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"sum({metadata.literal})")
        return cast(T, metadata)

    def avg(self, field: T) -> T:
        """
        Creates an AVG aggregate function call.

        :param field: The numeric field to average
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get average age of all users
        avg_age = await conn.execute(select(func.avg(User.age)))

        # Average with grouping
        by_dept = await conn.execute(
            select((Department.name, func.avg(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"avg({metadata.literal})")
        return cast(T, metadata)

    def max(self, field: T) -> T:
        """
        Creates a MAX aggregate function call.

        :param field: The field to get the maximum value from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get highest salary
        highest = await conn.execute(select(func.max(Employee.salary)))

        # Max with grouping
        by_dept = await conn.execute(
            select((Department.name, func.max(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"max({metadata.literal})")
        return cast(T, metadata)

    def min(self, field: T) -> T:
        """
        Creates a MIN aggregate function call.

        :param field: The field to get the minimum value from
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get lowest salary
        lowest = await conn.execute(select(func.min(Employee.salary)))

        # Min with grouping
        by_dept = await conn.execute(
            select((Department.name, func.min(Employee.salary)))
            .group_by(Department.name)
        )
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"min({metadata.literal})")
        return cast(T, metadata)

    def _column_to_metadata(self, field: Any) -> FunctionMetadata:
        """
        Internal helper method to convert a field to FunctionMetadata.
        Handles both raw columns and nested function calls.

        :param field: The field to convert
        :return: A FunctionMetadata instance
        :raises ValueError: If the field cannot be converted to a column
        """
        if is_function_metadata(field):
            return field
        elif is_column(field):
            return FunctionMetadata(literal=field.to_query()[0], original_field=field)
        else:
            raise ValueError(
                f"Unable to cast this type to a column: {field} ({type(field)})"
            )


func = FunctionBuilder()
