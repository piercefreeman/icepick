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

    def abs(self, field: T) -> T:
        """
        Creates an ABS function call to get the absolute value.

        :param field: The numeric field to get the absolute value of
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get absolute value of balance
        abs_balance = await conn.execute(select(func.abs(Account.balance)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"abs({metadata.literal})")
        return cast(T, metadata)

    def date_trunc(self, precision: str, field: T) -> T:
        """
        Truncates a timestamp or interval value to specified precision.

        :param precision: The precision to truncate to ('microseconds', 'milliseconds', 'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year', 'decade', 'century', 'millennium')
        :param field: The timestamp or interval field to truncate
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Truncate timestamp to month
        monthly = await conn.execute(select(func.date_trunc('month', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"date_trunc('{precision}', {metadata.literal})")
        return cast(T, metadata)

    def date_part(self, field: str, source: T) -> int:
        """
        Extracts a subfield from a date/time value.

        :param field: The subfield to extract ('century', 'day', 'decade', 'dow', 'doy', 'epoch', 'hour', 'isodow', 'isoyear', 'microseconds', 'millennium', 'milliseconds', 'minute', 'month', 'quarter', 'second', 'timezone', 'timezone_hour', 'timezone_minute', 'week', 'year')
        :param source: The date/time field to extract from
        :return: A function metadata object that resolves to an integer

        ```python {{sticky: True}}
        # Get month from timestamp
        month = await conn.execute(select(func.date_part('month', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(source)
        metadata.literal = QueryLiteral(f"date_part('{field}', {metadata.literal})")
        return cast(int, metadata)

    def extract(self, field: str, source: T) -> int:
        """
        Extracts a subfield from a date/time value using SQL standard syntax.

        :param field: The subfield to extract ('century', 'day', 'decade', 'dow', 'doy', 'epoch', 'hour', 'isodow', 'isoyear', 'microseconds', 'millennium', 'milliseconds', 'minute', 'month', 'quarter', 'second', 'timezone', 'timezone_hour', 'timezone_minute', 'week', 'year')
        :param source: The date/time field to extract from
        :return: A function metadata object that resolves to an integer

        ```python {{sticky: True}}
        # Get year from timestamp
        year = await conn.execute(select(func.extract('year', User.created_at)))
        ```
        """
        metadata = self._column_to_metadata(source)
        metadata.literal = QueryLiteral(f"extract({field} from {metadata.literal})")
        return cast(int, metadata)

    def age(self, timestamp: T, reference: T | None = None) -> T:
        """
        Calculates the difference between two timestamps.
        If reference is not provided, current_date is used.

        :param timestamp: The timestamp to calculate age from
        :param reference: Optional reference timestamp (defaults to current_date)
        :return: A function metadata object preserving the input type

        ```python {{sticky: True}}
        # Get age of a timestamp
        age = await conn.execute(select(func.age(User.birth_date)))

        # Get age between two timestamps
        age_diff = await conn.execute(select(func.age(Event.end_time, Event.start_time)))
        ```
        """
        metadata = self._column_to_metadata(timestamp)
        if reference is not None:
            ref_metadata = self._column_to_metadata(reference)
            metadata.literal = QueryLiteral(f"age({metadata.literal}, {ref_metadata.literal})")
        else:
            metadata.literal = QueryLiteral(f"age({metadata.literal})")
        return cast(T, metadata)

    def current_date(self) -> T:
        """
        Returns the current date.

        :return: A function metadata object that resolves to a date

        ```python {{sticky: True}}
        # Get current date
        today = await conn.execute(select(func.current_date()))
        ```
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("current_date"),
            original_field=None,  # type: ignore
        )
        return cast(T, metadata)

    def current_time(self) -> T:
        """
        Returns the current time with time zone.

        :return: A function metadata object that resolves to a time with time zone

        ```python {{sticky: True}}
        # Get current time
        now = await conn.execute(select(func.current_time()))
        ```
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("current_time"),
            original_field=None,  # type: ignore
        )
        return cast(T, metadata)

    def current_timestamp(self) -> T:
        """
        Returns the current timestamp with time zone.

        :return: A function metadata object that resolves to a timestamp with time zone

        ```python {{sticky: True}}
        # Get current timestamp
        now = await conn.execute(select(func.current_timestamp()))
        ```
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("current_timestamp"),
            original_field=None,  # type: ignore
        )
        return cast(T, metadata)

    def date(self, field: T) -> T:
        """
        Converts a timestamp to a date by dropping the time component.

        :param field: The timestamp field to convert
        :return: A function metadata object that resolves to a date

        ```python {{sticky: True}}
        # Get just the date part
        event_date = await conn.execute(select(func.date(Event.timestamp)))
        ```
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"date({metadata.literal})")
        return cast(T, metadata)

    def make_date(self, year: T, month: T, day: T) -> T:
        """
        Creates a date from year, month, and day values.

        :param year: The year value
        :param month: The month value (1-12)
        :param day: The day value (1-31)
        :return: A function metadata object that resolves to a date

        ```python {{sticky: True}}
        # Create a date from components
        date = await conn.execute(select(func.make_date(2023, 12, 25)))
        ```
        """
        year_meta = self._column_to_metadata(year)
        month_meta = self._column_to_metadata(month)
        day_meta = self._column_to_metadata(day)
        metadata = FunctionMetadata(
            literal=QueryLiteral(f"make_date({year_meta.literal}, {month_meta.literal}, {day_meta.literal})"),
            original_field=year_meta.original_field,
        )
        return cast(T, metadata)

    def make_time(self, hour: T, min: T, sec: T) -> T:
        """
        Creates a time from hour, minute, and second values.

        :param hour: The hour value (0-23)
        :param min: The minute value (0-59)
        :param sec: The second value (0-59.999999)
        :return: A function metadata object that resolves to a time

        ```python {{sticky: True}}
        # Create a time from components
        time = await conn.execute(select(func.make_time(14, 30, 0)))
        ```
        """
        hour_meta = self._column_to_metadata(hour)
        min_meta = self._column_to_metadata(min)
        sec_meta = self._column_to_metadata(sec)
        metadata = FunctionMetadata(
            literal=QueryLiteral(f"make_time({hour_meta.literal}, {min_meta.literal}, {sec_meta.literal})"),
            original_field=hour_meta.original_field,
        )
        return cast(T, metadata)

    def make_timestamp(self, year: T, month: T, day: T, hour: T, min: T, sec: T) -> T:
        """
        Creates a timestamp from year, month, day, hour, minute, and second values.

        :param year: The year value
        :param month: The month value (1-12)
        :param day: The day value (1-31)
        :param hour: The hour value (0-23)
        :param min: The minute value (0-59)
        :param sec: The second value (0-59.999999)
        :return: A function metadata object that resolves to a timestamp

        ```python {{sticky: True}}
        # Create a timestamp from components
        ts = await conn.execute(select(func.make_timestamp(2023, 12, 25, 14, 30, 0)))
        ```
        """
        year_meta = self._column_to_metadata(year)
        month_meta = self._column_to_metadata(month)
        day_meta = self._column_to_metadata(day)
        hour_meta = self._column_to_metadata(hour)
        min_meta = self._column_to_metadata(min)
        sec_meta = self._column_to_metadata(sec)
        metadata = FunctionMetadata(
            literal=QueryLiteral(
                f"make_timestamp({year_meta.literal}, {month_meta.literal}, {day_meta.literal}, "
                f"{hour_meta.literal}, {min_meta.literal}, {sec_meta.literal})"
            ),
            original_field=year_meta.original_field,
        )
        return cast(T, metadata)

    def make_interval(self, years: T | None = None, months: T | None = None, weeks: T | None = None,
                     days: T | None = None, hours: T | None = None, mins: T | None = None,
                     secs: T | None = None) -> T:
        """
        Creates an interval from various time unit values.

        :param years: Number of years
        :param months: Number of months
        :param weeks: Number of weeks
        :param days: Number of days
        :param hours: Number of hours
        :param mins: Number of minutes
        :param secs: Number of seconds
        :return: A function metadata object that resolves to an interval

        ```python {{sticky: True}}
        # Create an interval
        interval = await conn.execute(
            select(func.make_interval(years=1, months=6, days=15))
        )
        ```
        """
        parts = []
        if years is not None:
            years_meta = self._column_to_metadata(years)
            parts.append(f"years => {years_meta.literal}")
            original_field = years_meta.original_field
        if months is not None:
            months_meta = self._column_to_metadata(months)
            parts.append(f"months => {months_meta.literal}")
            original_field = months_meta.original_field
        if weeks is not None:
            weeks_meta = self._column_to_metadata(weeks)
            parts.append(f"weeks => {weeks_meta.literal}")
            original_field = weeks_meta.original_field
        if days is not None:
            days_meta = self._column_to_metadata(days)
            parts.append(f"days => {days_meta.literal}")
            original_field = days_meta.original_field
        if hours is not None:
            hours_meta = self._column_to_metadata(hours)
            parts.append(f"hours => {hours_meta.literal}")
            original_field = hours_meta.original_field
        if mins is not None:
            mins_meta = self._column_to_metadata(mins)
            parts.append(f"mins => {mins_meta.literal}")
            original_field = mins_meta.original_field
        if secs is not None:
            secs_meta = self._column_to_metadata(secs)
            parts.append(f"secs => {secs_meta.literal}")
            original_field = secs_meta.original_field

        if not parts:
            raise ValueError("At least one interval component must be specified")

        metadata = FunctionMetadata(
            literal=QueryLiteral(f"make_interval({', '.join(parts)})"),
            original_field=original_field,  # type: ignore
        )
        return cast(T, metadata)

    # String Functions
    def lower(self, field: T) -> T:
        """
        Converts string to lowercase.

        :param field: The string field to convert
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"lower({metadata.literal})")
        return cast(T, metadata)

    def upper(self, field: T) -> T:
        """
        Converts string to uppercase.

        :param field: The string field to convert
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"upper({metadata.literal})")
        return cast(T, metadata)

    def length(self, field: T) -> int:
        """
        Returns length of string.

        :param field: The string field to measure
        :return: A function metadata object that resolves to an integer
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"length({metadata.literal})")
        return cast(int, metadata)

    def trim(self, field: T) -> T:
        """
        Removes whitespace from both ends of string.

        :param field: The string field to trim
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"trim({metadata.literal})")
        return cast(T, metadata)

    def substring(self, field: T, start: int, length: int) -> T:
        """
        Extracts substring.

        :param field: The string field to extract from
        :param start: Starting position (1-based)
        :param length: Number of characters to extract
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"substring({metadata.literal} from {start} for {length})")
        return cast(T, metadata)

    # Mathematical Functions
    def round(self, field: T) -> T:
        """
        Rounds to nearest integer.

        :param field: The numeric field to round
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"round({metadata.literal})")
        return cast(T, metadata)

    def ceil(self, field: T) -> T:
        """
        Rounds up to nearest integer.

        :param field: The numeric field to round up
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"ceil({metadata.literal})")
        return cast(T, metadata)

    def floor(self, field: T) -> T:
        """
        Rounds down to nearest integer.

        :param field: The numeric field to round down
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"floor({metadata.literal})")
        return cast(T, metadata)

    def power(self, field: T, exponent: int | float) -> T:
        """
        Raises a number to the specified power.

        :param field: The numeric field to raise
        :param exponent: The power to raise to
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"power({metadata.literal}, {exponent})")
        return cast(T, metadata)

    def sqrt(self, field: T) -> T:
        """
        Calculates square root.

        :param field: The numeric field to calculate square root of
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"sqrt({metadata.literal})")
        return cast(T, metadata)

    # Aggregate Functions
    def array_agg(self, field: T) -> list[T]:
        """
        Collects values into an array.

        :param field: The field to aggregate
        :return: A function metadata object that resolves to a list
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"array_agg({metadata.literal})")
        return cast(list[T], metadata)

    def string_agg(self, field: T, delimiter: str) -> str:
        """
        Concatenates values with delimiter.

        :param field: The field to aggregate
        :param delimiter: The delimiter to use between values
        :return: A function metadata object that resolves to a string
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"string_agg({metadata.literal}, '{delimiter}')")
        return cast(str, metadata)

    # Window Functions
    def row_number(self) -> int:
        """
        Returns the row number within the current partition.

        :return: A function metadata object that resolves to an integer
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("row_number()"),
            original_field=None,  # type: ignore
        )
        return cast(int, metadata)

    def rank(self) -> int:
        """
        Returns the rank with gaps.

        :return: A function metadata object that resolves to an integer
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("rank()"),
            original_field=None,  # type: ignore
        )
        return cast(int, metadata)

    def dense_rank(self) -> int:
        """
        Returns the rank without gaps.

        :return: A function metadata object that resolves to an integer
        """
        metadata = FunctionMetadata(
            literal=QueryLiteral("dense_rank()"),
            original_field=None,  # type: ignore
        )
        return cast(int, metadata)

    def lag(self, field: T) -> T:
        """
        Returns value from previous row.

        :param field: The field to get previous value of
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"lag({metadata.literal})")
        return cast(T, metadata)

    def lead(self, field: T) -> T:
        """
        Returns value from next row.

        :param field: The field to get next value of
        :return: A function metadata object preserving the input type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"lead({metadata.literal})")
        return cast(T, metadata)

    # Type Conversion Functions
    def cast(self, field: T, type_name: str) -> Any:
        """
        Converts value to specified type.

        :param field: The field to convert
        :param type_name: The target type name
        :return: A function metadata object with the new type
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"cast({metadata.literal} as {type_name})")
        return metadata

    def to_char(self, field: T, format: str) -> str:
        """
        Converts value to string with format.

        :param field: The field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a string
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_char({metadata.literal}, '{format}')")
        return cast(str, metadata)

    def to_number(self, field: T, format: str) -> float:
        """
        Converts string to number with format.

        :param field: The string field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a float
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_number({metadata.literal}, '{format}')")
        return cast(float, metadata)

    def to_timestamp(self, field: T, format: str) -> T:
        """
        Converts string to timestamp with format.

        :param field: The string field to convert
        :param format: The format string
        :return: A function metadata object that resolves to a timestamp
        """
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"to_timestamp({metadata.literal}, '{format}')")
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
