from __future__ import annotations

from abc import ABC, abstractmethod


class QueryElementBase(ABC):
    """
    Abstract base class for SQL query elements that require special string processing.
    This class provides the foundation for handling different types of SQL elements
    (like identifiers and literals) with their specific escaping and formatting rules.

    The class implements equality comparisons based on the processed string representation,
    making it suitable for comparing query elements in test assertions and caching.

    ```python {{sticky: True}}
    # Base class is not used directly, but through its subclasses:
    table_name = QueryIdentifier("users")  # -> "users"
    raw_sql = QueryLiteral("COUNT(*)")    # -> COUNT(*)
    ```
    """

    def __init__(self, value: str):
        """
        :param value: The raw string value to be processed
        """
        self._value = self.process_value(value)

    @abstractmethod
    def process_value(self, value: str) -> str:
        """
        Process the input value according to the specific rules of the query element type.
        Must be implemented by subclasses.

        :param value: The raw string value to process
        :return: The processed string value
        """
        pass

    def __eq__(self, compare):
        return str(self) == str(compare)

    def __ne__(self, compare):
        return str(self) != str(compare)

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"{self.__class__.__name__}({self._value})"


class QueryIdentifier(QueryElementBase):
    """
    Represents a SQL identifier (table name, column name, etc.) that needs to be
    properly quoted to prevent SQL injection and handle special characters.

    When used, the identifier is automatically wrapped in double quotes, making it
    safe for use in queries even if it contains special characters or SQL keywords.

    ```python {{sticky: True}}
    # In a query builder context:
    table = QueryIdentifier("user_data")
    column = QueryIdentifier("email_address")
    print(f"SELECT {column} FROM {table}")
    # -> SELECT "email_address" FROM "user_data"

    # Handles special characters and keywords safely:
    reserved = QueryIdentifier("group")
    print(str(reserved))  # -> "group"
    ```
    """

    def process_value(self, value: str):
        return f'"{value}"'


class QueryLiteral(QueryElementBase):
    """
    Represents a raw SQL literal that should be included in the query exactly as provided,
    without any additional processing or escaping.

    This class is used for parts of the query that are already properly formatted and
    should not be modified, such as SQL functions, operators, or pre-processed strings.

    Warning:
        Be careful when using QueryLiteral with user input, as it bypasses SQL escaping.
        It should primarily be used for trusted, programmatically generated SQL components.

    ```python {{sticky: True}}
    # Safe usage with SQL functions:
    count = QueryLiteral("COUNT(*)")
    print(f"SELECT {count} FROM users")
    # -> SELECT COUNT(*) FROM users

    # Complex SQL expressions:
    case = QueryLiteral("CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END")
    print(f"SELECT name, {case} FROM users")
    # -> SELECT name, CASE WHEN age > 18 THEN 'adult' ELSE 'minor' END FROM users
    ```
    """

    def process_value(self, value: str):
        return value
