from __future__ import annotations

from abc import ABC, abstractmethod

from iceaxe.typing import is_base_table, is_column


class QueryElementBase(ABC):
    """
    Abstract base class for SQL query elements that require special string processing.
    This class provides the foundation for handling different types of SQL elements
    (like identifiers and literals) with their specific escaping and formatting rules.

    The class implements equality comparisons, hashing, and sorting based on the processed
    string representation, making it suitable for comparing query elements in test assertions,
    caching, using elements as dictionary keys or in sets, and sorting collections.

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

    def __lt__(self, other):
        """
        Enable sorting of query elements based on their string representation.
        This makes QueryElementBase instances sortable using sorted() or list.sort().

        :param other: Another QueryElementBase instance to compare with
        :return: True if this element's string representation comes before the other's
        """
        return str(self) < str(other)

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"{self.__class__.__name__}({self._value})"

    def __hash__(self):
        """
        Generate a hash based on the string representation of the query element.
        This makes QueryElementBase instances usable as dictionary keys and in sets.

        :return: Hash value of the processed string
        """
        return hash(str(self))


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


class SQLGenerator:
    """
    The SQLGenerator class provides a convenient way to generate SQL-safe strings for various
    query elements. It acts as a singleton that handles the proper formatting and escaping
    of table names, column names, and other SQL elements.

    The class provides three main methods:
    - __call__: For generating table-qualified column names
    - select: For generating SELECT clause elements with proper aliases
    - raw: For generating raw identifiers without table qualification

    Each method handles both column and table inputs, applying appropriate formatting rules
    to prevent SQL injection and ensure proper identifier quoting.

    ```python {{sticky: True}}
    # Basic usage with columns
    sql(User.name)          # -> "users"."name"
    sql.select(User.name)   # -> "users"."name" AS "users_name"
    sql.raw(User.name)      # -> "name"

    # Basic usage with tables
    sql(User)               # -> "users"
    sql.select(User)        # -> "users"."id" as "users_id", "users"."name" as "users_name"
    sql.raw(User)          # -> "users"

    # Usage in query building
    query = f"SELECT {sql.select(User.name)} FROM {sql(User)}"
    # -> SELECT "users"."name" AS "users_name" FROM "users"
    ```
    """

    def __call__(self, obj) -> QueryElementBase:
        """
        Generate a table-qualified column name or table identifier. This is the default
        format used in most SQL contexts where a column or table reference is needed.

        For columns, the output includes the table name to prevent ambiguity in JOINs
        and complex queries. For tables, it returns just the quoted table name.

        :param obj: A column or table object
        :return: A SQL-safe string representation

        ```python {{sticky: True}}
        # Column usage
        sql(User.name)
        # -> "users"."name"

        sql(Post.title)
        # -> "posts"."title"

        # Table usage
        sql(User)
        # -> "users"

        # In a query context
        query = f"UPDATE {sql(User)} SET {sql(User.name)} = 'John'"
        # -> UPDATE "users" SET "users"."name" = 'John'

        # In a JOIN context
        query = f"SELECT {sql(User.name)} FROM {sql(User)} JOIN {sql(Post)} ON {sql(User.id)} = {sql(Post.user_id)}"
        # -> SELECT "users"."name" FROM "users" JOIN "posts" ON "users"."id" = "posts"."user_id"
        ```
        """
        if is_column(obj):
            table = QueryIdentifier(obj.root_model.get_table_name())
            column = QueryIdentifier(obj.key)
            return QueryLiteral(f"{table}.{column}")
        elif is_base_table(obj):
            return QueryIdentifier(obj.get_table_name())
        else:
            raise ValueError(f"Invalid type for sql: {type(obj)}")

    def select(self, obj) -> QueryElementBase:
        """
        Generate a SQL-safe string for selecting fields with proper aliases. This format
        is specifically designed for SELECT clauses where unique column aliases are needed
        to prevent name collisions.

        For columns, generates a table-qualified column with an alias that includes both
        table and column names. For tables, generates a comma-separated list of all
        columns with their aliases.

        :param obj: A column or table object
        :return: A SQL-safe string with proper SELECT clause formatting

        ```python {{sticky: True}}
        # Single column selection
        sql.select(User.name)
        # -> "users"."name" AS "users_name"

        # Full table selection
        sql.select(User)
        # -> "users"."id" as "users_id", "users"."name" as "users_name", "users"."email" as "users_email"

        # In a complex query with multiple tables
        query = f'''
            SELECT
                {sql.select(User)},
                {sql.select(Post.title)}
            FROM {sql(User)}
            JOIN {sql(Post)} ON {sql(User.id)} = {sql(Post.user_id)}
        '''
        # -> SELECT
        #    "users"."id" as "users_id", "users"."name" as "users_name",
        #    "posts"."title" AS "posts_title"
        #    FROM "users"
        #    JOIN "posts" ON "users"."id" = "posts"."user_id"
        ```
        """
        if is_column(obj):
            table = QueryIdentifier(obj.root_model.get_table_name())
            column = QueryIdentifier(obj.key)
            alias = QueryIdentifier(f"{obj.root_model.get_table_name()}_{obj.key}")
            return QueryLiteral(f"{table}.{column} AS {alias}")
        elif is_base_table(obj):
            table_token = QueryIdentifier(obj.get_table_name())
            select_fields: list[str] = []
            for field_name in obj.get_client_fields():
                field_token = QueryIdentifier(field_name)
                return_field = QueryIdentifier(f"{obj.get_table_name()}_{field_name}")
                select_fields.append(f"{table_token}.{field_token} AS {return_field}")
            return QueryLiteral(", ".join(select_fields))
        else:
            raise ValueError(f"Invalid type for select: {type(obj)}")

    def raw(self, obj) -> QueryElementBase:
        """
        Generate a raw identifier without table qualification. This is useful in specific
        contexts where you need just the column or table name without any additional
        qualification.

        For columns, returns just the column name without the table prefix. For tables,
        returns just the table name. All identifiers are still properly quoted.

        :param obj: A column or table object
        :return: A SQL-safe raw identifier

        ```python {{sticky: True}}
        # Column usage
        sql.raw(User.name)
        # -> "name"

        sql.raw(Post.title)
        # -> "title"

        # Table usage
        sql.raw(User)
        # -> "users"

        # Useful in specific contexts like ORDER BY or GROUP BY
        query = f"SELECT {sql.select(User.name)} FROM {sql(User)} ORDER BY {sql.raw(User.name)}"
        # -> SELECT "users"."name" AS "users_name" FROM "users" ORDER BY "name"

        # Or in UPDATE SET clauses where table qualification isn't needed
        query = f"UPDATE {sql(User)} SET {sql.raw(User.name)} = 'John'"
        # -> UPDATE "users" SET "name" = 'John'
        ```
        """
        if is_column(obj):
            return QueryIdentifier(obj.key)
        elif is_base_table(obj):
            return QueryIdentifier(obj.get_table_name())
        else:
            raise ValueError(f"Invalid type for raw: {type(obj)}")


sql = SQLGenerator()
