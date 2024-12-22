from typing import Generic, TypeVar, cast

T = TypeVar("T")


class Alias(Generic[T]):
    def __init__(self, name: str, type: T):
        self.name = name
        self.type = type

    def __str__(self):
        return self.name


def alias(name: str, type: T) -> T:
    """
    Creates an alias for a field in raw SQL queries, allowing for type-safe mapping of raw SQL results.
    This is particularly useful when you need to combine ORM-based queries with raw SQL, while maintaining
    type safety and automatic deserialization.

    ```python {{sticky: True}}
    # Basic usage with a single alias
    select(alias("name", int)).text(
        "SELECT concat(first_name, ' ', last_name) AS name FROM users"
    )

    # Combining with ORM models
    select((User, alias("total_orders", int))).text(
        "SELECT users.*, COUNT(orders.id) AS total_orders FROM users LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id"
    )

    # Multiple aliases in a single query
    select((
        alias("full_name", str),
        alias("order_count", int),
        alias("total_spent", float)
    )).text(
        '''
        SELECT
            concat(first_name, ' ', last_name) AS full_name,
            COUNT(orders.id) AS order_count,
            SUM(orders.amount) AS total_spent
        FROM users
        LEFT JOIN orders ON users.id = orders.user_id
        GROUP BY users.id
        '''
    )
    ```

    :param name: The name of the alias as it appears in the SQL query's AS clause
    :param type: The Python type to cast the result to (e.g., int, str, float). This should
        mirror the type of the field in the raw SQL query.
    :return: A type-safe alias that can be used in select() statements

    """
    return cast(T, Alias(name, type))
