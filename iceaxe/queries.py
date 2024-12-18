from __future__ import annotations

from copy import copy
from functools import wraps
from typing import Any, Generic, Literal, Type, TypeVar, TypeVarTuple, cast, overload

from iceaxe.base import (
    DBFieldClassDefinition,
    DBModelMetaclass,
    TableBase,
)
from iceaxe.comparison import ComparisonGroupType, FieldComparison, FieldComparisonGroup
from iceaxe.functions import FunctionMetadata
from iceaxe.queries_str import (
    QueryElementBase,
    QueryIdentifier,
    QueryLiteral,
)
from iceaxe.typing import (
    ALL_ENUM_TYPES,
    DATE_TYPES,
    JSON_WRAPPER_FALLBACK,
    PRIMITIVE_TYPES,
    PRIMITIVE_WRAPPER_TYPES,
    is_base_table,
    is_column,
    is_comparison,
    is_comparison_group,
    is_function_metadata,
)

P = TypeVar("P")

SUPPORTED_SELECTS = (
    TableBase
    | DBModelMetaclass
    | ALL_ENUM_TYPES
    | PRIMITIVE_TYPES
    | PRIMITIVE_WRAPPER_TYPES
    | DATE_TYPES
    | JSON_WRAPPER_FALLBACK
    | None
)

T = TypeVar("T", bound=SUPPORTED_SELECTS)
T2 = TypeVar("T2", bound=SUPPORTED_SELECTS)
T3 = TypeVar("T3", bound=SUPPORTED_SELECTS)
Ts = TypeVarTuple("Ts")


QueryType = TypeVar("QueryType", bound=Literal["SELECT", "INSERT", "UPDATE", "DELETE"])


JoinType = Literal["INNER", "LEFT", "RIGHT", "FULL"]
OrderDirection = Literal["ASC", "DESC"]


def allow_branching(fn):
    """
    Allows query method modifiers to implement their logic as if `self` is being
    modified, but in the background we'll actually return a new instance of the
    query builder to allow for branching of the same underlying query.

    """

    @wraps(fn)
    def new_fn(self, *args, **kwargs):
        self = copy(self)
        return fn(self, *args, **kwargs)

    return new_fn


class QueryBuilder(Generic[P, QueryType]):
    """
    The QueryBuilder owns all construction of the SQL string given
    python method chaining. Each function call returns a reference to
    self, so you can construct as many queries as you want in a single
    line of code.

    Internally we store most input-arguments as-is. We provide runtime
    value-checking to make sure the right objects are being passed in to query
    manipulation functions so our final build() will deterministically succeed
    if the query build was successful.

    Note that this runtime check-checking validates different types than the static
    analysis. To satisfy Python logical operations (like `join(ModelA.id == ModelB.id)`) we
    have many overloaded operators that return objects at runtime but are masked to their
    Python types for the purposes of static analysis. This implementation detail should
    be transparent to the user but is noted in case you see different types through
    runtime inspection than you see during the typehints.

    ```python {{sticky: True}}
    # Basic SELECT query
    query = (
        QueryBuilder()
        .select(User)
        .where(User.is_active == True)
        .order_by(User.created_at, "DESC")
    )

    # Complex query with joins and aggregates
    query = (
        QueryBuilder()
        .select((User.name, func.count(Order.id)))
        .join(Order, Order.user_id == User.id)
        .where(Order.status == "completed")
        .group_by(User.name)
        .having(func.count(Order.id) > 5)
    )
    ```
    """

    def __init__(self):
        self.query_type: QueryType | None = None
        self.main_model: Type[TableBase] | None = None

        self.return_typehint: P

        self.where_conditions: list[FieldComparison | FieldComparisonGroup] = []
        self.order_by_clauses: list[str] = []
        self.join_clauses: list[str] = []
        self.limit_value: int | None = None
        self.offset_value: int | None = None
        self.group_by_fields: list[DBFieldClassDefinition] = []
        self.having_conditions: list[FieldComparison] = []
        self.distinct_on_fields: list[QueryLiteral] = []

        # Query specific params
        self.update_values: list[tuple[DBFieldClassDefinition, Any]] = []
        self.select_fields: list[QueryLiteral] = []
        self.select_raw: list[
            DBFieldClassDefinition | Type[TableBase] | FunctionMetadata
        ] = []
        self.select_aggregate_count = 0

        # Text
        self.text_query: str | None = None
        self.text_variables: list[Any] = []

    @overload
    def select(self, fields: T | Type[T]) -> QueryBuilder[T, Literal["SELECT"]]: ...

    @overload
    def select(
        self, fields: tuple[T | Type[T]]
    ) -> QueryBuilder[tuple[T], Literal["SELECT"]]: ...

    @overload
    def select(
        self, fields: tuple[T | Type[T], T2 | Type[T2]]
    ) -> QueryBuilder[tuple[T, T2], Literal["SELECT"]]: ...

    @overload
    def select(
        # TypeVarTuples only match the typing of one-or-more elements, so we also
        # need a overloaded signature for 3 elements.
        self,
        fields: tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3]],
    ) -> QueryBuilder[tuple[T, T2, T3], Literal["SELECT"]]: ...

    @overload
    def select(
        self, fields: tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3], *Ts]
    ) -> QueryBuilder[tuple[T, T2, T3, *Ts], Literal["SELECT"]]: ...

    @allow_branching
    def select(
        self,
        fields: (
            T
            | Type[T]
            | tuple[T | Type[T]]
            | tuple[T | Type[T], T2 | Type[T2]]
            | tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3]]
            | tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3], *Ts]
        ),
    ) -> (
        QueryBuilder[T, Literal["SELECT"]]
        | QueryBuilder[tuple[T], Literal["SELECT"]]
        | QueryBuilder[tuple[T, T2], Literal["SELECT"]]
        | QueryBuilder[tuple[T, T2, T3], Literal["SELECT"]]
        | QueryBuilder[tuple[T, T2, T3, *Ts], Literal["SELECT"]]
    ):
        """
        Creates a SELECT query to fetch data from the database.

        ```python {{sticky: True}}
        # Select all fields from User
        query = QueryBuilder().select(User)

        # Select specific fields
        query = QueryBuilder().select((User.id, User.name))

        # Select with aggregation
        query = QueryBuilder().select((
            User.name,
            func.count(Order.id).as_("order_count")
        ))

        # Select from multiple tables
        query = QueryBuilder().select((User, Order))
        ```

        :param fields: The fields to select. Can be:
                      - A single field (e.g., User.id)
                      - A model class (e.g., User)
                      - A tuple of fields (e.g., (User.id, User.name))
                      - A tuple of model classes (e.g., (User, Post))
        :return: A QueryBuilder instance configured for SELECT operations

        """
        all_fields: tuple[
            DBFieldClassDefinition | Type[TableBase] | FunctionMetadata, ...
        ]
        if not isinstance(fields, tuple):
            all_fields = (fields,)  # type: ignore
        else:
            all_fields = fields  # type: ignore

        # Verify the field type
        for field in all_fields:
            if (
                not is_column(field)
                and not is_base_table(field)
                and not is_function_metadata(field)
            ):
                raise ValueError(
                    f"Invalid field type {field}. Must be:\n1. A column field\n2. A table\n3. A QueryLiteral\n4. A tuple of the above."
                )

        self._select_inner(all_fields)

        return self  # type: ignore

    def _select_inner(
        self,
        fields: tuple[DBFieldClassDefinition | Type[TableBase] | FunctionMetadata, ...],
    ):
        self.query_type = "SELECT"  # type: ignore
        self.return_typehint = fields  # type: ignore

        if not fields:
            raise ValueError("At least one field must be selected")

        # We always take the default FROM table as the first element
        representative_field = fields[0]
        if is_column(representative_field):
            self.main_model = representative_field.root_model
        elif is_base_table(representative_field):
            self.main_model = representative_field
        elif is_function_metadata(representative_field):
            self.main_model = representative_field.original_field.root_model

        for field in fields:
            if is_column(field):
                literal, _ = field.to_query()
                self.select_fields.append(literal)
                self.select_raw.append(field)
            elif is_base_table(field):
                self.select_fields.append(field.select_fields())
                self.select_raw.append(field)
            elif is_function_metadata(field):
                field.local_name = f"aggregate_{self.select_aggregate_count}"
                local_name_token = QueryLiteral(field.local_name)

                self.select_fields.append(
                    QueryLiteral(f"{field.literal} AS {local_name_token}")
                )
                self.select_raw.append(field)
                self.select_aggregate_count += 1

    @allow_branching
    def update(self, model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
        """
        Creates a new update query for the given model. Returns the same
        QueryBuilder that is now flagged as an UPDATE query.

        """
        self.query_type = "UPDATE"  # type: ignore
        self.main_model = model
        return self  # type: ignore

    @allow_branching
    def delete(self, model: Type[TableBase]) -> QueryBuilder[None, Literal["DELETE"]]:
        """
        Creates a new delete query for the given model. Returns the same
        QueryBuilder that is now flagged as a DELETE query.

        """
        self.query_type = "DELETE"  # type: ignore
        self.main_model = model
        return self  # type: ignore

    @allow_branching
    def where(self, *conditions: bool):
        """
        Adds WHERE conditions to filter the query results. Multiple conditions are combined with AND.
        For OR conditions, use the `or_` function.

        ```python {{sticky: True}}
        # Simple condition
        query = (
            QueryBuilder()
            .select(User)
            .where(User.age >= 18)
        )

        # Multiple conditions (AND)
        query = (
            QueryBuilder()
            .select(User)
            .where(
                User.age >= 18,
                User.is_active == True
            )
        )

        # Complex conditions with AND/OR
        query = (
            QueryBuilder()
            .select(User)
            .where(
                and_(
                    User.age >= 18,
                    or_(
                        User.role == "admin",
                        User.permissions.contains("manage_users")
                    )
                )
            )
        )
        ```

        :param conditions: One or more boolean conditions using field comparisons
        :return: The QueryBuilder instance for method chaining

        """
        # During typechecking these seem like bool values, since they're the result
        # of the comparison set. But at runtime they will be the whole object that
        # gives the comparison. We can assert that's true here.
        validated_comparisons: list[FieldComparison | FieldComparisonGroup] = []
        for condition in conditions:
            if not is_comparison(condition) and not is_comparison_group(condition):
                raise ValueError(f"Invalid where condition: {condition}")
            validated_comparisons.append(condition)

        self.where_conditions += validated_comparisons
        return self

    @allow_branching
    def order_by(self, field: Any, direction: OrderDirection = "ASC"):
        """
        Adds an ORDER BY clause to sort the query results.

        ```python {{sticky: True}}
        # Simple ascending sort
        query = (
            QueryBuilder()
            .select(User)
            .order_by(User.created_at)
        )

        # Descending sort
        query = (
            QueryBuilder()
            .select(User)
            .order_by(User.created_at, "DESC")
        )

        # Multiple sort criteria
        query = (
            QueryBuilder()
            .select(User)
            .order_by(User.last_name, "ASC")
            .order_by(User.first_name, "ASC")
        )
        ```

        :param field: The field to sort by (must be a column)
        :param direction: The sort direction, either "ASC" or "DESC"
        :return: The QueryBuilder instance for method chaining

        """
        if not is_column(field):
            raise ValueError(f"Invalid order by field: {field}")

        field_token, _ = field.to_query()
        self.order_by_clauses.append(f"{field_token} {direction}")
        return self

    @allow_branching
    def join(self, table: Type[TableBase], on: bool, join_type: JoinType = "INNER"):
        """
        Adds a JOIN clause to combine data from multiple tables.

        ```python {{sticky: True}}
        # Inner join
        query = (
            QueryBuilder()
            .select((User.name, Order.total))
            .join(Order, Order.user_id == User.id)
        )

        # Left join
        query = (
            QueryBuilder()
            .select((User.name, func.count(Order.id)))
            .join(Order, Order.user_id == User.id, "LEFT")
            .group_by(User.name)
        )

        # Multiple joins
        query = (
            QueryBuilder()
            .select((User.name, Order.id, Product.name))
            .join(Order, Order.user_id == User.id)
            .join(Product, Product.id == Order.product_id)
        )
        ```

        :param table: The table to join with
        :param on: The join condition (e.g., Table1.id == Table2.table1_id)
        :param join_type: The type of join: "INNER", "LEFT", "RIGHT", or "FULL"
        :return: The QueryBuilder instance for method chaining

        """
        if not is_comparison(on):
            raise ValueError(
                f"Invalid join condition: {on}, should be MyTable.column == OtherTable.column"
            )

        table_name = QueryLiteral(table.get_table_name())
        on_left, _ = on.left.to_query()
        comparison = QueryLiteral(on.comparison.value)
        on_right, _ = on.right.to_query()

        join_sql = f"{join_type} JOIN {table_name} ON {on_left} {comparison} {on_right}"
        self.join_clauses.append(join_sql)
        return self

    @allow_branching
    def set(self, column: T, value: T | None):
        """
        Sets a column to a specific value in an update query.

        """
        if not is_column(column):
            raise ValueError(f"Invalid column for set: {column}")

        self.update_values.append((column, value))
        return self

    @allow_branching
    def limit(self, value: int):
        """
        Limits the number of rows returned by the query.

        ```python {{sticky: True}}
        # Basic limit
        query = (
            QueryBuilder()
            .select(User)
            .limit(10)
        )

        # Limit with offset for pagination
        query = (
            QueryBuilder()
            .select(User)
            .order_by(User.created_at, "DESC")
            .limit(20)
            .offset(40)  # Skip first 40 rows
        )
        ```

        :param value: Maximum number of rows to return
        :return: The QueryBuilder instance for method chaining

        """
        self.limit_value = value
        return self

    @allow_branching
    def offset(self, value: int):
        """
        Skips the specified number of rows before returning results.

        ```python {{sticky: True}}
        # Basic offset
        query = (
            QueryBuilder()
            .select(User)
            .offset(10)
        )

        # Implementing pagination
        page_size = 20
        page_number = 3
        query = (
            QueryBuilder()
            .select(User)
            .order_by(User.created_at, "DESC")
            .limit(page_size)
            .offset((page_number - 1) * page_size)
        )
        ```

        :param value: Number of rows to skip
        :return: The QueryBuilder instance for method chaining

        """
        self.offset_value = value
        return self

    @allow_branching
    def group_by(self, *fields: Any):
        """
        Groups the results by specified fields, typically used with aggregate functions.

        ```python {{sticky: True}}
        # Simple grouping with count
        query = (
            QueryBuilder()
            .select((User.status, func.count(User.id)))
            .group_by(User.status)
        )

        # Multiple group by fields
        query = (
            QueryBuilder()
            .select((
                User.country,
                User.city,
                func.count(User.id),
                func.avg(User.age)
            ))
            .group_by(User.country, User.city)
        )

        # Group by with having
        query = (
            QueryBuilder()
            .select((User.department, func.count(User.id)))
            .group_by(User.department)
            .having(func.count(User.id) > 5)
        )
        ```

        :param fields: One or more fields to group by
        :return: The QueryBuilder instance for method chaining

        """
        valid_fields: list[DBFieldClassDefinition] = []

        for field in fields:
            if not is_column(field):
                raise ValueError(f"Invalid field for group by: {field}")
            valid_fields.append(field)

        self.group_by_fields = valid_fields
        return self

    @allow_branching
    def having(self, *conditions: bool):
        """
        Adds HAVING conditions to filter grouped results based on aggregate values.

        ```python {{sticky: True}}
        # Filter groups by count
        query = (
            QueryBuilder()
            .select((User.department, func.count(User.id)))
            .group_by(User.department)
            .having(func.count(User.id) > 10)
        )

        # Multiple having conditions
        query = (
            QueryBuilder()
            .select((
                User.department,
                func.count(User.id),
                func.avg(User.salary)
            ))
            .group_by(User.department)
            .having(
                func.count(User.id) >= 5,
                func.avg(User.salary) > 50000
            )
        )
        ```

        :param conditions: One or more conditions using aggregate functions
        :return: The QueryBuilder instance for method chaining

        """
        valid_conditions: list[FieldComparison] = []

        for condition in conditions:
            if not is_comparison(condition):
                raise ValueError(f"Invalid having condition: {condition}")
            valid_conditions.append(condition)

        self.having_conditions += valid_conditions
        return self

    @allow_branching
    def distinct_on(self, *fields: Any):
        """
        Adds a DISTINCT ON clause to remove duplicate rows based on specified fields.

        ```python {{sticky: True}}
        # Get distinct user names
        query = (
            QueryBuilder()
            .select((User.name, User.email))
            .distinct_on(User.name)
        )

        # Multiple distinct fields
        query = (
            QueryBuilder()
            .select((User.country, User.city, User.population))
            .distinct_on(User.country, User.city)
        )
        ```

        :param fields: Fields to check for distinctness
        :return: The QueryBuilder instance for method chaining

        """
        valid_fields: list[QueryLiteral] = []

        for field in fields:
            if not is_column(field):
                raise ValueError(f"Invalid field for group by: {field}")
            literal, _ = field.to_query()
            valid_fields.append(literal)

        self.distinct_on_fields = valid_fields
        return self

    @allow_branching
    def text(self, query: str, *variables: Any):
        """
        Uses a raw SQL query instead of the query builder.

        ```python {{sticky: True}}
        # Simple raw query
        query = (
            QueryBuilder()
            .text("SELECT * FROM users WHERE age > $1", 18)
        )

        # Complex raw query with multiple parameters
        query = (
            QueryBuilder()
            .text(
                '''
                SELECT u.name, COUNT(o.id) as order_count
                FROM users u
                LEFT JOIN orders o ON o.user_id = u.id
                WHERE u.created_at > $1
                GROUP BY u.name
                HAVING COUNT(o.id) > $2
                ''',
                datetime(2023, 1, 1),
                5
            )
        )
        ```

        :param query: Raw SQL query string with $1, $2, etc. as parameter placeholders
        :param variables: Values for the query parameters
        :return: The QueryBuilder instance for method chaining

        """
        self.text_query = query
        self.text_variables = list(variables)
        return self

    def build(self) -> tuple[str, list[Any]]:
        """
        Builds and returns the final SQL query string and parameter values.

        ```python {{sticky: True}}
        # Build a query
        query = (
            QueryBuilder()
            .select(User)
            .where(User.age > 18)
        )
        sql, params = query.build()
        print(sql)    # SELECT ... FROM users WHERE age > $1
        print(params) # [18]

        # Execute the built query
        async with conn.transaction():
            result = await conn.execute(*query.build())
        ```

        :return: A tuple of (query_string, parameter_list)

        """
        if self.text_query:
            return self.text_query, self.text_variables

        query = ""
        variables: list[Any] = []

        if self.query_type == "SELECT":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            fields = [str(field) for field in self.select_fields]
            query = "SELECT"

            if self.distinct_on_fields:
                distinct_fields = [
                    str(distinct_field) for distinct_field in self.distinct_on_fields
                ]
                query += f" DISTINCT ON ({', '.join(distinct_fields)})"

            query += f" {', '.join(fields)} FROM {primary_table}"
        elif self.query_type == "UPDATE":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())

            set_components = []
            for column, value in self.update_values:
                column_token, _ = column.to_query()
                set_components.append(f"{column_token} = ${len(variables) + 1}")
                variables.append(value)

            set_clause = ", ".join(set_components)
            query = f"UPDATE {primary_table} SET {set_clause}"
        elif self.query_type == "DELETE":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            query = f"DELETE FROM {primary_table}"

        if self.join_clauses:
            query += " " + " ".join(self.join_clauses)

        if self.where_conditions:
            comparison_group = cast(FieldComparisonGroup, and_(*self.where_conditions))  # type: ignore
            comparison_literal, comparison_variables = comparison_group.to_query(
                len(variables) + 1
            )
            query += f" WHERE {comparison_literal}"
            variables += comparison_variables

        if self.group_by_fields:
            query += " GROUP BY "
            query += ", ".join(
                f"{QueryIdentifier(field.root_model.get_table_name())}.{QueryIdentifier(field.key)}"
                for field in self.group_by_fields
            )

        if self.having_conditions:
            query += " HAVING "
            for i, having_condition in enumerate(self.having_conditions):
                if i > 0:
                    query += " AND "

                having_field = having_condition.left.literal
                having_value: QueryElementBase
                if is_function_metadata(having_condition.right):
                    having_value = having_condition.right.literal
                else:
                    variables.append(having_condition.right)
                    having_value = QueryLiteral("$" + str(len(variables)))

                query += (
                    f"{having_field} {having_condition.comparison.value} {having_value}"
                )

        if self.order_by_clauses:
            query += " ORDER BY " + ", ".join(self.order_by_clauses)

        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"

        if self.offset_value is not None:
            query += f" OFFSET {self.offset_value}"

        return query, variables


#
# Comparison chaining
#


def and_(
    *conditions: bool,
) -> bool:
    """
    Combines multiple conditions with logical AND.
    All conditions must be true for the group to be true.

    ```python {{sticky: True}}
    query = select(User).where(
        and_(
            User.age >= 21,
            User.status == "active",
            User.role == "member"
        )
    )
    ```

    :param conditions: Variable number of conditions to combine
    :return: A field comparison group object

    """
    field_comparisons: list[FieldComparison | FieldComparisonGroup] = []
    for condition in conditions:
        if not is_comparison(condition) and not is_comparison_group(condition):
            raise ValueError(f"Invalid having condition: {condition}")
        field_comparisons.append(condition)
    return cast(
        bool,
        FieldComparisonGroup(type=ComparisonGroupType.AND, elements=field_comparisons),
    )


def or_(
    *conditions: bool,
) -> bool:
    """
    Combines multiple conditions with logical OR.
    At least one condition must be true for the group to be true.

    ```python {{sticky: True}}
    query = select(User).where(
        or_(
            User.role == "admin",
            and_(
                User.role == "moderator",
                User.permissions.contains("manage_users")
            )
        )
    )
    ```

    :param conditions: Variable number of conditions to combine
    :return: A field comparison group object

    """
    field_comparisons: list[FieldComparison | FieldComparisonGroup] = []
    for condition in conditions:
        if not is_comparison(condition) and not is_comparison_group(condition):
            raise ValueError(f"Invalid having condition: {condition}")
        field_comparisons.append(condition)
    return cast(
        bool,
        FieldComparisonGroup(type=ComparisonGroupType.OR, elements=field_comparisons),
    )


#
# Shortcut entrypoints
# Instead of having to manually create a QueryBuilder object, these functions
# will create one for you and return it.
#


@overload
def select(fields: T | Type[T]) -> QueryBuilder[T, Literal["SELECT"]]: ...


@overload
def select(
    fields: tuple[T | Type[T]],
) -> QueryBuilder[tuple[T], Literal["SELECT"]]: ...


@overload
def select(
    fields: tuple[T | Type[T], T2 | Type[T2]],
) -> QueryBuilder[tuple[T, T2], Literal["SELECT"]]: ...


@overload
def select(
    fields: tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3], *Ts],
) -> QueryBuilder[tuple[T, T2, T3, *Ts], Literal["SELECT"]]: ...


def select(
    fields: (
        T
        | Type[T]
        | tuple[T | Type[T]]
        | tuple[T | Type[T], T2 | Type[T2]]
        | tuple[T | Type[T], T2 | Type[T2], T3 | Type[T3], *Ts]
    ),
) -> (
    QueryBuilder[T, Literal["SELECT"]]
    | QueryBuilder[tuple[T], Literal["SELECT"]]
    | QueryBuilder[tuple[T, T2], Literal["SELECT"]]
    | QueryBuilder[tuple[T, T2, T3, *Ts], Literal["SELECT"]]
):
    """
    Creates a SELECT query to fetch data from the database. This is a shortcut function that creates
    and returns a new QueryBuilder instance.

    ```python {{sticky: True}}
    # Select all fields from User
    users = await conn.execute(select(User))

    # Select specific fields
    results = await conn.execute(select((User.id, User.name)))

    # Select with conditions
    active_users = await conn.execute(
        select(User)
        .where(User.is_active == True)
        .order_by(User.created_at, "DESC")
        .limit(10)
    )
    ```

    :param fields: The fields to select. Can be:
                  - A single field or model class (e.g., User.id or User)
                  - A tuple of fields (e.g., (User.id, User.name))
                  - A tuple of model classes (e.g., (User, Post))
    :return: A QueryBuilder instance configured for SELECT operations

    """
    return QueryBuilder().select(fields)


def update(model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
    """
    Creates an UPDATE query to modify existing records in the database. This is a shortcut function
    that creates and returns a new QueryBuilder instance.

    ```python {{sticky: True}}
    # Update all users' status
    await conn.execute(
        update(User)
        .set(User.status, "inactive")
        .where(User.last_login < datetime.now() - timedelta(days=30))
    )

    # Update multiple fields with conditions
    await conn.execute(
        update(User)
        .set(User.verified, True)
        .set(User.verification_date, datetime.now())
        .where(User.email_confirmed == True)
    )
    ```

    :param model: The model class representing the table to update
    :return: A QueryBuilder instance configured for UPDATE operations

    """
    return QueryBuilder().update(model)


def delete(model: Type[TableBase]) -> QueryBuilder[None, Literal["DELETE"]]:
    """
    Creates a DELETE query to remove records from the database. This is a shortcut function
    that creates and returns a new QueryBuilder instance.

    ```python {{sticky: True}}
    # Delete inactive users
    await conn.execute(
        delete(User)
        .where(User.is_active == False)
    )

    # Delete with complex conditions
    await conn.execute(
        delete(User)
        .where(
            and_(
                User.created_at < datetime.now() - timedelta(days=90),
                User.email_confirmed == False
            )
        )
    )
    ```

    :param model: The model class representing the table to delete from
    :return: A QueryBuilder instance configured for DELETE operations

    """
    return QueryBuilder().delete(model)
