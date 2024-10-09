from __future__ import annotations

from typing import Any, Generic, Literal, Type, TypeVar, TypeVarTuple, overload

from iceaxe.base import (
    DBFieldClassComparison,
    DBFieldClassDefinition,
    DBModelMetaclass,
    TableBase,
)
from iceaxe.functions import FunctionMetadata, FunctionMetadataComparison
from iceaxe.queries_str import (
    QueryElementBase,
    QueryIdentifier,
    QueryLiteral,
    field_to_literal,
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
    is_function_metadata,
    is_function_metadata_comparison,
)

P = TypeVar("P")

T = TypeVar(
    "T",
    bound=TableBase
    | DBModelMetaclass
    | ALL_ENUM_TYPES
    | PRIMITIVE_TYPES
    | PRIMITIVE_WRAPPER_TYPES
    | DATE_TYPES
    | JSON_WRAPPER_FALLBACK,
)
Ts = TypeVarTuple("Ts")


QueryType = TypeVar("QueryType", bound=Literal["SELECT", "INSERT", "UPDATE", "DELETE"])


JoinType = Literal["INNER", "LEFT", "RIGHT", "FULL"]
OrderDirection = Literal["ASC", "DESC"]


class QueryBuilder(Generic[P, QueryType]):
    def __init__(self):
        self.query_type: QueryType | None = None
        self.main_model: Type[TableBase] | None = None

        self.return_typehint: P

        self.where_conditions: list[DBFieldClassComparison] = []
        self.order_by_clauses: list[str] = []
        self.join_clauses: list[str] = []
        self.limit_value: int | None = None
        self.offset_value: int | None = None
        self.group_by_fields: list[DBFieldClassDefinition] = []
        self.having_conditions: list[FunctionMetadataComparison] = []

        # Query specific params
        self.update_values: dict[str, Any] = {}
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
        self, fields: tuple[T | Type[T], *Ts]
    ) -> QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]: ...

    def select(
        self, fields: T | Type[T] | tuple[T | Type[T], *Ts]
    ) -> (
        QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]
        | QueryBuilder[T, Literal["SELECT"]]
    ):
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
                self.select_fields.append(field_to_literal(field))
                self.select_raw.append(field)
            elif is_base_table(field):
                table_token = QueryIdentifier(field.get_table_name())
                field_token = QueryLiteral("*")
                self.select_fields.append(QueryLiteral(f"{table_token}.{field_token}"))
                self.select_raw.append(field)
            elif is_function_metadata(field):
                field.local_name = f"aggregate_{self.select_aggregate_count}"
                local_name_token = QueryLiteral(field.local_name)

                self.select_fields.append(
                    QueryLiteral(f"{field.literal} AS {local_name_token}")
                )
                self.select_raw.append(field)
                self.select_aggregate_count += 1

    def update(self, model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
        self.query_type = "UPDATE"  # type: ignore
        self.main_model = model
        return self  # type: ignore

    def where(self, *conditions: bool):
        # During typechecking these seem like bool values, since they're the result
        # of the comparison set. But at runtime they will be the whole object that
        # gives the comparison. We can assert that's true here.
        validated_comparisons: list[DBFieldClassComparison] = []
        for condition in conditions:
            if not is_comparison(condition):
                raise ValueError(f"Invalid where condition: {condition}")
            validated_comparisons.append(condition)

        self.where_conditions += validated_comparisons
        return self

    def order_by(self, field: Any, direction: OrderDirection = "ASC"):
        if not is_column(field):
            raise ValueError(f"Invalid order by field: {field}")

        field_token = field_to_literal(field)
        self.order_by_clauses.append(f"{field_token} {direction}")
        return self

    def join(self, table: Type[TableBase], on: bool, join_type: JoinType = "INNER"):
        if not is_comparison(on):
            raise ValueError(
                f"Invalid join condition: {on}, should be MyTable.column == OtherTable.column"
            )

        table_name = QueryLiteral(table.get_table_name())
        on_left = field_to_literal(on.left)
        comparison = QueryLiteral(on.comparison.value)
        on_right = field_to_literal(on.right)

        join_sql = f"{join_type} JOIN {table_name} ON {on_left} {comparison} {on_right}"
        self.join_clauses.append(join_sql)
        return self

    def limit(self, value: int):
        self.limit_value = value
        return self

    def offset(self, value: int):
        self.offset_value = value
        return self

    def group_by(self, *fields: Any):
        valid_fields: list[DBFieldClassDefinition] = []

        for field in fields:
            if not is_column(field):
                raise ValueError(f"Invalid field for group by: {field}")
            valid_fields.append(field)

        self.group_by_fields = valid_fields
        return self

    def having(self, *conditions: bool):
        valid_conditions: list[FunctionMetadataComparison] = []

        for condition in conditions:
            if not is_function_metadata_comparison(condition):
                raise ValueError(f"Invalid having condition: {condition}")
            valid_conditions.append(condition)

        self.having_conditions += valid_conditions
        return self

    def text(self, query: str, *variables: Any):
        """
        Override the ORM builder and use a raw SQL query instead.
        """
        self.text_query = query
        self.text_variables = list(variables)
        return self

    def build(self) -> tuple[str, list[Any]]:
        if self.text_query:
            return self.text_query, self.text_variables

        query = ""
        variables: list[Any] = []

        if self.query_type == "SELECT":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            fields = [str(field) for field in self.select_fields]
            query = f"SELECT {', '.join(fields)} FROM {primary_table}"
        elif self.query_type == "UPDATE":
            if not self.main_model:
                raise ValueError("No model selected for query")

            primary_table = QueryIdentifier(self.main_model.get_table_name())
            set_clause = ", ".join(f"{k} = %s" for k in self.update_values.keys())
            query = f"UPDATE {primary_table} SET {set_clause}"

        if self.join_clauses:
            query += " " + " ".join(self.join_clauses)

        if self.where_conditions:
            query += " WHERE "
            for i, condition in enumerate(self.where_conditions):
                if i > 0:
                    query += " AND "

                field = field_to_literal(condition.left)
                value: QueryElementBase
                if is_column(condition.right):
                    # Support comparison to other fields (both identifiers)
                    value = field_to_literal(condition.right)
                else:
                    # Support comparison to static values
                    variables.append(condition.right)
                    value = QueryLiteral("$" + str(len(variables)))

                query += f"{field} {condition.comparison.value} {value}"

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
# Shortcut entrypoints
# Instead of having to manually create a QueryBuilder object, these functions
# will create one for you and return it.
#


@overload
def select(fields: T | Type[T]) -> QueryBuilder[T, Literal["SELECT"]]: ...


@overload
def select(
    fields: tuple[T | Type[T], *Ts],
) -> QueryBuilder[tuple[T, *Ts], Literal["SELECT"]]: ...


def select(
    fields: T | Type[T] | tuple[T | Type[T], *Ts],
) -> (
    QueryBuilder[tuple[T, *Ts], Literal["SELECT"]] | QueryBuilder[T, Literal["SELECT"]]
):
    return QueryBuilder().select(fields)


def update(model: Type[TableBase]) -> QueryBuilder[None, Literal["UPDATE"]]:
    return QueryBuilder().update(model)
