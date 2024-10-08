from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from inspect import isclass
from typing import Any, Generic, Literal, Type, TypeGuard, TypeVar, cast

from iceaxe.base import DBFieldClassComparison, DBFieldClassDefinition, TableBase

T = TypeVar("T")
P = TypeVar("P")

QueryType = TypeVar("QueryType", bound=Literal["SELECT", "INSERT", "UPDATE", "DELETE"])


class JoinType(Enum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"


class OrderDirection(Enum):
    ASC = "ASC"
    DESC = "DESC"


class Function(Enum):
    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MAX = "MAX"
    MIN = "MIN"


class QueryElementBase(ABC):
    def __init__(self, value: str):
        self._value = self.process_value(value)

    @abstractmethod
    def process_value(self, value: str) -> str:
        pass

    def __str__(self):
        return self._value

    def __repr__(self):
        return f"{self.__class__.__name__}({self._value})"


class QueryIdentifier(QueryElementBase):
    def process_value(self, value: str):
        return f'"{value}"'


class QueryLiteral(QueryElementBase):
    def process_value(self, value: str):
        return value


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

        # Query specific params
        self.update_values: dict[str, Any] = {}
        self.select_fields: list[QueryLiteral] = []
        self.select_raw: list[
            DBFieldClassDefinition | Type[TableBase] | QueryLiteral
        ] = []

        # Text
        self.text_query: str | None = None
        self.text_variables: list[Any] = []

    def select(self, fields: T) -> QueryBuilder[T, Literal["SELECT"]]:
        all_fields: tuple[DBFieldClassDefinition | Type[TableBase] | QueryLiteral, ...]
        if not isinstance(fields, tuple):
            all_fields = (fields,)  # type: ignore
        else:
            all_fields = fields

        # Verify the field type
        for field in all_fields:
            if (
                not is_column(field)
                and not is_base_table(field)
                and not isinstance(field, QueryLiteral)
            ):
                raise ValueError(
                    f"Invalid field type {field}. Must be:\n1. A column field\n2. A table\n3. A QueryLiteral\n4. A tuple of the above."
                )

        self._select_inner(all_fields)

        return self  # type: ignore

    def _select_inner(
        self,
        fields: tuple[DBFieldClassDefinition | Type[TableBase] | QueryLiteral, ...],
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
        else:
            raise ValueError(f"Unsupported first field type {representative_field}")

        for field in fields:
            if is_column(field):
                self.select_fields.append(field_to_literal(field))
                self.select_raw.append(field)
            elif is_base_table(field):
                table_token = QueryIdentifier(field.get_table_name())
                field_token = QueryLiteral("*")
                self.select_fields.append(QueryLiteral(f"{table_token}.{field_token}"))
                self.select_raw.append(field)
            elif is_literal(field):
                self.select_fields.append(field)
                self.select_raw.append(field)

    def update(self, model: Type[TableBase]) -> QueryBuilder[P, Literal["UPDATE"]]:
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

    def order_by(self, field: str, direction: OrderDirection = OrderDirection.ASC):
        self.order_by_clauses.append(f"{field} {direction.value}")
        return self

    def join(
        self, table: Type[TableBase], on: bool, join_type: JoinType = JoinType.INNER
    ):
        if not is_comparison(on):
            raise ValueError(
                f"Invalid join condition: {on}, should be MyTable.column == OtherTable.column"
            )

        table_name = QueryLiteral(table.get_table_name())
        on_left = field_to_literal(on.field)
        comparison = QueryLiteral(on.comparison.value)
        on_right = field_to_literal(on.value)

        join_sql = (
            f"{join_type.value} JOIN {table_name} ON {on_left} {comparison} {on_right}"
        )
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

                field = field_to_literal(condition.field)
                value: QueryElementBase
                if isinstance(condition.value, DBFieldClassDefinition):
                    # Support comparison to other fields (both identifiers)
                    value = field_to_literal(condition.value)
                else:
                    # Support comparison to static values
                    variables.append(condition.value)
                    value = QueryLiteral("$" + str(len(variables)))

                query += f"{field} {condition.comparison.value} {value}"

        if self.limit_value is not None:
            query += f" LIMIT {self.limit_value}"

        if self.offset_value is not None:
            query += f" OFFSET {self.offset_value}"

        if self.group_by_fields:
            query += " GROUP BY "
            query += ", ".join(
                f"{QueryIdentifier(field.root_model.get_table_name())}.{QueryIdentifier(field.key)}"
                for field in self.group_by_fields
            )

        return query, variables


def is_base_table(obj: Any) -> TypeGuard[type[TableBase]]:
    return isclass(obj) and issubclass(obj, TableBase)


def is_column(obj: Any) -> TypeGuard[DBFieldClassDefinition]:
    return isinstance(obj, DBFieldClassDefinition)


def is_comparison(obj: Any) -> TypeGuard[DBFieldClassComparison]:
    return isinstance(obj, DBFieldClassComparison)


def is_literal(obj: Any) -> TypeGuard[QueryLiteral]:
    return isinstance(obj, QueryLiteral)


def field_to_literal(field: DBFieldClassDefinition) -> QueryLiteral:
    table = QueryIdentifier(field.root_model.get_table_name())
    column = QueryIdentifier(field.key)
    return QueryLiteral(f"{table}.{column}")


class FunctionBuilder:
    # These should probably roll-up into a class like func
    def count(self, field: Any) -> int:
        field_name = self._column_to_name(field)
        return cast(int, QueryLiteral(f"count({field_name})"))

    def distinct(self, field: T) -> T:
        field_name = self._column_to_name(field)
        return cast(T, QueryLiteral(f"distinct {field_name}"))

    def _column_to_name(self, field: Any) -> QueryLiteral:
        if isinstance(field, QueryLiteral):
            return field
        elif is_column(field):
            return field_to_literal(field)
        else:
            raise ValueError(
                f"Unable to cast this type to a column: {field} ({type(field)})"
            )


func = FunctionBuilder()
