from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Generic, Self, TypeVar

from iceaxe.queries_str import QueryElementBase, QueryLiteral
from iceaxe.typing import is_column, is_comparison, is_comparison_group

T = TypeVar("T", bound="ComparisonBase")


class ComparisonType(StrEnum):
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    IN = "IN"
    NOT_IN = "NOT IN"
    LIKE = "LIKE"
    IS = "IS"
    IS_NOT = "IS NOT"


class ComparisonGroupType(StrEnum):
    AND = "AND"
    OR = "OR"


@dataclass
class FieldComparison(Generic[T]):
    left: T
    comparison: ComparisonType
    right: T | Any

    def to_query(self, start: int = 1):
        variables = []

        field, left_vars = self.left.to_query()
        variables += left_vars

        value: QueryElementBase
        comparison = self.comparison
        if is_column(self.right):
            # Support comparison to other fields (both identifiers)
            value, right_vars = self.right.to_query()
            variables += right_vars
        else:
            variable_offset = str(len(variables) + start)

            if self.right is None:
                # "None" values are not supported as query variables
                value = QueryLiteral("NULL")
            elif self.comparison in (ComparisonType.IN, ComparisonType.NOT_IN):
                variables.append(self.right)
                comparison_map = {
                    ComparisonType.IN: ComparisonType.EQ,
                    ComparisonType.NOT_IN: ComparisonType.NE,
                }
                comparison = comparison_map[self.comparison]
                value = QueryLiteral(f"ANY(${variable_offset})")
            else:
                # Support comparison to static values
                variables.append(self.right)
                value = QueryLiteral(f"${variable_offset}")

        return QueryLiteral(f"{field} {comparison.value} {value}"), variables


@dataclass
class FieldComparisonGroup:
    type: ComparisonGroupType
    elements: list["FieldComparison | FieldComparisonGroup"]

    def to_query(self, start: int = 1):
        queries = ""
        all_variables = []

        for i, element in enumerate(self.elements):
            if i > 0:
                queries += f" {self.type.value} "

            if is_comparison(element):
                query, variables = element.to_query(start=start + len(all_variables))
                queries += f"{query}"
                all_variables += variables
            elif is_comparison_group(element):
                query, variables = element.to_query(start=start + len(all_variables))
                queries += f"({query})"
                all_variables += variables
            else:
                raise ValueError(f"Unexpected element type: {type(element)}")

        return QueryLiteral(queries), all_variables


class ComparisonBase(ABC):
    def __eq__(self, other):  # type: ignore
        if other is None:
            return self._compare(ComparisonType.IS, None)
        return self._compare(ComparisonType.EQ, other)

    def __ne__(self, other):  # type: ignore
        if other is None:
            return self._compare(ComparisonType.IS_NOT, None)
        return self._compare(ComparisonType.NE, other)

    def __lt__(self, other):
        return self._compare(ComparisonType.LT, other)

    def __le__(self, other):
        return self._compare(ComparisonType.LE, other)

    def __gt__(self, other):
        return self._compare(ComparisonType.GT, other)

    def __ge__(self, other):
        return self._compare(ComparisonType.GE, other)

    def in_(self, other) -> bool:
        return self._compare(ComparisonType.IN, other)  # type: ignore

    def not_in(self, other) -> bool:
        return self._compare(ComparisonType.NOT_IN, other)  # type: ignore

    def like(self, other) -> bool:
        return self._compare(ComparisonType.LIKE, other)  # type: ignore

    def _compare(self, comparison: ComparisonType, other: Any) -> FieldComparison[Self]:
        return FieldComparison(left=self, comparison=comparison, right=other)

    @abstractmethod
    def to_query(self) -> tuple["QueryLiteral", list[Any]]:
        pass
