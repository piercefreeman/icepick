from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar, cast

from iceaxe.base import (
    ComparisonType,
    DBFieldClassDefinition,
)
from iceaxe.queries_str import QueryLiteral, field_to_literal
from iceaxe.typing import is_column, is_function_metadata

T = TypeVar("T")


@dataclass
class FunctionMetadataComparison:
    left: FunctionMetadata
    comparison: ComparisonType
    right: FunctionMetadata | Any


@dataclass
class FunctionMetadata:
    literal: QueryLiteral
    original_field: DBFieldClassDefinition
    local_name: str | None = None

    def __eq__(self, other):  # type: ignore
        return self._compare(ComparisonType.EQ, other)

    def __ne__(self, other):  # type: ignore
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

    def _compare(self, comparison: ComparisonType, other: Any):
        return FunctionMetadataComparison(left=self, comparison=comparison, right=other)


class FunctionBuilder:
    def count(self, field: Any) -> int:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"count({metadata.literal})")
        return cast(int, metadata)

    def distinct(self, field: T) -> T:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"distinct {metadata.literal}")
        return cast(T, metadata)

    def sum(self, field: T) -> T:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"sum({metadata.literal})")
        return cast(T, metadata)

    def avg(self, field: T) -> T:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"avg({metadata.literal})")
        return cast(T, metadata)

    def max(self, field: T) -> T:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"max({metadata.literal})")
        return cast(T, metadata)

    def min(self, field: T) -> T:
        metadata = self._column_to_metadata(field)
        metadata.literal = QueryLiteral(f"min({metadata.literal})")
        return cast(T, metadata)

    def _column_to_metadata(self, field: Any) -> FunctionMetadata:
        if is_function_metadata(field):
            return field
        elif is_column(field):
            return FunctionMetadata(
                literal=field_to_literal(field), original_field=field
            )
        else:
            raise ValueError(
                f"Unable to cast this type to a column: {field} ({type(field)})"
            )


func = FunctionBuilder()
