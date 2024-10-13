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
    literal: QueryLiteral
    original_field: DBFieldClassDefinition
    local_name: str | None = None

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
        return self.literal, []


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
            return FunctionMetadata(literal=field.to_query()[0], original_field=field)
        else:
            raise ValueError(
                f"Unable to cast this type to a column: {field} ({type(field)})"
            )


func = FunctionBuilder()
