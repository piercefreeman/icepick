from __future__ import annotations

from inspect import isclass
from typing import Any, TypeGuard

from iceaxe.base import (
    DBFieldClassComparison,
    DBFieldClassDefinition,
    TableBase,
)
from iceaxe.functions import FunctionMetadata
from iceaxe.queries_str import QueryLiteral


def is_base_table(obj: Any) -> TypeGuard[type[TableBase]]:
    return isclass(obj) and issubclass(obj, TableBase)


def is_column(obj: Any) -> TypeGuard[DBFieldClassDefinition]:
    return isinstance(obj, DBFieldClassDefinition)


def is_comparison(obj: Any) -> TypeGuard[DBFieldClassComparison]:
    return isinstance(obj, DBFieldClassComparison)


def is_literal(obj: Any) -> TypeGuard[QueryLiteral]:
    return isinstance(obj, QueryLiteral)


def is_function_metadata(obj: Any) -> TypeGuard[FunctionMetadata]:
    return isinstance(obj, FunctionMetadata)


def col(obj: Any):
    if not is_column(obj):
        raise ValueError(f"Invalid column: {obj}")
    return obj
