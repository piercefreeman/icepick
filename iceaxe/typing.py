from __future__ import annotations

from inspect import isclass
from typing import TYPE_CHECKING, Any, TypeGuard

if TYPE_CHECKING:
    from iceaxe.base import (
        DBFieldClassComparison,
        DBFieldClassDefinition,
        TableBase,
    )
    from iceaxe.functions import FunctionMetadata, FunctionMetadataComparison
    from iceaxe.queries_str import QueryLiteral


def is_base_table(obj: Any) -> TypeGuard[type[TableBase]]:
    from iceaxe.base import TableBase

    return isclass(obj) and issubclass(obj, TableBase)


def is_column(obj: Any) -> TypeGuard[DBFieldClassDefinition]:
    from iceaxe.base import DBFieldClassDefinition

    return isinstance(obj, DBFieldClassDefinition)


def is_comparison(obj: Any) -> TypeGuard[DBFieldClassComparison]:
    from iceaxe.base import DBFieldClassComparison

    return isinstance(obj, DBFieldClassComparison)


def is_literal(obj: Any) -> TypeGuard[QueryLiteral]:
    from iceaxe.queries_str import QueryLiteral

    return isinstance(obj, QueryLiteral)


def is_function_metadata(obj: Any) -> TypeGuard[FunctionMetadata]:
    from iceaxe.functions import FunctionMetadata

    return isinstance(obj, FunctionMetadata)


def is_function_metadata_comparison(obj: Any) -> TypeGuard[FunctionMetadataComparison]:
    from iceaxe.functions import FunctionMetadataComparison

    return isinstance(obj, FunctionMetadataComparison)


def col(obj: Any):
    if not is_column(obj):
        raise ValueError(f"Invalid column: {obj}")
    return obj
