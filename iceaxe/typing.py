from __future__ import annotations

from datetime import date, datetime, time, timedelta
from enum import Enum, IntEnum, StrEnum
from inspect import isclass
from typing import (
    TYPE_CHECKING,
    Any,
    Type,
    TypeGuard,
)
from uuid import UUID

if TYPE_CHECKING:
    from iceaxe.base import (
        DBFieldClassComparison,
        DBFieldClassDefinition,
        TableBase,
    )
    from iceaxe.functions import FunctionMetadata, FunctionMetadataComparison
    from iceaxe.queries_str import QueryLiteral


ALL_ENUM_TYPES = Type[Enum | StrEnum | IntEnum]
PRIMITIVE_TYPES = int | float | str | bool | bytes | UUID
PRIMITIVE_WRAPPER_TYPES = list[PRIMITIVE_TYPES] | PRIMITIVE_TYPES
DATE_TYPES = datetime | date | time | timedelta
JSON_WRAPPER_FALLBACK = list[Any] | dict[Any, Any]


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


def column(obj: Any):
    if not is_column(obj):
        raise ValueError(f"Invalid column: {obj}")
    return obj
