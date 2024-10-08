from dataclasses import dataclass
from enum import StrEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Concatenate,
    ParamSpec,
    Type,
    Unpack,
    cast,
    dataclass_transform,
)

from pydantic import BaseModel, Field as PydanticField
from pydantic.fields import FieldInfo, _FieldInfoInputs
from pydantic.main import _model_construction
from pydantic_core import PydanticUndefined

from iceaxe.postgres import PostgresFieldBase

P = ParamSpec("P")


class DBFieldInputs(_FieldInfoInputs, total=False):
    primary_key: bool
    autoincrement: bool
    postgres_config: PostgresFieldBase | None
    foreign_key: str | None


class DBFieldInfo(FieldInfo):
    primary_key: bool = False

    # If the field is a primary key and has no default, it should autoincrement
    autoincrement: bool = False

    # Polymorphic customization of postgres parameters depending on the field type
    postgres_config: PostgresFieldBase | None = None

    foreign_key: str | None = None

    def __init__(self, **kwargs: Unpack[DBFieldInputs]):
        # The super call should persist all kwargs as _attributes_set
        # We're intentionally passing kwargs that we know aren't in the
        # base typehinted dict
        super().__init__(**kwargs)  # type: ignore
        self.primary_key = kwargs.pop("primary_key", False)
        self.autoincrement = kwargs.pop(
            "autoincrement", (self.primary_key and self.default is None)
        )
        self.postgres_config = kwargs.pop("postgres_config", None)
        self.foreign_key = kwargs.pop("foreign_key", None)

    @classmethod
    def extend_field(
        cls,
        field: FieldInfo,
        primary_key: bool,
        postgres_config: PostgresFieldBase | None,
        foreign_key: str | None,
    ):
        return cls(
            primary_key=primary_key,
            postgres_config=postgres_config,
            foreign_key=foreign_key,
            **field._attributes_set,  # type: ignore
        )


def __get_db_field(_: Callable[Concatenate[Any, P], Any] = PydanticField):  # type: ignore
    """
    Workaround constructor to pass typehints through our function subclass
    to the original PydanticField constructor

    """

    def func(
        primary_key: bool = False,
        postgres_config: PostgresFieldBase | None = None,
        foreign_key: str | None = None,
        default: Any = PydanticUndefined,
        *args: P.args,
        **kwargs: P.kwargs,
    ):
        raw_field = PydanticField(default=default, **kwargs)  # type: ignore

        # The Any request is required for us to be able to assign fields to any
        # arbitrary type, like `value: str = Field()`
        return cast(
            Any,
            DBFieldInfo.extend_field(
                raw_field,
                primary_key=primary_key,
                postgres_config=postgres_config,
                foreign_key=foreign_key,
            ),
        )

    return func


Field = __get_db_field()


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


@dataclass
class DBFieldClassDefinition:
    root_model: Type["TableBase"]
    key: str
    field_definition: FieldInfo

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
        return DBFieldClassComparison(left=self, comparison=comparison, right=other)


@dataclass
class DBFieldClassComparison:
    left: DBFieldClassDefinition
    comparison: ComparisonType
    right: DBFieldClassDefinition | Any


@dataclass_transform(kw_only_default=True, field_specifiers=(PydanticField,))
class DBModelMetaclass(_model_construction.ModelMetaclass):
    if not TYPE_CHECKING:

        def __new__(
            mcs, name: str, bases: tuple, namespace: dict[str, Any], **kwargs: Any
        ) -> type:
            mcs.is_constructing = True
            cls = super().__new__(mcs, name, bases, namespace, **kwargs)
            mcs.is_constructing = False

            # If we have already set the class's fields, we should wrap them
            if hasattr(cls, "model_fields"):
                cls.model_fields = {
                    field: info
                    if isinstance(info, DBFieldInfo)
                    else DBFieldInfo.extend_field(
                        info,
                        primary_key=False,
                        postgres_config=None,
                        foreign_key=None,
                    )
                    for field, info in cls.model_fields.items()
                }

            return cls

        def __getattr__(self, key: str) -> Any:
            # Inspired by the approach in our render logic
            # https://github.com/piercefreeman/mountaineer/blob/fdda3a58c0fafebb43a58b4f3d410dbf44302fd6/mountaineer/render.py#L252
            if self.is_constructing:
                return super().__getattr__(key)

            try:
                return super().__getattr__(key)
            except AttributeError:
                # Determine if this field is defined within the spec
                # If so, return it
                if key in self.model_fields:
                    return DBFieldClassDefinition(
                        root_model=self,
                        key=key,
                        field_definition=self.model_fields[key],
                    )
                raise


class TableBase(BaseModel, metaclass=DBModelMetaclass):
    if TYPE_CHECKING:
        model_fields: ClassVar[dict[str, DBFieldInfo]]  # type: ignore

    table_name: ClassVar[str] = PydanticUndefined  # type: ignore
    modified_attrs: dict[str, Any] = Field(default_factory=dict, exclude=True)

    def __setattr__(self, name, value):
        if name in self.model_fields:
            self.modified_attrs[name] = value
        super().__setattr__(name, value)

    def get_modified_attributes(self) -> dict[str, Any]:
        return self.modified_attrs

    def clear_modified_attributes(self):
        self.modified_attrs.clear()

    @classmethod
    def get_table_name(cls):
        if cls.table_name == PydanticUndefined:
            return cls.__name__.lower()
        return cls.table_name
