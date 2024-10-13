from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    ParamSpec,
    Type,
    Unpack,
    cast,
)

from pydantic import Field as PydanticField
from pydantic.fields import FieldInfo, _FieldInfoInputs
from pydantic_core import PydanticUndefined

from iceaxe.comparison import ComparisonBase
from iceaxe.postgres import PostgresFieldBase
from iceaxe.queries_str import QueryIdentifier, QueryLiteral

if TYPE_CHECKING:
    from iceaxe.base import TableBase

P = ParamSpec("P")


class DBFieldInputs(_FieldInfoInputs, total=False):
    primary_key: bool
    autoincrement: bool
    postgres_config: PostgresFieldBase | None
    foreign_key: str | None
    unique: bool
    index: bool
    check_expression: str | None
    is_json: bool


class DBFieldInfo(FieldInfo):
    primary_key: bool = False

    # If the field is a primary key and has no default, it should autoincrement
    autoincrement: bool = False

    # Polymorphic customization of postgres parameters depending on the field type
    postgres_config: PostgresFieldBase | None = None

    # Link to another table (formatted like "table_name.column_name")
    foreign_key: str | None = None

    # Value constraints
    unique: bool = False
    index: bool = False
    check_expression: str | None = None

    is_json: bool = False

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
        self.unique = kwargs.pop("unique", False)
        self.index = kwargs.pop("index", False)
        self.check_expression = kwargs.pop("check_expression", None)
        self.is_json = kwargs.pop("is_json", False)

    @classmethod
    def extend_field(
        cls,
        field: FieldInfo,
        primary_key: bool,
        postgres_config: PostgresFieldBase | None,
        foreign_key: str | None,
        unique: bool,
        index: bool,
        check_expression: str | None,
        is_json: bool,
    ):
        return cls(
            primary_key=primary_key,
            postgres_config=postgres_config,
            foreign_key=foreign_key,
            unique=unique,
            index=index,
            check_expression=check_expression,
            is_json=is_json,
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
        unique: bool = False,
        index: bool = False,
        check_expression: str | None = None,
        is_json: bool = False,
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
                unique=unique,
                index=index,
                check_expression=check_expression,
                is_json=is_json,
            ),
        )

    return func


class DBFieldClassDefinition(ComparisonBase):
    root_model: Type["TableBase"]
    key: str
    field_definition: FieldInfo

    def __init__(
        self,
        root_model: Type["TableBase"],
        key: str,
        field_definition: FieldInfo,
    ):
        self.root_model = root_model
        self.key = key
        self.field_definition = field_definition

    def to_query(self):
        table = QueryIdentifier(self.root_model.get_table_name())
        column = QueryIdentifier(self.key)
        return QueryLiteral(f"{table}.{column}"), []


Field = __get_db_field()
