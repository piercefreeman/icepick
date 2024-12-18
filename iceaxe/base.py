from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Type,
    dataclass_transform,
)

from pydantic import BaseModel, Field as PydanticField
from pydantic.main import _model_construction
from pydantic_core import PydanticUndefined

from iceaxe.field import DBFieldClassDefinition, DBFieldInfo, Field
from iceaxe.queries_str import QueryIdentifier, QueryLiteral


@dataclass_transform(kw_only_default=True, field_specifiers=(PydanticField,))
class DBModelMetaclass(_model_construction.ModelMetaclass):
    _registry: list[Type["TableBase"]] = []
    # {class: kwargs}
    _cached_args: dict[Type["TableBase"], dict[str, Any]] = {}

    def __new__(
        mcs, name: str, bases: tuple, namespace: dict[str, Any], **kwargs: Any
    ) -> type:
        raw_kwargs = {**kwargs}

        mcs.is_constructing = True
        autodetect = mcs._extract_kwarg(kwargs, "autodetect", True)
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        mcs.is_constructing = False

        # Allow future calls to subclasses / generic instantiations to reference the same
        # kwargs as the base class
        mcs._cached_args[cls] = raw_kwargs

        # If we have already set the class's fields, we should wrap them
        if hasattr(cls, "__pydantic_fields__"):
            cls.__pydantic_fields__ = {
                field: info
                if isinstance(info, DBFieldInfo)
                else DBFieldInfo.extend_field(
                    info,
                    primary_key=False,
                    postgres_config=None,
                    foreign_key=None,
                    unique=False,
                    index=False,
                    check_expression=None,
                    is_json=False,
                )
                for field, info in cls.model_fields.items()
            }

        # Avoid registering HandlerBase itself
        if cls.__name__ not in {"TableBase", "BaseModel"} and autodetect:
            DBModelMetaclass._registry.append(cls)

        return cls

    def __getattr__(self, key: str) -> Any:
        # Inspired by the approach in our render logic
        # https://github.com/piercefreeman/mountaineer/blob/fdda3a58c0fafebb43a58b4f3d410dbf44302fd6/mountaineer/render.py#L252
        if self.is_constructing:
            return super().__getattr__(key)  # type: ignore

        try:
            return super().__getattr__(key)  # type: ignore
        except AttributeError:
            # Determine if this field is defined within the spec
            # If so, return it
            if key in self.model_fields:
                return DBFieldClassDefinition(
                    root_model=self,  # type: ignore
                    key=key,
                    field_definition=self.model_fields[key],
                )
            raise

    @classmethod
    def get_registry(cls):
        return cls._registry

    @classmethod
    def _extract_kwarg(cls, kwargs: dict[str, Any], key: str, default: Any = None):
        """
        Kwarg extraction that supports standard instantiation and pydantic's approach
        for Generic models where it hydrates a fully new class in memory with the type
        annotations set to generic values.

        """
        if key in kwargs:
            return kwargs.pop(key)

        if "__pydantic_generic_metadata__" in kwargs:
            origin_model = kwargs["__pydantic_generic_metadata__"]["origin"]
            if origin_model in cls._cached_args:
                return cls._cached_args[origin_model].get(key, default)

        return default

    @property
    def model_fields(self) -> dict[str, DBFieldInfo]:  # type: ignore
        # model_fields must be reimplemented in our custom metaclass, otherwise
        # clients will get the super typehinting signature when they try
        # to access Model.model_fields. This overrides the ClassVar typehint
        # that's placed in the TableBase itself.
        return super().model_fields  # type: ignore


class UniqueConstraint(BaseModel):
    columns: list[str]


class IndexConstraint(BaseModel):
    columns: list[str]


INTERNAL_TABLE_FIELDS = ["modified_attrs"]


class TableBase(BaseModel, metaclass=DBModelMetaclass):
    if TYPE_CHECKING:
        model_fields: ClassVar[dict[str, DBFieldInfo]]  # type: ignore

    table_name: ClassVar[str] = PydanticUndefined  # type: ignore
    table_args: ClassVar[list[UniqueConstraint | IndexConstraint]] = PydanticUndefined  # type: ignore

    # Private methods
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

    @classmethod
    def get_client_fields(cls):
        return {
            field: info
            for field, info in cls.model_fields.items()
            if field not in INTERNAL_TABLE_FIELDS
        }

    @classmethod
    def select_fields(cls):
        """
        Returns a query selectable string that can be used to select all fields
        from this model. This is the format that needs to be passed to our parser
        to serialize the raw postgres field values as TableBase objects.

        The exact format is formatted as:
        "{table_name}.{field_name} as {table_name}_{field_name}".

        """
        table_token = QueryIdentifier(cls.get_table_name())
        select_fields: list[str] = []
        for field_name in cls.get_client_fields():
            field_token = QueryIdentifier(field_name)
            return_field = QueryIdentifier(f"{cls.get_table_name()}_{field_name}")
            select_fields.append(f"{table_token}.{field_token} as {return_field}")
        return QueryLiteral(", ".join(select_fields))
