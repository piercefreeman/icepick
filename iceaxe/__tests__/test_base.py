from typing import Generic, TypeVar

from iceaxe.base import (
    DBModelMetaclass,
    TableBase,
)
from iceaxe.queries import QueryLiteral


def test_autodetect():
    class WillAutodetect(TableBase):
        pass

    assert WillAutodetect in DBModelMetaclass.get_registry()


def test_not_autodetect():
    class WillNotAutodetect(TableBase, autodetect=False):
        pass

    assert WillNotAutodetect not in DBModelMetaclass.get_registry()


def test_not_autodetect_generic(clear_registry):
    T = TypeVar("T")

    class GenericSuperclass(TableBase, Generic[T], autodetect=False):
        value: T

    class WillAutodetect(GenericSuperclass[int]):
        pass

    assert DBModelMetaclass.get_registry() == [WillAutodetect]


def test_select_fields():
    class TestModel(TableBase):
        field_one: str
        field_two: int

    select_fields = TestModel.select_fields()

    # The select_fields property should return a QueryLiteral that formats fields as:
    # "{table_name}.{field_name} as {table_name}_{field_name}"
    assert isinstance(select_fields, QueryLiteral)
    assert (
        str(select_fields)
        == '"testmodel"."field_one" as "testmodel_field_one", "testmodel"."field_two" as "testmodel_field_two"'
    )
