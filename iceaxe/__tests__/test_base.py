from typing import Any, Generic, TypeVar

import pytest

from iceaxe.base import (
    ComparisonType,
    DBFieldClassComparison,
    DBFieldClassDefinition,
    DBModelMetaclass,
    FieldInfo,
    TableBase,
)


@pytest.fixture
def db_field():
    return DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=FieldInfo()
    )


def test_eq(db_field: DBFieldClassDefinition):
    result = db_field == 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.right == 5


def test_ne(db_field: DBFieldClassDefinition):
    result = db_field != 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NE
    assert result.right == 5


def test_lt(db_field: DBFieldClassDefinition):
    result = db_field < 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LT
    assert result.right == 5


def test_le(db_field):
    result = db_field <= 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LE
    assert result.right == 5


def test_gt(db_field: DBFieldClassDefinition):
    result = db_field > 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GT
    assert result.right == 5


def test_ge(db_field: DBFieldClassDefinition):
    result = db_field >= 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GE
    assert result.right == 5


def test_in(db_field: DBFieldClassDefinition):
    result = db_field.in_([1, 2, 3])
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IN
    assert result.right == [1, 2, 3]


def test_not_in(db_field: DBFieldClassDefinition):
    result = db_field.not_in([1, 2, 3])
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NOT_IN
    assert result.right == [1, 2, 3]


def test_contains(db_field: DBFieldClassDefinition):
    result = db_field.like("test")
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LIKE
    assert result.right == "test"


def test_compare(db_field: DBFieldClassDefinition):
    result = db_field._compare(ComparisonType.EQ, 10)
    assert isinstance(result, DBFieldClassComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.right == 10


@pytest.mark.parametrize(
    "value",
    [
        None,
        "",
        0,
        [],
        {},
        True,
        False,
        3.14,
        complex(1, 2),
        DBFieldClassDefinition(
            root_model=TableBase, key="other_key", field_definition=FieldInfo()
        ),
    ],
)
# Test case for _compare method
def test_comparison_with_different_types(db_field: DBFieldClassDefinition, value: Any):
    for method in [
        db_field.__eq__,
        db_field.__ne__,
        db_field.__lt__,
        db_field.__le__,
        db_field.__gt__,
        db_field.__ge__,
        db_field.in_,
        db_field.not_in,
        db_field.like,
    ]:
        result = method(value)
        assert isinstance(result, DBFieldClassComparison)
        assert result.left == db_field
        assert isinstance(result.comparison, ComparisonType)
        assert result.right == value


def test_db_field_class_comparison_instantiation(db_field: DBFieldClassDefinition):
    comparison = DBFieldClassComparison(
        left=db_field, comparison=ComparisonType.EQ, right=5
    )
    assert comparison.left == db_field
    assert comparison.comparison == ComparisonType.EQ
    assert comparison.right == 5


def test_comparison_type_enum():
    assert ComparisonType.EQ == "="
    assert ComparisonType.NE == "!="
    assert ComparisonType.LT == "<"
    assert ComparisonType.LE == "<="
    assert ComparisonType.GT == ">"
    assert ComparisonType.GE == ">="
    assert ComparisonType.IN == "IN"
    assert ComparisonType.NOT_IN == "NOT IN"
    assert ComparisonType.LIKE == "LIKE"


def test_db_field_class_definition_instantiation():
    field_def = DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=FieldInfo()
    )
    assert field_def.root_model == TableBase
    assert field_def.key == "test_key"
    assert isinstance(field_def.field_definition, FieldInfo)


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
