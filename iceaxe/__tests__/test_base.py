import pytest

from envelope.db.base import (
    ComparisonType,
    DBFieldClassComparison,
    DBFieldClassDefinition,
    FieldInfo,
    TableBase,
)


# Fixture for creating a DBFieldClassDefinition instance
@pytest.fixture
def db_field():
    return DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=FieldInfo()
    )


# Test cases for comparison methods
def test_eq(db_field):
    result = db_field == 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.value == 5


def test_ne(db_field):
    result = db_field != 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.NE
    assert result.value == 5


def test_lt(db_field):
    result = db_field < 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.LT
    assert result.value == 5


def test_le(db_field):
    result = db_field <= 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.LE
    assert result.value == 5


def test_gt(db_field):
    result = db_field > 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.GT
    assert result.value == 5


def test_ge(db_field):
    result = db_field >= 5
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.GE
    assert result.value == 5


def test_in(db_field):
    result = db_field.in_([1, 2, 3])
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.IN
    assert result.value == [1, 2, 3]


def test_not_in(db_field):
    result = db_field.not_in([1, 2, 3])
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.NOT_IN
    assert result.value == [1, 2, 3]


def test_contains(db_field):
    result = db_field.contains("test")
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.CONTAINS
    assert result.value == "test"


# Test case for _compare method
def test_compare(db_field):
    result = db_field._compare(ComparisonType.EQ, 10)
    assert isinstance(result, DBFieldClassComparison)
    assert result.field == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.value == 10


# Test cases for edge cases and different types
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
def test_comparison_with_different_types(db_field, value):
    for method in [
        db_field.__eq__,
        db_field.__ne__,
        db_field.__lt__,
        db_field.__le__,
        db_field.__gt__,
        db_field.__ge__,
        db_field.in_,
        db_field.not_in,
        db_field.contains,
    ]:
        result = method(value)
        assert isinstance(result, DBFieldClassComparison)
        assert result.field == db_field
        assert isinstance(result.comparison, ComparisonType)
        assert result.value == value


# Test case for DBFieldClassComparison instantiation
def test_db_field_class_comparison_instantiation(db_field):
    comparison = DBFieldClassComparison(
        field=db_field, comparison=ComparisonType.EQ, value=5
    )
    assert comparison.field == db_field
    assert comparison.comparison == ComparisonType.EQ
    assert comparison.value == 5


# Test case for ComparisonType enum
def test_comparison_type_enum():
    assert ComparisonType.EQ == "="
    assert ComparisonType.NE == "!="
    assert ComparisonType.LT == "<"
    assert ComparisonType.LE == "<="
    assert ComparisonType.GT == ">"
    assert ComparisonType.GE == ">="
    assert ComparisonType.IN == "IN"
    assert ComparisonType.NOT_IN == "NOT IN"
    assert ComparisonType.CONTAINS == "CONTAINS"


# Test case for DBFieldClassDefinition instantiation
def test_db_field_class_definition_instantiation():
    field_def = DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=FieldInfo()
    )
    assert field_def.root_model == TableBase
    assert field_def.key == "test_key"
    assert isinstance(field_def.field_definition, FieldInfo)