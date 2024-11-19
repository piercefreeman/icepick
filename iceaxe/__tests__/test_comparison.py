from re import compile as re_compile
from typing import Any

import pytest
from typing_extensions import assert_type

from iceaxe.__tests__.helpers import pyright_raises
from iceaxe.base import TableBase
from iceaxe.comparison import ComparisonType, FieldComparison
from iceaxe.field import DBFieldClassDefinition, DBFieldInfo
from iceaxe.typing import column


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
    assert ComparisonType.NOT_LIKE == "NOT LIKE"
    assert ComparisonType.ILIKE == "ILIKE"
    assert ComparisonType.NOT_ILIKE == "NOT ILIKE"
    assert ComparisonType.IS == "IS"
    assert ComparisonType.IS_NOT == "IS NOT"


@pytest.fixture
def db_field():
    return DBFieldClassDefinition(
        root_model=TableBase, key="test_key", field_definition=DBFieldInfo()
    )


def test_eq(db_field: DBFieldClassDefinition):
    result = db_field == 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.EQ
    assert result.right == 5


def test_eq_none(db_field: DBFieldClassDefinition):
    result = db_field == None  # noqa: E711
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IS
    assert result.right is None


def test_ne(db_field: DBFieldClassDefinition):
    result = db_field != 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NE
    assert result.right == 5


def test_ne_none(db_field: DBFieldClassDefinition):
    result = db_field != None  # noqa: E711
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IS_NOT
    assert result.right is None


def test_lt(db_field: DBFieldClassDefinition):
    result = db_field < 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LT
    assert result.right == 5


def test_le(db_field):
    result = db_field <= 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LE
    assert result.right == 5


def test_gt(db_field: DBFieldClassDefinition):
    result = db_field > 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GT
    assert result.right == 5


def test_ge(db_field: DBFieldClassDefinition):
    result = db_field >= 5
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.GE
    assert result.right == 5


def test_in(db_field: DBFieldClassDefinition):
    result = db_field.in_([1, 2, 3])
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.IN
    assert result.right == [1, 2, 3]


def test_not_in(db_field: DBFieldClassDefinition):
    result = db_field.not_in([1, 2, 3])
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.NOT_IN
    assert result.right == [1, 2, 3]


def test_contains(db_field: DBFieldClassDefinition):
    result = db_field.like("test")
    assert isinstance(result, FieldComparison)
    assert result.left == db_field
    assert result.comparison == ComparisonType.LIKE
    assert result.right == "test"


def test_compare(db_field: DBFieldClassDefinition):
    result = db_field._compare(ComparisonType.EQ, 10)
    assert isinstance(result, FieldComparison)
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
            root_model=TableBase, key="other_key", field_definition=DBFieldInfo()
        ),
    ],
)
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
        assert isinstance(result, FieldComparison)
        assert result.left == db_field
        assert isinstance(result.comparison, ComparisonType)
        assert result.right == value


#
# Typehinting
# These checks are run as part of the static typechecking we do
# for our codebase, not as part of the pytest runtime.
#


def test_typehint_like():
    class UserDemo(TableBase):
        id: int
        value_str: str
        value_int: int

    str_col = column(UserDemo.value_str)
    int_col = column(UserDemo.value_int)

    assert_type(str_col, DBFieldClassDefinition[str])
    assert_type(int_col, DBFieldClassDefinition[int])

    assert_type(str_col.ilike("test"), bool)
    assert_type(str_col.not_ilike("test"), bool)
    assert_type(str_col.like("test"), bool)
    assert_type(str_col.not_like("test"), bool)

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.ilike(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.not_ilike(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.like(5)  # type: ignore

    with pyright_raises(
        "reportAttributeAccessIssue",
        matches=re_compile('Cannot access attribute "ilike"'),
    ):
        int_col.not_like(5)  # type: ignore


def test_typehint_in():
    class UserDemo(TableBase):
        id: int
        value_str: str
        value_int: int

    str_col = column(UserDemo.value_str)
    int_col = column(UserDemo.value_int)

    assert_type(str_col.in_(["test"]), bool)
    assert_type(int_col.in_([5]), bool)

    assert_type(str_col.not_in(["test"]), bool)
    assert_type(int_col.not_in([5]), bool)

    with pyright_raises(
        "reportArgumentType",
        matches=re_compile('cannot be assigned to parameter "other"'),
    ):
        str_col.in_(["test", 5])  # type: ignore

    with pyright_raises(
        "reportArgumentType",
        matches=re_compile('cannot be assigned to parameter "other"'),
    ):
        str_col.not_in(["test", 5])  # type: ignore
