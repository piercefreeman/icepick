from iceaxe.comparison import ComparisonType


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
