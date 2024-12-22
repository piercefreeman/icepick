from iceaxe.base import TableBase
from iceaxe.queries_str import QueryIdentifier, QueryLiteral, sql


class DemoModel(TableBase):
    field_one: str
    field_two: int


def test_query_identifier():
    """Test the QueryIdentifier class for proper SQL identifier quoting."""
    identifier = QueryIdentifier("test_field")
    assert str(identifier) == '"test_field"'

    # Test with special characters and SQL keywords
    identifier = QueryIdentifier("group")
    assert str(identifier) == '"group"'

    identifier = QueryIdentifier("test.field")
    assert str(identifier) == '"test.field"'


def test_query_literal():
    """Test the QueryLiteral class for raw SQL inclusion."""
    literal = QueryLiteral("COUNT(*)")
    assert str(literal) == "COUNT(*)"

    literal = QueryLiteral("CASE WHEN x > 0 THEN 1 ELSE 0 END")
    assert str(literal) == "CASE WHEN x > 0 THEN 1 ELSE 0 END"


def test_query_element_hashable():
    """Test that QueryElementBase subclasses are hashable and work in sets/dicts."""
    # Test set behavior
    elements = {
        QueryIdentifier("users"),
        QueryIdentifier("users"),  # Duplicate should be removed
        QueryIdentifier("posts"),
        QueryLiteral("COUNT(*)"),
        QueryLiteral("COUNT(*)"),  # Duplicate should be removed
    }
    assert len(elements) == 3
    assert QueryIdentifier("users") in elements
    assert QueryLiteral("COUNT(*)") in elements

    # Test dictionary behavior
    element_map = {
        QueryIdentifier("users"): "users table",
        QueryLiteral("COUNT(*)"): "count function",
    }
    assert element_map[QueryIdentifier("users")] == "users table"
    assert element_map[QueryLiteral("COUNT(*)")] == "count function"

    # Test hash consistency with equality
    id1 = QueryIdentifier("test")
    id2 = QueryIdentifier("test")
    assert id1 == id2
    assert hash(id1) == hash(id2)

    lit1 = QueryLiteral("COUNT(*)")
    lit2 = QueryLiteral("COUNT(*)")
    assert lit1 == lit2
    assert hash(lit1) == hash(lit2)


def test_query_element_sortable():
    """Test that QueryElementBase subclasses can be sorted."""
    # Test sorting of identifiers
    identifiers = [
        QueryIdentifier("users"),
        QueryIdentifier("posts"),
        QueryIdentifier("comments"),
    ]
    sorted_identifiers = sorted(identifiers)
    assert [str(literal) for literal in sorted_identifiers] == [
        '"comments"',
        '"posts"',
        '"users"',
    ]

    # Test sorting of literals
    literals = [
        QueryLiteral("SUM(*)"),
        QueryLiteral("COUNT(*)"),
        QueryLiteral("AVG(*)"),
    ]
    sorted_literals = sorted(literals)
    assert [str(literal) for literal in sorted_literals] == [
        "AVG(*)",
        "COUNT(*)",
        "SUM(*)",
    ]

    # Test sorting mixed elements
    mixed = [
        QueryIdentifier("users"),
        QueryLiteral("COUNT(*)"),
        QueryIdentifier("posts"),
    ]
    sorted_mixed = sorted(mixed)
    assert [str(literal) for literal in sorted_mixed] == [
        '"posts"',
        '"users"',
        "COUNT(*)",
    ]

    # Test sorting with duplicates
    with_duplicates = [
        QueryIdentifier("users"),
        QueryIdentifier("posts"),
        QueryIdentifier("users"),
        QueryIdentifier("comments"),
    ]
    sorted_with_duplicates = sorted(with_duplicates)
    assert [str(literal) for literal in sorted_with_duplicates] == [
        '"comments"',
        '"posts"',
        '"users"',
        '"users"',
    ]

    # Test reverse sorting
    reverse_sorted = sorted(identifiers, reverse=True)
    assert [str(literal) for literal in reverse_sorted] == [
        '"users"',
        '"posts"',
        '"comments"',
    ]


def test_sql_call_column():
    """Test SQLGenerator's __call__ method with a column."""
    result = sql(DemoModel.field_one)
    assert isinstance(result, QueryLiteral)
    assert str(result) == '"demomodel"."field_one"'


def test_sql_call_table():
    """Test SQLGenerator's __call__ method with a table."""
    result = sql(DemoModel)
    assert isinstance(result, QueryIdentifier)
    assert str(result) == '"demomodel"'


def test_sql_select_column():
    """Test SQLGenerator's select method with a column."""
    result = sql.select(DemoModel.field_one)
    assert isinstance(result, QueryLiteral)
    assert str(result) == '"demomodel"."field_one" AS "demomodel_field_one"'


def test_sql_select_table():
    """Test SQLGenerator's select method with a table."""
    result = sql.select(DemoModel)
    assert isinstance(result, QueryLiteral)
    assert str(result) == (
        '"demomodel"."field_one" AS "demomodel_field_one", '
        '"demomodel"."field_two" AS "demomodel_field_two"'
    )


def test_sql_raw_column():
    """Test SQLGenerator's raw method with a column."""
    result = sql.raw(DemoModel.field_one)
    assert isinstance(result, QueryIdentifier)
    assert str(result) == '"field_one"'


def test_sql_raw_table():
    """Test SQLGenerator's raw method with a table."""
    result = sql.raw(DemoModel)
    assert isinstance(result, QueryIdentifier)
    assert str(result) == '"demomodel"'
