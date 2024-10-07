from iceaxe.base import Field, TableBase
from iceaxe.queries import QueryBuilder


class UserDemo(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str
    email: str


def test_select():
    new_query = QueryBuilder().select(UserDemo)
    assert new_query.build() == ('SELECT "userdemo".* FROM "userdemo"', [])


def test_select_single_field():
    new_query = QueryBuilder().select(UserDemo.email)
    assert new_query.build() == ('SELECT "userdemo"."email" FROM "userdemo"', [])


def test_where():
    new_query = QueryBuilder().select(UserDemo.id).where(UserDemo.id > 0)
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE "userdemo"."id" > $1',
        [0],
    )


def test_where_columns():
    new_query = (
        QueryBuilder().select(UserDemo.id).where(UserDemo.name == UserDemo.email)
    )
    assert new_query.build() == (
        'SELECT "userdemo"."id" FROM "userdemo" WHERE "userdemo"."name" = "userdemo"."email"',
        [],
    )
