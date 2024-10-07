import pytest
import pytest_asyncio

from envelope.config import AppConfig
from envelope.db.base import Field, TableBase
from envelope.db.queries import QueryBuilder
from envelope.db.session import (
    DBConnection,
    get_db_connection,
)


class UserDemo(TableBase):
    id: int = Field(primary_key=True, default=None)
    name: str
    email: str


@pytest_asyncio.fixture
async def db_connection(config: AppConfig):
    conn = await get_db_connection(config=config)
    # Create a test table
    await conn.conn.fetch("""
        CREATE TABLE IF NOT EXISTS userdemo (
            id SERIAL PRIMARY KEY,
            name TEXT,
            email TEXT
        )
    """)
    yield conn
    # Drop the test table after all tests
    await conn.conn.fetch("DROP TABLE IF EXISTS userdemo")
    await conn.conn.close()


@pytest_asyncio.fixture(autouse=True)
async def clear_table(db_connection):
    await db_connection.conn.fetch("DELETE FROM userdemo")
    await db_connection.conn.fetch("ALTER SEQUENCE userdemo_id_seq RESTART WITH 1")


@pytest.mark.asyncio
async def test_db_connection_exec(db_connection: DBConnection):
    # Insert a test user
    await db_connection.conn.fetch(
        "INSERT INTO userdemo (name, email) VALUES ($1, $2)",
        "Test User",
        "test@example.com",
    )

    # Query the database
    result = await db_connection.conn.fetch("SELECT * FROM userdemo")

    assert len(result) == 1
    assert result[0]["name"] == "Test User"
    assert result[0]["email"] == "test@example.com"


@pytest.mark.asyncio
async def test_db_connection_insert(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE name = $1", "John Doe"
    )
    assert len(result) == 1
    assert result[0]["id"] == user.id
    assert result[0]["name"] == "John Doe"
    assert result[0]["email"] == "john@example.com"
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_connection_update(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    user.name = "Jane Doe"
    await db_connection.update([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 1
    assert result[0]["name"] == "Jane Doe"
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_obj_mixin_track_modifications():
    user = UserDemo(name="John Doe", email="john@example.com")
    assert user.get_modified_attributes() == {}

    user.name = "Jane Doe"
    assert user.get_modified_attributes() == {"name": "Jane Doe"}

    user.email = "jane@example.com"
    assert user.get_modified_attributes() == {
        "name": "Jane Doe",
        "email": "jane@example.com",
    }

    user.clear_modified_attributes()
    assert user.get_modified_attributes() == {}


@pytest.mark.asyncio
async def test_db_connection_insert_multiple(db_connection: DBConnection):
    userdemo = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]

    await db_connection.insert(userdemo)

    result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY id")
    assert len(result) == 2
    assert result[0]["name"] == "John Doe"
    assert result[1]["name"] == "Jane Doe"
    assert userdemo[0].id == result[0]["id"]
    assert userdemo[1].id == result[1]["id"]
    assert all(user.get_modified_attributes() == {} for user in userdemo)


@pytest.mark.asyncio
async def test_db_connection_update_multiple(db_connection: DBConnection):
    userdemo = [
        UserDemo(name="John Doe", email="john@example.com"),
        UserDemo(name="Jane Doe", email="jane@example.com"),
    ]
    await db_connection.insert(userdemo)

    userdemo[0].name = "Johnny Doe"
    userdemo[1].email = "janey@example.com"

    await db_connection.update(userdemo)

    result = await db_connection.conn.fetch("SELECT * FROM userdemo ORDER BY id")
    assert len(result) == 2
    assert result[0]["name"] == "Johnny Doe"
    assert result[1]["email"] == "janey@example.com"
    assert all(user.get_modified_attributes() == {} for user in userdemo)


@pytest.mark.asyncio
async def test_db_connection_insert_empty_list(db_connection: DBConnection):
    await db_connection.insert([])
    result = await db_connection.conn.fetch("SELECT * FROM userdemo")
    assert len(result) == 0


@pytest.mark.asyncio
async def test_db_connection_update_empty_list(db_connection: DBConnection):
    await db_connection.update([])
    # This test doesn't really assert anything, as an empty update shouldn't change the database


@pytest.mark.asyncio
async def test_db_connection_update_no_modifications(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    await db_connection.update([user])

    result = await db_connection.conn.fetch(
        "SELECT * FROM userdemo WHERE id = $1", user.id
    )
    assert len(result) == 1
    assert result[0]["name"] == "John Doe"
    assert result[0]["email"] == "john@example.com"


@pytest.mark.asyncio
async def test_select(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    # Table selection
    result_1 = await db_connection.exec(QueryBuilder().select(UserDemo))
    assert result_1 == [UserDemo(id=user.id, name="John Doe", email="john@example.com")]

    # Single column selection
    result_2 = await db_connection.exec(QueryBuilder().select(UserDemo.email))
    assert result_2 == ["john@example.com"]

    # Multiple column selection
    result_3 = await db_connection.exec(
        QueryBuilder().select((UserDemo.name, UserDemo.email))
    )
    assert result_3 == [("John Doe", "john@example.com")]

    # Table and column selection
    result_4 = await db_connection.exec(
        QueryBuilder().select((UserDemo, UserDemo.email))
    )
    assert result_4 == [
        (
            UserDemo(id=user.id, name="John Doe", email="john@example.com"),
            "john@example.com",
        )
    ]


@pytest.mark.asyncio
async def test_select_where(db_connection: DBConnection):
    user = UserDemo(name="John Doe", email="john@example.com")
    await db_connection.insert([user])

    new_query = QueryBuilder().select(UserDemo).where(UserDemo.name == "John Doe")
    result = await db_connection.exec(new_query)
    assert result == [
        UserDemo(id=user.id, name="John Doe", email="john@example.com"),
    ]
