import pytest

from iceaxe.base import TableBase
from iceaxe.queries import select
from iceaxe.schemas.cli import create_all
from iceaxe.session import DBConnection


class DemoCustomModel(TableBase):
    value: str


@pytest.mark.asyncio
async def test_create_all(db_connection: DBConnection):
    # Drop the existing table if it already exists
    await db_connection.conn.fetch("DROP TABLE IF EXISTS democustommodel CASCADE")

    await create_all(db_connection, models=[DemoCustomModel])

    # Now we try to insert a row into the new table
    await db_connection.insert([DemoCustomModel(value="test")])

    result = await db_connection.exec(select(DemoCustomModel))
    assert len(result) == 1
    assert result[0].value == "test"
