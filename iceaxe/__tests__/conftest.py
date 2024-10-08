import asyncpg
import pytest_asyncio

from iceaxe.session import DBConnection


@pytest_asyncio.fixture
async def db_connection():
    conn = DBConnection(
        await asyncpg.connect(
            host="localhost",
            port=5438,
            user="iceaxe",
            password="mysecretpassword",
            database="iceaxe_test_db",
        )
    )

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
    # await conn.conn.fetch("DROP TABLE IF EXISTS userdemo")
    await conn.conn.close()


@pytest_asyncio.fixture(autouse=True)
async def clear_table(db_connection):
    await db_connection.conn.fetch("DELETE FROM userdemo")
    await db_connection.conn.fetch("ALTER SEQUENCE userdemo_id_seq RESTART WITH 1")
