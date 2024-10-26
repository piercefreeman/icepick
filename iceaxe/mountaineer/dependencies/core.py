"""
Optional compatibility layer for `mountaineer` dependency access.

"""

from typing import AsyncGenerator

import asyncpg
from mountaineer import CoreDependencies, Depends

from iceaxe.mountaineer.config import DatabaseConfig
from iceaxe.session import DBConnection


async def get_db_connection(
    config: DatabaseConfig = Depends(
        CoreDependencies.get_config_with_type(DatabaseConfig)
    ),
) -> AsyncGenerator[DBConnection, None]:
    conn = await asyncpg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        database=config.POSTGRES_DB,
    )
    try:
        yield DBConnection(conn)
    finally:
        await conn.close()
