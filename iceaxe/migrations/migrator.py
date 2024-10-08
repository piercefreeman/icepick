from typing import cast

from iceaxe.logging import LOGGER
from iceaxe.migrations.actions import DatabaseActions
from iceaxe.session import DBConnection


class Migrator:
    """
    Main interface for client migrations. Mountaineer provides a simple shim on top of
    common database migration options within `migrator.actor`. This lets you add columns,
    drop columns, migrate types, and the like. For more complex migrations, you can use
    the `migrator.db_session` to run raw SQL queries within the current migration transaction.

    """

    def __init__(self, db_connection: DBConnection):
        self.actor = DatabaseActions(dry_run=False, db_connection=db_connection)
        self.db_connection = db_connection

    async def init_db(self):
        """
        Initialize our migration management table if it doesn't already exist
        within the attached postgres database. This will be a no-op if the table
        already exists.

        Client callers should call this method once before running any migrations.

        """
        # Create the table if it doesn't exist
        await self.db_connection.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_info (
                active_revision VARCHAR(255)
            )
        """
        )

        # Check if the table is empty and insert a default value if necessary
        rows = await self.db_connection.conn.fetch(
            "SELECT COUNT(*) AS migration_count FROM migration_info"
        )
        count = rows[0]["migration_count"] if rows else 0
        if count == 0:
            await self.db_connection.conn.execute(
                "INSERT INTO migration_info (active_revision) VALUES (NULL)"
            )

    async def set_active_revision(self, value: str | None):
        LOGGER.info(f"Setting active revision to {value}")

        query = """
            UPDATE migration_info SET active_revision = $1
        """

        await self.db_connection.conn.execute(query, value)

        LOGGER.info("Active revision set")

    async def get_active_revision(self) -> str | None:
        query = """
            SELECT active_revision FROM migration_info
        """

        result = await self.db_connection.conn.fetch(query)
        return cast(str | None, result[0]["active_revision"] if result else None)
