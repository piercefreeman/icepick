from abc import abstractmethod

from iceaxe.migrations.migrator import Migrator
from iceaxe.session import DBConnection


class MigrationRevisionBase:
    """
    Base class for all revisions. Both the "up" and the "down"
    also accepts all dependency injection values.

    """

    # up and down revision are both set, except for the initial revision
    # where down_revision is None
    up_revision: str
    down_revision: str | None

    async def handle_up(self, db_connection: DBConnection):
        """
        Internal method to handle the up migration.
        """
        # Isolated migrator context just for this migration
        async with db_connection.transaction():
            migrator = Migrator(db_connection)
            await self.up(migrator)
            await migrator.set_active_revision(self.up_revision)

    async def handle_down(self, db_connection: DBConnection):
        """
        Internal method to handle the down migration.
        """
        async with db_connection.transaction():
            migrator = Migrator(db_connection)
            await self.down(migrator)
            await migrator.set_active_revision(self.down_revision)

    @abstractmethod
    async def up(self, migrator: Migrator):
        """
        Perform the migration "up" action. Clients should place their
        migration logic here.

        """
        pass

    @abstractmethod
    async def down(self, migrator: Migrator):
        """
        Perform the migration "down" action. Clients should place their
        migration logic here.

        """
        pass
