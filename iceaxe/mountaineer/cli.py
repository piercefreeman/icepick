"""
Alternative entrypoints to migrations/cli that use Mountaineer configurations
to simplify the setup of the database connection.

"""

from mountaineer import ConfigBase, CoreDependencies, Depends
from mountaineer.dependencies import get_function_dependencies

from iceaxe.migrations.cli import handle_apply, handle_generate, handle_rollback
from iceaxe.mountaineer.config import DatabaseConfig
from iceaxe.mountaineer.dependencies.core import get_db_connection
from iceaxe.session import DBConnection


async def generate_migration(message: str | None = None):
    async def _inner(
        db_config: DatabaseConfig = Depends(
            CoreDependencies.get_config_with_type(DatabaseConfig)
        ),
        core_config: ConfigBase = Depends(
            CoreDependencies.get_config_with_type(ConfigBase)
        ),
        db_connection: DBConnection = Depends(get_db_connection),
    ):
        if not core_config.PACKAGE:
            raise ValueError("No package provided in the configuration")

        await handle_generate(
            package=core_config.PACKAGE,
            db_connection=db_connection,
            message=message,
        )

    async with get_function_dependencies(callable=_inner) as values:
        await _inner(**values)


async def apply_migration():
    async def _inner(
        core_config: ConfigBase = Depends(
            CoreDependencies.get_config_with_type(ConfigBase)
        ),
        db_connection: DBConnection = Depends(get_db_connection),
    ):
        if not core_config.PACKAGE:
            raise ValueError("No package provided in the configuration")

        await handle_apply(
            package=core_config.PACKAGE,
            db_connection=db_connection,
        )

    async with get_function_dependencies(callable=_inner) as values:
        await _inner(**values)


async def rollback_migration():
    async def _inner(
        core_config: ConfigBase = Depends(
            CoreDependencies.get_config_with_type(ConfigBase)
        ),
        db_connection: DBConnection = Depends(get_db_connection),
    ):
        if not core_config.PACKAGE:
            raise ValueError("No package provided in the configuration")

        await handle_rollback(
            package=core_config.PACKAGE,
            db_connection=db_connection,
        )

    async with get_function_dependencies(callable=_inner) as values:
        await _inner(**values)
