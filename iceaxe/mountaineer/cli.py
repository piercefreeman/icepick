# from types import ModuleType
# from typing import overload

# from click import secho
# from fastapi import Depends
# from mountaineer.database import DatabaseDependencies, SQLModel
# from mountaineer.dependencies import get_function_dependencies
# from sqlalchemy import text
# from sqlalchemy.ext.asyncio import AsyncEngine

from mountaineer import ConfigBase, CoreDependencies, Depends
from mountaineer.dependencies import get_function_dependencies

from iceaxe.migrations.cli import handle_apply, handle_generate, handle_rollback
from iceaxe.mountaineer.config import DatabaseConfig
from iceaxe.mountaineer.dependencies.core import get_db_connection
from iceaxe.session import DBConnection

#
# Alternative entrypoints to migrations/cli that use Mountaineer configurations
# to simplify the setup of the database connection.
#


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


# @overload
# async def handle_createdb(model_module: ModuleType) -> None: ...


# @overload
# async def handle_createdb(models: list[SQLModel]) -> None: ...


# async def handle_createdb(*args, **kwargs):
#     """
#     Strictly speaking, passing a list of models isn't required for this function. We'll happily
#     accept being called with `handle_createdb()`. We just encourage an explicit passing of either
#     the models module or the SQLModels themselves to make sure they are in-scope of the table
#     registry when this function is run. This is how we determine which tables to create at runtime.

#     :param model_module: The module containing the SQLModels to create

#     :param models: An explicit list of SQLModels to create

#     """

#     async def run_migrations(
#         engine: AsyncEngine = Depends(DatabaseDependencies.get_db),
#     ):
#         async with engine.begin() as connection:
#             await connection.run_sync(SQLModel.metadata.create_all)

#             # Log the tables that were created
#             result = await connection.execute(
#                 text(
#                     """
#                 SELECT table_name
#                 FROM information_schema.tables
#                 WHERE table_schema = 'public'
#                 """
#                 )
#             )
#             tables = "\n".join([f"* {table[0]}" for table in result.fetchall()])
#             secho(f"Database tables created:\n{tables}", fg="green")

#     async with get_function_dependencies(callable=run_migrations) as values:
#         await run_migrations(**values)
