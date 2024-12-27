import time
from typing import Sequence

import pytest

from iceaxe.__tests__.conf_models import UserDemo
from iceaxe.logging import CONSOLE, LOGGER
from iceaxe.session import DBConnection


def generate_test_users(count: int) -> Sequence[UserDemo]:
    """
    Generate a sequence of test users for bulk insertion.

    :param count: Number of users to generate
    :return: Sequence of UserDemo instances
    """
    return [
        UserDemo(name=f"User {i}", email=f"user{i}@example.com") for i in range(count)
    ]


@pytest.mark.asyncio
@pytest.mark.integration_tests
async def test_bulk_insert_performance(db_connection: DBConnection):
    """
    Test the performance of bulk inserting 500k records.
    """
    NUM_USERS = 500_000
    users = generate_test_users(NUM_USERS)
    LOGGER.info(f"Generated {NUM_USERS} test users")

    start_time = time.time()

    await db_connection.insert(users)

    total_time = time.time() - start_time
    records_per_second = NUM_USERS / total_time

    CONSOLE.print("\nBulk Insert Performance:")
    CONSOLE.print(f"Total time: {total_time:.2f} seconds")
    CONSOLE.print(f"Records per second: {records_per_second:.2f}")

    result = await db_connection.conn.fetchval("SELECT COUNT(*) FROM userdemo")
    assert result == NUM_USERS
