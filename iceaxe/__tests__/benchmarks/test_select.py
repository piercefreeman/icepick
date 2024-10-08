from time import monotonic_ns
from typing import Any

import asyncpg
import pytest

from iceaxe.__tests__.conf_models import UserDemo, run_profile
from iceaxe.logging import LOGGER
from iceaxe.queries import QueryBuilder
from iceaxe.session import DBConnection


async def insert_users(conn: asyncpg.Connection, num_users: int):
    users = [(f"User {i}", f"user{i}@example.com") for i in range(num_users)]
    await conn.executemany("INSERT INTO userdemo (name, email) VALUES ($1, $2)", users)


async def fetch_users_raw(conn: asyncpg.Connection) -> list[Any]:
    return await conn.fetch("SELECT * FROM userdemo")  # type: ignore


@pytest.mark.asyncio
@pytest.mark.integration_tests
async def test_benchmark(db_connection: DBConnection, request):
    num_users = 500_000
    num_loops = 10

    # Insert users using raw asyncpg
    await insert_users(db_connection.conn, num_users)

    # Benchmark raw asyncpg query
    start_time = monotonic_ns()
    raw_results: list[Any] = []
    for _ in range(num_loops):
        raw_results = await fetch_users_raw(db_connection.conn)
    raw_time = monotonic_ns() - start_time
    LOGGER.info(f"Raw asyncpg query time: {raw_time / 1e9:.4f} seconds")

    # Benchmark DBConnection.exec query
    start_time = monotonic_ns()
    query = QueryBuilder().select(UserDemo.id)
    db_results: list[int] = []
    for _ in range(num_loops):
        db_results = await db_connection.exec(query)
    db_time = monotonic_ns() - start_time
    LOGGER.info(f"DBConnection.exec query time: {db_time / 1e9:.4f} seconds")

    # Slower than the raw run since we need to run the performance instrumentation
    with run_profile(request):
        # Right now we don't cache results so we can run multiple times to get a better measure of samples
        for _ in range(num_loops):
            query = QueryBuilder().select(UserDemo.id)
            db_results = await db_connection.exec(query)

    # Compare results
    assert len(raw_results) == len(db_results) == num_users, "Result count mismatch"

    # Calculate performance difference
    performance_diff = (db_time - raw_time) / raw_time * 100
    LOGGER.info(f"Performance difference: {performance_diff:.2f}%")

    # Assert that DBConnection.exec is at most 5% slower than raw query
    # assert (
    #    performance_diff <= 5
    # ), f"DBConnection.exec is {performance_diff:.2f}% slower than raw query, which exceeds the 5% threshold"

    LOGGER.info("Benchmark completed successfully.")
