"""Shared fixtures for component tests.

The `postgres_db` fixture spawns a Postgres container, applies the canonical
schema (uptime_service_validation/database/create_tables.sql), and yields a
fresh psycopg2 connection. Each test gets its own container, so tests are
isolated and ordering doesn't matter.

These fixtures use testcontainers — Docker is required to run them.
"""

from pathlib import Path

import psycopg2
import pytest
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_SQL = REPO_ROOT / "uptime_service_validation" / "database" / "create_tables.sql"


@pytest.fixture
def postgres_container():
    """Spawn an ephemeral Postgres container for the duration of one test."""
    with PostgresContainer("postgres:16-alpine") as postgres:
        yield postgres


@pytest.fixture
def postgres_conn(postgres_container):
    """A psycopg2 connection to the container, with the production schema applied.

    Yields a connection in autocommit mode so tests don't need to manage
    transactions for setup writes.
    """
    conn = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        dbname=postgres_container.dbname,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL.read_text())

    try:
        yield conn
    finally:
        conn.close()
