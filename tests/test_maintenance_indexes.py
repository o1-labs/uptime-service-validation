"""Component tests for the online index maintenance in maintenance/indexes.py."""

from uptime_service_validation.maintenance.indexes import (
    SUBMISSIONS_SUBMITTED_AT,
    ensure_index_concurrently,
    index_state,
)

NAME = SUBMISSIONS_SUBMITTED_AT["name"]


def _drop(conn):
    with conn.cursor() as cur:
        cur.execute(f"DROP INDEX IF EXISTS {NAME}")


def test_schema_creates_the_index(postgres_conn):
    """Fresh databases get the index from create_tables.sql."""
    assert index_state(postgres_conn, NAME) == "valid"


def test_creates_missing_index(postgres_conn):
    """An existing database without the index gets it."""
    _drop(postgres_conn)
    assert index_state(postgres_conn, NAME) is None

    assert ensure_index_concurrently(postgres_conn, **SUBMISSIONS_SUBMITTED_AT, log=lambda _: None)
    assert index_state(postgres_conn, NAME) == "valid"


def test_is_idempotent(postgres_conn):
    """A second run is a no-op."""
    assert not ensure_index_concurrently(postgres_conn, **SUBMISSIONS_SUBMITTED_AT, log=lambda _: None)
    assert index_state(postgres_conn, NAME) == "valid"


def test_rebuilds_invalid_leftover(postgres_conn):
    """An INVALID index from an interrupted CONCURRENTLY build is replaced.

    Simulated by flipping indisvalid, which is what Postgres leaves behind
    when a concurrent build fails.
    """
    with postgres_conn.cursor() as cur:
        cur.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass",
            (NAME,),
        )
    assert index_state(postgres_conn, NAME) == "invalid"

    assert ensure_index_concurrently(postgres_conn, **SUBMISSIONS_SUBMITTED_AT, log=lambda _: None)
    assert index_state(postgres_conn, NAME) == "valid"


def test_time_range_query_uses_the_index(postgres_conn):
    """The coordinator's batch query (DB.get_submissions shape) can use it."""
    with postgres_conn.cursor() as cur:
        cur.execute("SET enable_seqscan = off")
        cur.execute(
            """
            EXPLAIN SELECT * FROM submissions
            WHERE submitted_at >= '2026-01-01' AND submitted_at < '2026-01-01 00:20'
            ORDER BY submitted_at DESC
            """
        )
        plan = "\n".join(r[0] for r in cur.fetchall())
    assert NAME in plan
