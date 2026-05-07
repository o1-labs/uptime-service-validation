"""Component tests for the DB wrapper class in helper.py.

These tests run against a real Postgres container (via testcontainers), with
the production schema applied. They exercise actual SQL queries — catching
schema drift, query bugs, and ORM/library version regressions that pure
unit tests can't.
"""

import logging
from datetime import datetime, timedelta, timezone

from uptime_service_validation.coordinator.helper import DB


def _seed_bot_log(conn, *, batch_end_epoch: int, batch_start_epoch: int = None):
    """Insert a bot_logs row with the given epoch boundary."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_logs
              (processing_time, files_processed, file_timestamps,
               batch_start_epoch, batch_end_epoch)
            VALUES (0, 0, NOW(), %s, %s)
            RETURNING id
            """,
            (batch_start_epoch or batch_end_epoch, batch_end_epoch),
        )
        return cur.fetchone()[0]


def test_get_batch_timings_returns_latest_bot_log(postgres_conn):
    """get_batch_timings reads the row with the largest batch_end_epoch.

    Bug class: an ORDER BY drop, an off-by-one in fetchone(), or a join
    against the wrong table all surface here.
    """
    earliest = datetime(2026, 1, 1, tzinfo=timezone.utc)
    latest = datetime(2026, 1, 2, tzinfo=timezone.utc)
    _seed_bot_log(postgres_conn, batch_end_epoch=int(earliest.timestamp()))
    expected_id = _seed_bot_log(postgres_conn, batch_end_epoch=int(latest.timestamp()))

    db = DB(postgres_conn, logging)
    batch = db.get_batch_timings(timedelta(minutes=5))

    assert batch.bot_log_id == expected_id, "expected the latest row"
    assert batch.start_time == latest


def test_get_batch_timings_carries_interval(postgres_conn):
    """The Batch returned should carry the requested interval forward."""
    _seed_bot_log(postgres_conn, batch_end_epoch=int(datetime.now(timezone.utc).timestamp()))
    db = DB(postgres_conn, logging)

    batch_5min = db.get_batch_timings(timedelta(minutes=5))
    batch_20min = db.get_batch_timings(timedelta(minutes=20))

    assert batch_5min.end_time - batch_5min.start_time == timedelta(minutes=5)
    assert batch_20min.end_time - batch_20min.start_time == timedelta(minutes=20)
