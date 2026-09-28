"""Component tests for the online index maintenance in maintenance/indexes.py."""

import importlib.util
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import pytest

from uptime_service_validation.maintenance.indexes import (
    SUBMISSIONS_SUBMITTED_AT,
    ensure_index_concurrently,
    index_state,
)

NAME = SUBMISSIONS_SUBMITTED_AT["name"]
QUIET = {"log": lambda _: None}
REPO_ROOT = Path(__file__).resolve().parent.parent


def _dsn(container):
    return dict(
        host=container.get_container_host_ip(),
        port=str(container.get_exposed_port(5432)),
        dbname=container.dbname,
        user=container.username,
        password=container.password,
    )


def _ensure(conn):
    return ensure_index_concurrently(conn, **SUBMISSIONS_SUBMITTED_AT, **QUIET)


def test_schema_does_not_create_the_index(postgres_conn):
    """create_tables.sql runs on every coordinator start; it must not build the
    index with a plain (write-blocking) CREATE INDEX."""
    assert index_state(postgres_conn, NAME) is None


def test_creates_missing_index(postgres_conn):
    assert _ensure(postgres_conn)
    assert index_state(postgres_conn, NAME) == "valid"


def test_is_idempotent(postgres_conn):
    _ensure(postgres_conn)
    assert not _ensure(postgres_conn)
    assert index_state(postgres_conn, NAME) == "valid"


def test_rebuilds_invalid_leftover(postgres_conn):
    """An INVALID index from an interrupted CONCURRENTLY build is replaced.

    Simulated by flipping indisvalid, which is what Postgres leaves behind
    when a concurrent build fails.
    """
    _ensure(postgres_conn)
    with postgres_conn.cursor() as cur:
        cur.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass",
            (NAME,),
        )
    assert index_state(postgres_conn, NAME) == "invalid"

    assert _ensure(postgres_conn)
    assert index_state(postgres_conn, NAME) == "valid"


def test_refuses_an_index_with_the_same_name_but_another_definition(postgres_conn):
    with postgres_conn.cursor() as cur:
        cur.execute(f"CREATE INDEX {NAME} ON submissions (submitter)")
    with pytest.raises(RuntimeError, match="not ON submissions"):
        _ensure(postgres_conn)


def test_requires_an_autocommit_connection(postgres_conn):
    postgres_conn.autocommit = False
    try:
        with pytest.raises(AssertionError, match="autocommit"):
            _ensure(postgres_conn)
    finally:
        postgres_conn.rollback()
        postgres_conn.autocommit = True


def test_concurrent_runs_build_the_index_once(postgres_container, postgres_conn):
    """Two runs started at the same time: one builds, the other waits and finds it valid."""
    results, errors = [], []
    barrier = threading.Barrier(2)

    def run():
        conn = psycopg2.connect(**_dsn(postgres_container))
        conn.autocommit = True
        try:
            barrier.wait()
            results.append(_ensure(conn))
        except Exception as e:  # surfaced below
            errors.append(e)
        finally:
            conn.close()

    threads = [threading.Thread(target=run) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert sorted(results) == [False, True]
    assert index_state(postgres_conn, NAME) == "valid"


def test_create_database_task_builds_the_index_online(postgres_container, monkeypatch):
    """`invoke create-database` (coordinator initContainer, every start) ends
    with a valid index, and a second run is a no-op."""
    spec = importlib.util.spec_from_file_location("repo_tasks", REPO_ROOT / "tasks.py")
    repo_tasks = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(repo_tasks)
    dsn = _dsn(postgres_container)
    monkeypatch.chdir(REPO_ROOT)  # create_database opens create_tables.sql by relative path
    for key, env in [("host", "POSTGRES_HOST"), ("port", "POSTGRES_PORT"), ("dbname", "POSTGRES_DB"),
                     ("user", "POSTGRES_USER"), ("password", "POSTGRES_PASSWORD")]:
        monkeypatch.setenv(env, dsn[key])

    repo_tasks.create_database.body(None)
    repo_tasks.create_database.body(None)  # re-run on every pod start: must stay a no-op

    conn = psycopg2.connect(**dsn)
    try:
        assert index_state(conn, NAME) == "valid"
    finally:
        conn.close()


def _load_submissions(conn, rows=50_000):
    """~6 days of submissions, 10 s apart, 300 submitters; then ANALYZE."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO submissions (submitted_at_date, submitted_at, submitter)
            SELECT ts::date, ts, 'bp' || (g %% 300)
            FROM generate_series(1, %s) g,
                 LATERAL (SELECT timestamp '2026-01-01' + g * interval '10 seconds' AS ts) t
            """,
            (rows,),
        )
        cur.execute("ANALYZE submissions")


def _batch_plan(conn):
    """EXPLAIN of DB.get_submissions' query with the parameter types the coordinator passes."""
    start = datetime(2026, 1, 3, tzinfo=timezone.utc)  # tz-aware, like Batch times
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
        cur.execute(
            """
            EXPLAIN SELECT * FROM submissions
            WHERE submitted_at >= %s AND submitted_at < %s
            ORDER BY submitted_at DESC
            """,
            (start, start + timedelta(minutes=20)),
        )
        return "\n".join(r[0] for r in cur.fetchall())


def test_time_range_query_uses_the_index(postgres_conn):
    """With realistic data and default planner settings, the batch query uses the index."""
    _ensure(postgres_conn)
    _load_submissions(postgres_conn)
    assert NAME in _batch_plan(postgres_conn)


def test_time_range_query_seq_scans_without_the_index(postgres_conn):
    """Control: without the index the same query is a sequential scan."""
    _load_submissions(postgres_conn)
    plan = _batch_plan(postgres_conn)
    assert NAME not in plan
    assert "Seq Scan" in plan
