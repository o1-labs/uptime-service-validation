"""Online index creation for tables that are written continuously.

The uptime backend inserts into `submissions` all the time, so a plain
CREATE INDEX (which blocks writes for the whole build) is not acceptable on
production. CREATE INDEX CONCURRENTLY does not block writes, but it cannot
run inside a transaction and, if it fails, leaves an INVALID index behind
that must be dropped before retrying. `ensure_index_concurrently` handles
both.

`invoke create-database` calls it after applying create_tables.sql. That
task runs on every coordinator start (the chart's initdb initContainer), so
indexes on live tables must be created here and never in the SQL script.
"""

import time

from psycopg2 import sql

LOCK_POLL_SECONDS = 2

SUBMISSIONS_SUBMITTED_AT = {
    "name": "idx_submissions_submitted_at",
    "table": "submissions",
    "column": "submitted_at",
}


def index_state(conn, name, table=None, column=None):
    """Return None if the index doesn't exist, else "valid" or "invalid".

    The name is resolved through search_path, like CREATE/DROP INDEX do.
    If table and column are given, raise RuntimeError when an index with
    this name exists but isn't a single-column index on table (column).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.indisvalid, i.indisready, i.indrelid::regclass::text,
                   i.indnatts, a.attname
            FROM pg_index i
            LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
            WHERE i.indexrelid = to_regclass(%s)
            """,
            (name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    valid, ready, on_table, natts, first_column = row
    if table is not None and (on_table != table or natts != 1 or first_column != column):
        raise RuntimeError(
            f"Index {name} exists but is not ON {table} ({column}); "
            f"found ON {on_table} with first column {first_column} of {natts}. "
            "Refusing to touch it."
        )
    return "valid" if valid and ready else "invalid"


def _lock(cur, name, log):
    """Take a session advisory lock that serializes builds of `name`.

    Polls with pg_try_advisory_lock instead of blocking in pg_advisory_lock:
    a session waiting inside that statement holds an open transaction, and
    CREATE INDEX CONCURRENTLY in the lock holder waits for every older
    transaction to finish, so the two would deadlock.
    """
    announced = False
    while True:
        cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (name,))
        if cur.fetchone()[0]:
            return
        if not announced:
            log(f"Another session is building {name}; waiting for it to finish...")
            announced = True
        time.sleep(LOCK_POLL_SECONDS)


def ensure_index_concurrently(conn, name, table, column, log=print):
    """Create a single-column btree index without blocking writes.

    Idempotent: returns False if a valid index already exists, True if it
    was (re)created. An invalid leftover from an interrupted build is
    dropped first. Concurrent callers are serialized with an advisory lock,
    so the index is built once and the others find it valid. The connection
    must be in autocommit mode: CONCURRENTLY refuses to run inside a
    transaction block.
    """
    assert conn.autocommit, "CREATE INDEX CONCURRENTLY needs an autocommit connection"
    with conn.cursor() as cur:
        # The build can take minutes on a large table and waits for open
        # snapshots (e.g. the hourly DB dump); don't let a session or role
        # default cut it short.
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET lock_timeout = 0")
        _lock(cur, name, log)
        try:
            state = index_state(conn, name, table, column)
            if state == "valid":
                log(f"Index {name} already exists and is valid, nothing to do.")
                return False
            if state == "invalid":
                log(f"Index {name} exists but is INVALID (interrupted build), dropping it.")
                cur.execute(
                    sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(sql.Identifier(name))
                )
            log(f"Creating index {name} ON {table} ({column}) CONCURRENTLY...")
            cur.execute(
                sql.SQL("CREATE INDEX CONCURRENTLY {} ON {} ({})").format(
                    sql.Identifier(name), sql.Identifier(table), sql.Identifier(column)
                )
            )
            state = index_state(conn, name, table, column)
            if state != "valid":
                raise RuntimeError(f"Index {name} is {state} after build; drop it and retry.")
            log(f"Index {name} created and valid.")
            return True
        finally:
            cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (name,))
