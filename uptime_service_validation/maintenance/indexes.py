"""Online index creation for tables that are written continuously.

The uptime backend inserts into `submissions` all the time, so a plain
CREATE INDEX (which blocks writes for the whole build) is not acceptable on
production. CREATE INDEX CONCURRENTLY does not block writes, but it cannot
run inside a transaction and, if it fails, leaves an INVALID index behind
that must be dropped before retrying. `ensure_index_concurrently` handles
both.
"""

from psycopg2 import sql

SUBMISSIONS_SUBMITTED_AT = {
    "name": "idx_submissions_submitted_at",
    "table": "submissions",
    "column": "submitted_at",
}


def index_state(conn, name):
    """Return None if the index doesn't exist, else "valid" or "invalid"."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.indisvalid
            FROM pg_class c
            JOIN pg_index i ON i.indexrelid = c.oid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = %s AND n.nspname = 'public'
            """,
            (name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return "valid" if row[0] else "invalid"


def ensure_index_concurrently(conn, name, table, column, log=print):
    """Create a single-column btree index without blocking writes.

    Idempotent: returns False if a valid index already exists, True if it
    was (re)created. An invalid leftover from an interrupted build is
    dropped first.
    """
    # CONCURRENTLY refuses to run inside a transaction block.
    conn.autocommit = True
    state = index_state(conn, name)
    if state == "valid":
        log(f"Index {name} already exists and is valid, nothing to do.")
        return False

    with conn.cursor() as cur:
        # The build can take minutes on a large table; don't let a
        # session-level default cut it short.
        cur.execute("SET statement_timeout = 0")
        if state == "invalid":
            log(f"Index {name} exists but is INVALID (interrupted build), dropping it.")
            cur.execute(
                sql.SQL("DROP INDEX CONCURRENTLY IF EXISTS {}").format(
                    sql.Identifier(name)
                )
            )
        log(f"Creating index {name} ON {table} ({column}) CONCURRENTLY...")
        cur.execute(
            sql.SQL("CREATE INDEX CONCURRENTLY {} ON {} ({})").format(
                sql.Identifier(name), sql.Identifier(table), sql.Identifier(column)
            )
        )

    state = index_state(conn, name)
    if state != "valid":
        raise RuntimeError(f"Index {name} is {state} after build; drop it and retry.")
    log(f"Index {name} created and valid.")
    return True
