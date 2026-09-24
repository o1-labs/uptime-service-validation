"""Manual score corrections for the Delegation Program.

Two kinds of incident need correcting by hand:

* Batches in which block producers could not earn a point through no fault
  of their own, e.g. a hard-fork transition. `exclude_batches` takes them
  out of the score entirely: out of the denominator (files_processed = -1,
  which update_scoreboard already skips) and out of the numerator (their
  points_summary rows are removed, otherwise BPs that did score in them
  would go above 100%).
* Submissions that a verifier bug rejected. `credit_rejected_submissions`
  gives the point for every batch in which a BP had a submission rejected
  with a given validation error and got no point otherwise. The credit is a
  regular points row, so the existing trigger fills points_summary.

Each operation runs in one transaction and prints the effect on every BP's
score (computed like DB.update_scoreboard). It commits only with
apply=True, so a dry run shows exactly what an apply does. Every change is
recorded in score_corrections and can be undone with `revert_correction`.

The coordinator writes the new scores to nodes and score_history at its
next batch. Rows already in score_history are not rewritten.
"""

from datetime import datetime, timezone

AUDIT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS score_corrections (
    id SERIAL PRIMARY KEY,
    correction TEXT NOT NULL,       -- label shared by all rows of one correction
    action TEXT NOT NULL,           -- exclude_batch | remove_point | credit_point
    bot_log_id INT NOT NULL,
    node_id INT,                    -- remove_point, credit_point
    point_id INT,                   -- credit_point: the inserted points row
    old_files_processed INT,        -- exclude_batch: value before the change
    reason TEXT,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reverted_at TIMESTAMPTZ
);
-- No foreign keys on purpose: cleanup_old_data() must still be able to
-- delete old batches.
CREATE INDEX IF NOT EXISTS idx_score_corrections_correction ON score_corrections (correction);
"""

# Same window and formula as DB.update_scoreboard, anchored at the latest batch.
SCORES_SQL = """
WITH w AS (SELECT max(batch_end_epoch) AS end_epoch FROM bot_logs),
win AS (
    SELECT b.id, b.files_processed FROM bot_logs b, w
    WHERE b.batch_start_epoch >= w.end_epoch - %(days)s * 86400
      AND b.batch_end_epoch <= w.end_epoch
),
surveys AS (SELECT count(*) AS n FROM win WHERE files_processed > -1),
pts AS (
    SELECT ps.node_id, count(*) AS p
    FROM points_summary ps JOIN win ON win.id = ps.bot_log_id
    GROUP BY 1
)
SELECT n.block_producer_key, pts.p, surveys.n
FROM pts JOIN nodes n ON n.id = pts.node_id, surveys
"""


def parse_utc(value):
    """Parse an ISO-8601 timestamp that must carry a timezone."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(
            f"timestamp {value!r} has no timezone; use e.g. 2026-09-03T15:00:00Z"
        )
    return dt.astimezone(timezone.utc)


def score_percent(points, surveys):
    """trunc(points * 100 / surveys, 2), as in update_scoreboard."""
    if not surveys:
        return 0.0
    return (points * 10000 // surveys) / 100


def current_scores(cur, days=90):
    """Return {block_producer_key: (points, surveys)} for the score window."""
    cur.execute(SCORES_SQL, {"days": days})
    return {key: (p, n) for key, p, n in cur.fetchall()}


def report(before, after, log=print):
    """Print the score change of every BP whose score changes."""
    surveys_before = next(iter(before.values()), (0, 0))[1]
    surveys_after = next(iter(after.values()), (0, 0))[1]
    log(f"Batches in the score window: {surveys_before} -> {surveys_after}")

    def pct(scores, key):
        p, n = scores.get(key, (0, surveys_after))
        return score_percent(p, n)

    keys = set(before) | set(after)

    def points(scores, key):
        return scores.get(key, (0, 0))[0]

    changed = sorted(
        (k for k in keys if pct(before, k) != pct(after, k) or points(before, k) != points(after, k)),
        key=lambda k: (-pct(after, k), k),
    )
    for threshold in (99, 95, 90):
        b = sum(1 for k in keys if pct(before, k) >= threshold)
        a = sum(1 for k in keys if pct(after, k) >= threshold)
        log(f"BPs at >= {threshold}%: {b} -> {a}")
    log(f"BPs whose score changes: {len(changed)} of {len(keys)}")
    log(f"{'block_producer_key':<56} {'points':>13} {'score %':>17}")
    for k in changed:
        pb, pa = points(before, k), points(after, k)
        log(f"{k:<56} {pb:>6}->{pa:<6} {pct(before, k):>7.2f}->{pct(after, k):<7.2f}")
    return changed


def _run(conn, apply, days, log, change):
    """Run `change(cur)` in one transaction between two score snapshots.

    Commits only if apply is True; otherwise rolls everything back,
    including the audit table creation. The connection's autocommit setting
    is restored afterwards.
    """
    was_autocommit = conn.autocommit
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # Don't queue forever behind the coordinator's batch transaction.
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute("SET LOCAL statement_timeout = 0")
            cur.execute(AUDIT_TABLE_SQL)
            before = current_scores(cur, days)
            summary = change(cur)
            after = current_scores(cur, days)
            report(before, after, log)
        if apply:
            conn.commit()
            log("APPLIED. The coordinator publishes the new scores at its next batch.")
        else:
            conn.rollback()
            log("DRY RUN: nothing was changed. Re-run with --apply to commit.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = was_autocommit
    summary["before"], summary["after"] = before, after
    return summary


def exclude_batches(conn, correction, start, end, reason, apply=False, days=90, log=print):
    """Remove every batch that lies completely inside [start, end) from the score."""
    start, end = parse_utc(start), parse_utc(end)
    params = {
        "c": correction,
        "r": reason,
        "s": int(start.timestamp()),
        "e": int(end.timestamp()),
    }

    def change(cur):
        cur.execute(
            """
            CREATE TEMP TABLE _batches ON COMMIT DROP AS
            SELECT id, files_processed, batch_start_epoch, batch_end_epoch FROM bot_logs
            WHERE batch_start_epoch >= %(s)s AND batch_end_epoch <= %(e)s
              AND files_processed > -1
            """,
            params,
        )
        cur.execute(
            """
            SELECT count(*), to_timestamp(min(batch_start_epoch)), to_timestamp(max(batch_end_epoch))
            FROM _batches
            """
        )
        n_batches, first, last = cur.fetchone()
        log(f"[{correction}] excluding {n_batches} batches ({first} .. {last})")
        # Batches are contiguous, not aligned to the clock: the ones that
        # cross --start or --end are kept. Say so, instead of silently.
        cur.execute(
            """
            SELECT to_timestamp(batch_start_epoch), to_timestamp(batch_end_epoch)
            FROM bot_logs
            WHERE batch_start_epoch < %(e)s AND batch_end_epoch > %(s)s
              AND NOT (batch_start_epoch >= %(s)s AND batch_end_epoch <= %(e)s)
              AND files_processed > -1
            ORDER BY batch_start_epoch
            """,
            params,
        )
        for b_start, b_end in cur.fetchall():
            log(f"[{correction}] WARNING: batch {b_start} .. {b_end} overlaps the period "
                "partially and is NOT excluded; widen --start/--end to include it")
        cur.execute(
            """
            INSERT INTO score_corrections (correction, action, bot_log_id, old_files_processed, reason)
            SELECT %(c)s, 'exclude_batch', id, files_processed, %(r)s FROM _batches
            """,
            params,
        )
        cur.execute(
            """
            INSERT INTO score_corrections (correction, action, bot_log_id, node_id, reason)
            SELECT %(c)s, 'remove_point', ps.bot_log_id, ps.node_id, %(r)s
            FROM points_summary ps JOIN _batches b ON b.id = ps.bot_log_id
            """,
            params,
        )
        cur.execute("DELETE FROM points_summary ps USING _batches b WHERE ps.bot_log_id = b.id")
        removed = cur.rowcount
        cur.execute("UPDATE bot_logs SET files_processed = -1 WHERE id IN (SELECT id FROM _batches)")
        log(f"[{correction}] removed {removed} per-batch points earned in those batches")
        return {"batches": n_batches, "removed_points": removed}

    return _run(conn, apply, days, log, change)


def credit_rejected_submissions(
    conn, correction, start, end, error, reason, apply=False, days=90, log=print,
    require_point_within_hours=None,
):
    """Credit 1 point for each (BP, batch) in [start, end) where the BP had a
    submission whose validation_error contains `error` and got no point.

    Excluded batches (files_processed = -1) are never credited.

    A rejected submission has no state hash, so the credit cannot repeat the
    "on the canonical chain" check. With require_point_within_hours=N, a
    batch is credited only if the BP earned a real point in a batch ending
    within N hours of it, as evidence that its node was on the chain then.
    """
    if not error:
        raise ValueError("error must be a non-empty substring of validation_error")
    start, end = parse_utc(start), parse_utc(end)
    params = {
        "c": correction,
        "r": reason,
        "err": error,
        "tag": f"score-correction:{correction}",
        "s": int(start.timestamp()),
        "e": int(end.timestamp()),
        # submissions.submitted_at is a UTC timestamp without time zone
        "s_ts": start.replace(tzinfo=None),
        "e_ts": end.replace(tzinfo=None),
        "near": int(require_point_within_hours * 3600) if require_point_within_hours else None,
    }

    def change(cur):
        cur.execute(
            """
            CREATE TEMP TABLE _batches ON COMMIT DROP AS
            SELECT id, batch_start_epoch, batch_end_epoch FROM bot_logs
            WHERE batch_start_epoch >= %(s)s AND batch_end_epoch <= %(e)s
              AND files_processed > -1
            """,
            params,
        )
        cur.execute("CREATE INDEX ON _batches (batch_start_epoch)")
        cur.execute("ANALYZE _batches")
        # Each rejected submission is mapped to the batch whose
        # [start, end) contains it, the same boundaries the coordinator uses.
        cur.execute(
            """
            CREATE TEMP TABLE _rejected ON COMMIT DROP AS
            SELECT s.submitter, b.id AS bot_log_id, b.batch_end_epoch
            FROM submissions s
            CROSS JOIN LATERAL (
                SELECT id, batch_end_epoch FROM _batches
                WHERE batch_start_epoch <= extract(epoch FROM s.submitted_at AT TIME ZONE 'UTC')
                ORDER BY batch_start_epoch DESC
                LIMIT 1
            ) b
            WHERE s.submitted_at >= %(s_ts)s AND s.submitted_at < %(e_ts)s
              AND position(%(err)s IN s.validation_error) > 0
              AND extract(epoch FROM s.submitted_at AT TIME ZONE 'UTC') < b.batch_end_epoch
            """,
            params,
        )
        cur.execute(
            """
            SELECT count(DISTINCT submitter) FROM _rejected r
            WHERE NOT EXISTS (SELECT 1 FROM nodes n WHERE n.block_producer_key = r.submitter)
            """
        )
        unknown = cur.fetchone()[0]
        if unknown:
            log(
                f"[{correction}] WARNING: {unknown} submitters with rejected submissions "
                "are not in nodes (never had a verified submission) and get no credit"
            )
        cur.execute(
            """
            CREATE TEMP TABLE _credits ON COMMIT DROP AS
            SELECT DISTINCT n.id AS node_id, r.bot_log_id, r.batch_end_epoch
            FROM _rejected r
            JOIN nodes n ON n.block_producer_key = r.submitter
            WHERE NOT EXISTS (
                SELECT 1 FROM points_summary ps
                WHERE ps.node_id = n.id AND ps.bot_log_id = r.bot_log_id
            )
            AND (%(near)s::bigint IS NULL OR EXISTS (
                SELECT 1 FROM bot_logs b2
                JOIN points_summary ps2 ON ps2.bot_log_id = b2.id AND ps2.node_id = n.id
                WHERE b2.batch_end_epoch BETWEEN r.batch_end_epoch - %(near)s::bigint
                                             AND r.batch_end_epoch + %(near)s::bigint
            ))
            """,
            params,
        )
        cur.execute("SELECT count(*), count(DISTINCT node_id) FROM _credits")
        n_credits, n_bps = cur.fetchone()
        guard = (f" (only with a real point within {require_point_within_hours} h)"
                 if require_point_within_hours else "")
        log(f"[{correction}] crediting {n_credits} batches to {n_bps} BPs{guard}")
        cur.execute(
            """
            WITH ins AS (
                INSERT INTO points (file_name, file_timestamps, node_id, amount, created_at, bot_log_id)
                SELECT %(tag)s, to_timestamp(c.batch_end_epoch), c.node_id, 1, NOW(), c.bot_log_id
                FROM _credits c
                RETURNING id, node_id, bot_log_id
            )
            INSERT INTO score_corrections (correction, action, bot_log_id, node_id, point_id, reason)
            SELECT %(c)s, 'credit_point', bot_log_id, node_id, id, %(r)s FROM ins
            """,
            params,
        )
        return {"credits": n_credits, "bps": n_bps}

    return _run(conn, apply, days, log, change)


def revert_correction(conn, correction, apply=False, days=90, log=print):
    """Undo every not-yet-reverted change recorded under `correction`."""
    params = {"c": correction}
    active = "sc.correction = %(c)s AND sc.reverted_at IS NULL"

    def change(cur):
        cur.execute(
            f"SELECT action, count(*) FROM score_corrections sc WHERE {active} GROUP BY 1 ORDER BY 1",
            params,
        )
        counts = dict(cur.fetchall())
        if not counts:
            raise ValueError(f"no active changes recorded for correction {correction!r}")
        log(f"[{correction}] reverting {counts}")
        cur.execute(
            f"""
            DELETE FROM points p USING score_corrections sc
            WHERE {active} AND sc.action = 'credit_point' AND p.id = sc.point_id
            """,
            params,
        )
        # (BP, batch) pairs that a points row still backs, after the credits
        # are gone. points has no index on (bot_log_id, node_id), so read it
        # once instead of probing it per pair.
        cur.execute(
            f"""
            CREATE TEMP TABLE _backed ON COMMIT DROP AS
            SELECT DISTINCT p.bot_log_id, p.node_id FROM points p
            WHERE p.bot_log_id IN (
                SELECT sc.bot_log_id FROM score_corrections sc
                WHERE {active} AND sc.action IN ('credit_point', 'remove_point')
            )
            """,
            params,
        )
        cur.execute("CREATE INDEX ON _backed (bot_log_id, node_id)")
        cur.execute("ANALYZE _backed")
        # Drop the summary row only if no real point is left for that pair.
        cur.execute(
            f"""
            DELETE FROM points_summary ps USING score_corrections sc
            WHERE {active} AND sc.action = 'credit_point'
              AND ps.bot_log_id = sc.bot_log_id AND ps.node_id = sc.node_id
              AND NOT EXISTS (
                  SELECT 1 FROM _backed k
                  WHERE k.bot_log_id = sc.bot_log_id AND k.node_id = sc.node_id
              )
            """,
            params,
        )
        cur.execute(
            f"""
            INSERT INTO points_summary (bot_log_id, node_id)
            SELECT sc.bot_log_id, sc.node_id FROM score_corrections sc
            WHERE {active} AND sc.action = 'remove_point'
              AND EXISTS (SELECT 1 FROM bot_logs b WHERE b.id = sc.bot_log_id)
              -- Only restore a summary row that a points row still backs: a
              -- credit reverted in between must not come back as a phantom.
              AND EXISTS (
                  SELECT 1 FROM _backed k
                  WHERE k.bot_log_id = sc.bot_log_id AND k.node_id = sc.node_id
              )
            ON CONFLICT (bot_log_id, node_id) DO NOTHING
            """,
            params,
        )
        cur.execute(
            f"""
            UPDATE bot_logs b SET files_processed = sc.old_files_processed
            FROM score_corrections sc
            WHERE {active} AND sc.action = 'exclude_batch' AND b.id = sc.bot_log_id
            """,
            params,
        )
        cur.execute(
            f"UPDATE score_corrections sc SET reverted_at = NOW() WHERE {active}", params
        )
        return {"reverted": counts}

    return _run(conn, apply, days, log, change)


def list_corrections(conn, log=print):
    """Print a summary of every recorded correction."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('score_corrections')")
            if cur.fetchone()[0] is None:
                log("No corrections recorded (score_corrections does not exist).")
                return []
            cur.execute(
                """
                SELECT correction, action, count(*), min(applied_at), max(reverted_at),
                       count(DISTINCT node_id), min(reason)
                FROM score_corrections GROUP BY 1, 2 ORDER BY 4, 1, 2
                """
            )
            rows = cur.fetchall()
    finally:
        # Read-only; don't leave a transaction open on the caller's connection.
        if not conn.autocommit:
            conn.rollback()
    for correction, action, n, applied, reverted, bps, reason in rows:
        state = f"reverted {reverted:%Y-%m-%d %H:%M}Z" if reverted else "active"
        log(f"{correction:<32} {action:<14} {n:>7} rows {bps:>4} BPs  "
            f"applied {applied:%Y-%m-%d %H:%M}Z  {state}  {reason or ''}")
    return rows
