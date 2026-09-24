"""Component tests for maintenance/score_corrections.py.

World used by every test: two BPs, four 20-minute batches.

    batch      1    2    3    4
    GOOD       pt   pt   pt   pt
    HIT        pt   pt   --   --   (3: rejected "Mismatched sok_message",
                                     4: rejected "Fail to decode block")
"""

from datetime import datetime, timedelta, timezone

import pytest

from uptime_service_validation.maintenance.score_corrections import (
    credit_rejected_submissions,
    exclude_batches,
    list_corrections,
    parse_utc,
    revert_correction,
    score_percent,
)

T0 = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
STEP = timedelta(minutes=20)
GOOD, HIT = "B62qGOOD", "B62qHIT"
SOK = "Mismatched sok_message"
QUIET = {"log": lambda _: None}


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture
def world(postgres_conn):
    ids = {}
    with postgres_conn.cursor() as cur:
        for key in (GOOD, HIT):
            cur.execute(
                "INSERT INTO nodes (block_producer_key, updated_at) VALUES (%s, NOW()) RETURNING id",
                (key,),
            )
            ids[key] = cur.fetchone()[0]
        batches = []
        for i in range(4):
            start = T0 + STEP * i
            cur.execute(
                """
                INSERT INTO bot_logs (processing_time, files_processed, file_timestamps,
                                      batch_start_epoch, batch_end_epoch)
                VALUES (0, 10, NOW(), %s, %s) RETURNING id
                """,
                (int(start.timestamp()), int((start + STEP).timestamp())),
            )
            batches.append(cur.fetchone()[0])
        for i, batch in enumerate(batches):
            earners = [GOOD] + ([HIT] if i < 2 else [])
            for key in earners:
                cur.execute(
                    "INSERT INTO points (node_id, bot_log_id, amount, created_at) VALUES (%s, %s, 1, NOW())",
                    (ids[key], batch),
                )

        def submit(batch_index, error):
            at = (T0 + STEP * batch_index + timedelta(minutes=5)).replace(tzinfo=None)
            cur.execute(
                """
                INSERT INTO submissions (submitted_at_date, submitted_at, submitter,
                                         validation_error, verified)
                VALUES (%s, %s, %s, %s, true)
                """,
                (at.date(), at, HIT, f"Transaction_snark.verify: {error}"),
            )

        submit(2, SOK)
        submit(3, "Fail to decode block")
    return {"conn": postgres_conn, "ids": ids, "batches": batches}


def summary_pairs(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT bot_log_id, node_id FROM points_summary ORDER BY 1, 2")
        return cur.fetchall()


def files_processed(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, files_processed FROM bot_logs ORDER BY id")
        return cur.fetchall()


def period():
    return iso(T0), iso(T0 + STEP * 4)


def test_parse_utc_requires_timezone():
    assert parse_utc("2026-09-03T15:00:00Z") == datetime(2026, 9, 3, 15, tzinfo=timezone.utc)
    with pytest.raises(ValueError):
        parse_utc("2026-09-03T15:00:00")


def test_score_percent_truncates_like_update_scoreboard():
    assert score_percent(6416, 6480) == 99.01
    assert score_percent(2, 3) == 66.66
    assert score_percent(1, 0) == 0.0


def test_credit_dry_run_changes_nothing(world):
    conn = world["conn"]
    before = summary_pairs(conn)

    result = credit_rejected_submissions(conn, "sok", *period(), SOK, "test", **QUIET)

    assert result["credits"] == 1
    assert result["after"][HIT][0] == result["before"][HIT][0] + 1
    assert summary_pairs(conn) == before
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass('score_corrections')")
        assert cur.fetchone()[0] is None, "dry run must roll back the audit table too"


def test_credit_only_matching_error_and_is_idempotent(world):
    conn, ids, batches = world["conn"], world["ids"], world["batches"]

    result = credit_rejected_submissions(conn, "sok", *period(), SOK, "test", apply=True, **QUIET)

    assert result == {**result, "credits": 1, "bps": 1}
    assert (batches[2], ids[HIT]) in summary_pairs(conn)
    assert (batches[3], ids[HIT]) not in summary_pairs(conn), "other errors are not credited"
    assert result["after"][HIT] == (3, 4)

    again = credit_rejected_submissions(conn, "sok", *period(), SOK, "test", apply=True, **QUIET)
    assert again["credits"] == 0


def test_credit_skips_batches_that_already_have_a_point(world):
    conn, ids, batches = world["conn"], world["ids"], world["batches"]
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO points (node_id, bot_log_id, amount, created_at) VALUES (%s, %s, 1, NOW())",
            (ids[HIT], batches[2]),
        )

    result = credit_rejected_submissions(conn, "sok", *period(), SOK, "test", apply=True, **QUIET)

    assert result["credits"] == 0


def test_exclude_removes_batch_from_both_sides_of_the_score(world):
    conn, batches = world["conn"], world["batches"]
    start, end = T0 + STEP * 3, T0 + STEP * 4

    result = exclude_batches(conn, "fork", iso(start), iso(end), "test", apply=True, **QUIET)

    assert result["batches"] == 1
    assert result["removed_points"] == 1  # GOOD's point in batch 4
    assert dict(files_processed(conn))[batches[3]] == -1
    assert result["after"][GOOD] == (3, 3), "GOOD stays at 100%, not above"
    assert result["after"][HIT] == (2, 3)


def test_credit_never_touches_excluded_batches(world):
    conn = world["conn"]
    exclude_batches(conn, "fork", iso(T0 + STEP * 2), iso(T0 + STEP * 3), "test", apply=True, **QUIET)

    result = credit_rejected_submissions(conn, "sok", *period(), SOK, "test", apply=True, **QUIET)

    assert result["credits"] == 0


def test_revert_restores_the_original_state(world):
    conn = world["conn"]
    summary_before, files_before = summary_pairs(conn), files_processed(conn)
    exclude_batches(conn, "fork", iso(T0 + STEP * 3), iso(T0 + STEP * 4), "test", apply=True, **QUIET)
    credit_rejected_submissions(conn, "sok", *period(), SOK, "test", apply=True, **QUIET)

    revert_correction(conn, "sok", apply=True, **QUIET)
    revert_correction(conn, "fork", apply=True, **QUIET)

    assert summary_pairs(conn) == summary_before
    assert files_processed(conn) == files_before
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM points WHERE file_name LIKE 'score-correction:%'")
        assert cur.fetchone()[0] == 0
    assert all(row[4] is not None for row in list_corrections(conn, **QUIET)), "all marked reverted"
    with pytest.raises(ValueError):
        revert_correction(conn, "sok", apply=True, **QUIET)


def test_revert_is_exact_when_exclusion_follows_credit_on_same_batch(world):
    conn = world["conn"]
    summary_before, files_before = summary_pairs(conn), files_processed(conn)
    credit_rejected_submissions(conn, "sok", *period(), SOK, "t", apply=True, **QUIET)
    exclude_batches(conn, "fork", iso(T0 + STEP * 2), iso(T0 + STEP * 3), "t", apply=True, **QUIET)
    revert_correction(conn, "sok", apply=True, **QUIET)
    revert_correction(conn, "fork", apply=True, **QUIET)
    assert files_processed(conn) == files_before
    assert summary_pairs(conn) == summary_before, "phantom points_summary row left behind"


def test_exclude_reports_partially_overlapping_batches(world):
    lines = []
    exclude_batches(world["conn"], "fork", iso(T0 + timedelta(minutes=10)), iso(T0 + timedelta(minutes=70)),
                    "t", log=lines.append)
    assert any("partially" in line and "NOT excluded" in line for line in lines), lines


def test_cleanup_old_data_still_deletes_orphan_statehash_after_credit(world):
    conn = world["conn"]
    with conn.cursor() as cur:
        cur.execute("INSERT INTO statehash (value) VALUES ('tip') RETURNING id")
        cur.execute("UPDATE points SET statehash_id = %s", (cur.fetchone()[0],))
        cur.execute("INSERT INTO bot_logs_statehash (parent_statehash_id, statehash_id, weight, bot_log_id) "
                    "SELECT statehash_id, statehash_id, 1, bot_log_id FROM points LIMIT 1")
        cur.execute("INSERT INTO statehash (value) VALUES ('orphan1')")
        cur.execute("SELECT cleanup_old_data(100000)")
        cur.execute("SELECT count(*) FROM statehash WHERE value = 'orphan1'")
        assert cur.fetchone()[0] == 0  # control
        cur.execute("INSERT INTO statehash (value) VALUES ('orphan2')")
    credit_rejected_submissions(conn, "sok", *period(), SOK, "t", apply=True, **QUIET)
    with conn.cursor() as cur:
        cur.execute("SELECT cleanup_old_data(100000)")
        cur.execute("SELECT count(*) FROM statehash WHERE value = 'orphan2'")
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize("hours, expected", [(None, 1), (1, 1), (0.25, 0)])
def test_credit_guard_requires_a_real_point_nearby(world, hours, expected):
    """HIT's last real point is in batch 2 (ends 20 min before the rejected
    batch 3 ends): a 1 h window credits it, a 15 min window doesn't."""
    result = credit_rejected_submissions(world["conn"], "sok", *period(), SOK, "t",
                                         require_point_within_hours=hours, **QUIET)
    assert result["credits"] == expected
