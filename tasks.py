from datetime import datetime, timedelta, timezone
import re
from invoke import task
import os
import psycopg2
from psycopg2 import sql

from uptime_service_validation.maintenance import connect_from_env
from uptime_service_validation.maintenance.indexes import (
    SUBMISSIONS_SUBMITTED_AT,
    ensure_index_concurrently,
)
from uptime_service_validation.maintenance.score_corrections import (
    credit_rejected_submissions,
    exclude_batches,
    list_corrections,
    revert_correction,
)


@task
def create_database(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    # Establishing connection to PostgreSQL server
    # (connect to initial database 'postgres' to create a new database)
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname="postgres",
        user=db_user,
        password=db_password,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Creating the database
    try:
        cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' created successfully")
    except psycopg2.errors.DuplicateDatabase:
        print(f"Database '{db_name}' already exists, not creating")

    cursor.close()
    conn.close()

    # Connect to the new database
    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Path to the SQL script relative to tasks.py
    sql_script_path = "uptime_service_validation/database/create_tables.sql"

    # Running the SQL script file
    with open(sql_script_path, "r") as file:
        sql_script = file.read()
        cursor.execute(sql_script)
        print("'create_tables.sql' script completed successfully")

    cursor.close()
    # Indexes on continuously-written tables are built online, outside the
    # script: it runs on every coordinator start and must never block writes.
    ensure_index_concurrently(conn, **SUBMISSIONS_SUBMITTED_AT)
    conn.close()


@task
def init_database(ctx, batch_end_epoch=None, mins_ago=None, override_empty=False):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    cursor = conn.cursor()

    if mins_ago is not None:
        batch_end_epoch = (
            datetime.now(timezone.utc) - timedelta(minutes=int(mins_ago))
        ).timestamp()
    elif batch_end_epoch is None:
        batch_end_epoch = datetime.now(timezone.utc).timestamp()
    else:
        datetime_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}.\d{2}:\d{2}:\d{2}.*$")
        # Convert batch_end_epoch to a timestamp in utc if it is a datetime string
        # use regex to check if it is of format 'YYYY-MM-DD HH:MM:SS'
        if datetime_pattern.match(batch_end_epoch):
            batch_end_epoch = datetime.fromisoformat(batch_end_epoch).timestamp()
            print(f"Converted datetime string to timestamp: {batch_end_epoch}")
        else:
            batch_end_epoch = int(batch_end_epoch)
            print(f"Using provided timestamp: {batch_end_epoch}")

    # Check if the table is empty, if override_empty is False
    should_insert = True
    if not override_empty:
        cursor.execute("SELECT COUNT(*) FROM bot_logs")
        count = cursor.fetchone()[0]
        should_insert = count == 0

    if should_insert:
        processing_time = 0
        files_processed = -1  # -1 indicates that this is initialization
        file_timestamps = datetime.fromtimestamp(batch_end_epoch, timezone.utc)
        batch_start_epoch = batch_end_epoch

        # Inserting data into the bot_logs table
        cursor.execute(
            "INSERT INTO bot_logs (processing_time, files_processed, file_timestamps, batch_start_epoch, batch_end_epoch) \
            VALUES (%s, %s, %s, %s, %s)",
            (
                processing_time,
                files_processed,
                file_timestamps,
                batch_start_epoch,
                batch_end_epoch,
            ),
        )
        print(f"Row inserted into bot_logs table. batch_end_epoch: {batch_end_epoch}.")
    else:
        print(
            "Table bot_logs is not empty. Row not inserted. You can override this by passing --override-empty."
        )

    conn.commit()
    cursor.close()
    conn.close()

@task
def create_ro_user(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")
    db_ro_user = os.environ.get("POSTGRES_RO_USER")
    db_ro_password = os.environ.get("POSTGRES_RO_PASSWORD")

    conn = psycopg2.connect(
        host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_password
    )
    cursor = conn.cursor()

    # Check if the user exists
    user_exists = False
    cursor.execute("SELECT 1 FROM pg_roles WHERE rolname=%s;", (db_ro_user,))
    user_exists = cursor.fetchone() is not None

    if not user_exists:
        cursor.execute(sql.SQL("CREATE USER {} WITH PASSWORD %s;").format(sql.Identifier(db_ro_user)), (db_ro_password,))
        cursor.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {};").format(sql.Identifier(db_name),sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {};").format(sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {};").format(sql.Identifier(db_ro_user)))
        cursor.execute(sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {};").format(sql.Identifier(db_ro_user)))
        print(f"User {db_ro_user} created")
    else:
        print(f"User {db_ro_user} already exists")

    conn.commit()
    cursor.close()
    conn.close()

@task
def drop_database(ctx):
    db_host = os.environ.get("POSTGRES_HOST")
    db_port = os.environ.get("POSTGRES_PORT")
    db_name = os.environ.get("POSTGRES_DB")
    db_user = os.environ.get("POSTGRES_USER")
    db_password = os.environ.get("POSTGRES_PASSWORD")

    # Establishing connection to PostgreSQL server
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname="postgres",
        user=db_user,
        password=db_password,
    )
    conn.autocommit = True
    cursor = conn.cursor()

    # Dropping the database
    try:
        cursor.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(db_name)))
        print(f"Database '{db_name}' dropped!")
    except Exception as e:
        print(f"Error dropping database '{db_name}'! Error: {e}")

    cursor.close()
    conn.close()


@task
def add_submissions_index(ctx):
    """Create idx_submissions_submitted_at online (CREATE INDEX CONCURRENTLY).

    Safe to run against a live database and safe to re-run. `invoke
    create-database` already does this on every coordinator start; use this
    task for a manual rebuild.
    """
    conn = connect_from_env(autocommit=True)
    try:
        ensure_index_concurrently(conn, **SUBMISSIONS_SUBMITTED_AT)
    finally:
        conn.close()


_CORRECTION_HELP = {
    "correction": "Label for this correction; used to list and revert it, e.g. 2026-09-mesa-fork",
    "start": "Start of the period, ISO-8601 with timezone, e.g. 2026-09-03T15:00:00Z",
    "end": "End of the period (exclusive), ISO-8601 with timezone",
    "reason": "Free text stored with every change, e.g. a link to the incident",
    "apply": "Commit the change. Without it the task is a dry run",
}


@task(help=_CORRECTION_HELP)
def score_correction_exclude_batches(ctx, correction, start, end, reason, apply=False):
    """Remove the batches inside [start, end) from every BP's score.

    Dry run by default: prints the score change of every BP, then rolls back.
    """
    conn = connect_from_env()
    try:
        exclude_batches(conn, correction, start, end, reason, apply=apply)
    finally:
        conn.close()


@task(help={
    **_CORRECTION_HELP,
    "error": "Substring of submissions.validation_error to credit",
    "require_point_within_hours": "Credit a batch only if the BP earned a real point within N hours of it",
})
def score_correction_credit_rejected(ctx, correction, start, end, error, reason, apply=False,
                                     require_point_within_hours=None):
    """Credit the batches in which a BP's submission was rejected with ERROR and it got no point.

    Dry run by default: prints the score change of every BP, then rolls back.
    """
    conn = connect_from_env()
    try:
        credit_rejected_submissions(
            conn, correction, start, end, error, reason, apply=apply,
            require_point_within_hours=float(require_point_within_hours) if require_point_within_hours else None,
        )
    finally:
        conn.close()


@task(help={"correction": _CORRECTION_HELP["correction"], "apply": _CORRECTION_HELP["apply"]})
def score_correction_revert(ctx, correction, apply=False):
    """Undo every change recorded under CORRECTION. Dry run by default."""
    conn = connect_from_env()
    try:
        revert_correction(conn, correction, apply=apply)
    finally:
        conn.close()


@task
def score_correction_list(ctx):
    """List every recorded score correction."""
    conn = connect_from_env()
    try:
        list_corrections(conn)
    finally:
        conn.close()
