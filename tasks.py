from datetime import datetime, timedelta, timezone
from invoke import task
import os
import psycopg2
from psycopg2 import sql


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
        batch_end_epoch = int(batch_end_epoch)

    # Check if the table is empty, if override_empty is False
    should_insert = True
    if not override_empty:
        cursor.execute("SELECT COUNT(*) FROM bot_logs")
        count = cursor.fetchone()[0]
        should_insert = count == 0

    if should_insert:
        processing_time = 0
        files_processed = 0
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
