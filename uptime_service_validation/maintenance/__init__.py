"""One-off and on-demand maintenance operations on the coordinator database.

These are invoked through `invoke` tasks (see tasks.py) and ship in the
coordinator image, so they can run inside the cluster with the coordinator's
own Postgres environment:

    kubectl exec deploy/<coordinator> -- invoke --list
"""

import os

import psycopg2


def connect_from_env(autocommit=False):
    """Open a connection using the same POSTGRES_* variables as the coordinator."""
    kwargs = dict(
        host=os.environ.get("POSTGRES_HOST"),
        port=os.environ.get("POSTGRES_PORT"),
        dbname=os.environ.get("POSTGRES_DB"),
        user=os.environ.get("POSTGRES_USER"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )
    sslmode = os.environ.get("POSTGRES_SSLMODE")
    if sslmode:
        kwargs["sslmode"] = sslmode
    conn = psycopg2.connect(**kwargs)
    conn.autocommit = autocommit
    return conn
