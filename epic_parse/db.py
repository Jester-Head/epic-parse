"""Postgres connection helpers.

Connection settings come from the standard libpq environment variables
(PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE), loaded from a `.env` file
in the project root. Copy `.env.example` to `.env` and fill in your password.
"""
import logging
import os
from contextlib import contextmanager
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg import sql

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCHEMA_FILE = PROJECT_ROOT / "db" / "schema.sql"

load_dotenv(PROJECT_ROOT / ".env")

log = logging.getLogger(__name__)

# The GIN search indexes on posts (same definitions as db/schema.sql). Updating them row
# by row during a multi-million-row insert/update is extremely slow, so bulk operations
# drop them and rebuild them once at the end.
SEARCH_INDEXES = {
    "posts_body_search": "CREATE INDEX posts_body_search ON posts USING gin (body_tsv)",
    "posts_extra": "CREATE INDEX posts_extra ON posts USING gin (extra jsonb_path_ops)",
}


def database_name() -> str:
    return os.environ.get("PGDATABASE", "epic_parse")


def connect(dbname: str | None = None) -> psycopg.Connection:
    """Open a connection; host/user/password come from the environment.

    Autocommit is on: each statement is saved immediately (so a crash mid-crawl
    keeps everything fetched so far). Group statements with `conn.transaction()`
    when they must succeed or fail together.
    """
    return psycopg.connect(dbname=dbname or database_name(), autocommit=True)


def init_db() -> None:
    """Create the database if it doesn't exist, then apply schema.sql."""
    name = database_name()
    # CREATE DATABASE can't run inside a transaction, hence autocommit.
    with psycopg.connect(dbname="postgres", autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
            print(f"Created database {name!r}")
    with connect() as conn:
        conn.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    print(f"Schema applied to {name!r}")


def source_id(conn: psycopg.Connection, name: str) -> int:
    row = conn.execute(
        "INSERT INTO sources (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING id",
        (name,),
    ).fetchone()
    return row[0]


@contextmanager
def bulk_mode(conn: psycopg.Connection, enabled: bool = True):
    """Use inside conn.transaction(): more memory for sorts, search indexes rebuilt at the end.

    Dropping the indexes locks the posts table until the transaction commits; if the
    transaction fails, the drop is rolled back along with everything else.
    """
    if not enabled:
        yield
        return
    conn.execute("SET LOCAL work_mem = '256MB'")
    conn.execute("SET LOCAL maintenance_work_mem = '1GB'")
    for name in SEARCH_INDEXES:
        conn.execute(f"DROP INDEX IF EXISTS {name}")
    yield
    log.info("  rebuilding search indexes...")
    for create in SEARCH_INDEXES.values():
        conn.execute(create)
