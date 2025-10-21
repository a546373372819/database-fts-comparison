import sqlite3
import sqlite3
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
import logging
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import psycopg2
import io
import time
from tqdm import tqdm
import argparse
from datetime import datetime
import logging
import sys
# --- CONFIGURATION ---
parser = argparse.ArgumentParser(description="Migrate SQLite database to PostgreSQL.")
parser.add_argument("--sqlite", required=True, help="Path to SQLite database file")
args = parser.parse_args()
BATCH_SIZE = 10000  # adjust based on performance
SQLITE_DB_PATH = args.sqlite
PROGRESS_INTERVAL = 100000
TARGET_DB_NAME = "dms_model"
TABLE_NAME = "idobj_text_fts5"
SEARCH_VECTOR_COL = "search_vector"   # <— single source of truth
TS_CONFIG = "simple"                  # change to 'english' if you prefer stemming/stopwords
POSTGRES_INIT_CONN_INFO = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",
    "port": 5432
}
POSTGRES_CONN_INFO = "dbname=dms_model user=postgres password=admin host=localhost port=5432"


# --- UTILITIES ---
def log(msg: str):
    """Print a timestamped message."""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}",flush=True)

# --- DB CREATION ---
def ensure_postgres_database_exists():
    """Ensure the target PostgreSQL database exists; create if it does not."""
    log("Checking if PostgreSQL database exists...")
    conn = psycopg2.connect(**POSTGRES_INIT_CONN_INFO)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (TARGET_DB_NAME,))
    exists = cur.fetchone()

    if not exists:
        log(f"Database '{TARGET_DB_NAME}' not found — creating...")
        cur.execute(f'CREATE DATABASE "{TARGET_DB_NAME}";')
        log(f" Database '{TARGET_DB_NAME}' created successfully.")
    else:
        log(f" Database '{TARGET_DB_NAME}' already exists.")

    cur.close()
    conn.close()

# --- MIGRATION ---
def create_table_if_not_exists(sqlite_cur, pg_cur, pg_conn, search_col):
    """Read SQLite schema and create equivalent table in Postgres (lowercase cols)."""
    sqlite_cur.execute(f"PRAGMA table_info({TABLE_NAME})")
    cols = sqlite_cur.fetchall()
    if not cols:
        raise RuntimeError(f"No columns found in SQLite table '{TABLE_NAME}'")

    type_map = {
        "INTEGER": "BIGINT",
        "INT": "BIGINT",
        "TEXT": "TEXT",
        "REAL": "DOUBLE PRECISION",
        "BLOB": "BYTEA"
    }

    col_defs = []
    for _cid, name, col_type, _notnull, _dflt, pk in cols:
        col_name = name.lower()
        # Force gid to be INT regardless of SQLite type
        if col_name == "gid":
            pg_type = "INTEGER"
        else:
            pg_type = type_map.get((col_type or "TEXT").upper(), "TEXT")
        col_def = f'"{col_name}" {pg_type}'
        if pk:
            col_def += " PRIMARY KEY"
        col_defs.append(col_def)

    col_defs.append(f'"{search_col}" tsvector')

    create_stmt = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({', '.join(col_defs)});"
    pg_cur.execute(create_stmt)
    pg_conn.commit()
    log(f"Ensured PostgreSQL table exists: {TABLE_NAME}")

    # Add missing search_vector column if table existed before
    pg_cur.execute("""
        SELECT 1 FROM information_schema.columns
        WHERE table_name = %s AND column_name = %s
    """, (TABLE_NAME, search_col))
    if not pg_cur.fetchone():
        pg_cur.execute(f'ALTER TABLE {TABLE_NAME} ADD COLUMN "{search_col}" tsvector;')
        pg_conn.commit()
        log(f"Added missing column '{search_col}'.")


def create_search_vector_and_index(pg_cur, pg_conn):
    """Create combined tsvector column and GIN index."""
    log("Creating tsvector and GIN index...")
    update_sql = f"""
        UPDATE {TABLE_NAME}
        SET {SEARCH_VECTOR_COL} = 
            to_tsvector('simple',
                coalesce(idobj_name, '') || ' ' ||
                coalesce(idobj_customid, '') || ' ' ||
                coalesce(idobj_alias, '')
            );
    """
    pg_cur.execute(update_sql)

    index_sql = f"CREATE INDEX IF NOT EXISTS {TABLE_NAME}_search_idx ON {TABLE_NAME} USING GIN({SEARCH_VECTOR_COL});"
    pg_cur.execute(index_sql)
    pg_conn.commit()
    log("Created search vector and GIN index.")


def migrate_table():
    log(f"Connecting to SQLite at {SQLITE_DB_PATH}")
    sqlite_conn = sqlite3.connect(SQLITE_DB_PATH)
    sqlite_cur = sqlite_conn.cursor()

    log(f"Connecting to PostgreSQL: {POSTGRES_CONN_INFO}")
    pg_conn = psycopg2.connect(POSTGRES_CONN_INFO)
    pg_cur = pg_conn.cursor()

    create_table_if_not_exists(sqlite_cur, pg_cur, pg_conn,SEARCH_VECTOR_COL)

    sqlite_cur.execute(f"PRAGMA table_info({TABLE_NAME})")
    columns = [row[1].lower() for row in sqlite_cur.fetchall()]
    col_names = ", ".join(columns)

    insert_query = f"INSERT INTO {TABLE_NAME} ({col_names}) VALUES %s"

    sqlite_cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total_rows = sqlite_cur.fetchone()[0]
    log(f"Total rows to transfer: {total_rows:,}")

    processed = 0
    sqlite_cur.execute(f"SELECT {col_names} FROM {TABLE_NAME}")

    while True:
        batch = sqlite_cur.fetchmany(BATCH_SIZE)
        if not batch:
            break

        execute_values(pg_cur, insert_query, batch)
        pg_conn.commit()

        processed += len(batch)
        if processed % PROGRESS_INTERVAL < BATCH_SIZE:
            percent = (processed / total_rows) * 100
            log(f"Processed {processed:,}/{total_rows:,} rows ({percent:.2f}%)")

    create_search_vector_and_index(pg_cur, pg_conn)

    sqlite_conn.close()
    pg_conn.close()
    log("Migration completed successfully.")


if __name__ == "__main__":
    start = time.time()
    log("Starting SQLite to PostgreSQL migration...")
    try:
        migrate_table()
    except Exception as e:
        log(f"Error: {e}")
        sys.exit(1)
    finally:
        log(f"Done in {time.time() - start:.2f} seconds.")