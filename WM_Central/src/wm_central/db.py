"""CENTRAL's persistent data (SQLite). Only WM_Central touches this database."""
import os
import sqlite3

DB_PATH = os.environ.get("WM_DB_PATH", "wm_central.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS stations (
    id       TEXT PRIMARY KEY,
    location TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS operators (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
"""


def connect(path: str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the database file and make sure the tables exist."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row  # rows behave like dicts: row["location"]
    conn.executescript(SCHEMA)
    return conn


def add_station(conn: sqlite3.Connection, ws_id: str, location: str) -> None:
    with conn:  # transaction: commit on success, rollback on error
        conn.execute("INSERT INTO stations (id, location) VALUES (?, ?)", (ws_id, location))


def add_operator(conn: sqlite3.Connection, op_id: str, name: str) -> None:
    with conn:
        conn.execute("INSERT INTO operators (id, name) VALUES (?, ?)", (op_id, name))


def get_station(conn: sqlite3.Connection, ws_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM stations WHERE id = ?", (ws_id,)).fetchone()


def get_operator(conn: sqlite3.Connection, op_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM operators WHERE id = ?", (op_id,)).fetchone()


def list_stations(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM stations ORDER BY id").fetchall()
