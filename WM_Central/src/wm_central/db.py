# Trwałe dane Central w SQLite: tabela stacji (id, lokalizacja) i operatorów (id, nazwa).
# Tylko Central korzysta z bazy. Ścieżkę pliku zmienia zmienna WM_DB_PATH (w Dockerze:
# /data/wm_central.db na wolumenie). Stan "na żywo" (WATERING, LEAK, ...) NIE jest
# w bazie – trzyma go w pamięci StationTable (stations.py).
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


# Otwiera (lub tworzy) plik bazy i tworzy tabele, jeśli ich nie ma. Każdy wątek otwiera
# własne połączenie (obiektów sqlite3 nie wolno dzielić między wątkami): run()
# w __init__.py, server.authenticate, kafka_handler.consume, seed.main.
def connect(path: str = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the database file and make sure the tables exist."""
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row  # rows behave like dicts: row["location"]
    conn.executescript(SCHEMA)
    return conn


# Dodaje stację (INSERT). Powtórzone ID -> sqlite3.IntegrityError. Używana przez seed.py.
def add_station(conn: sqlite3.Connection, ws_id: str, location: str) -> None:
    with conn:  # transaction: commit on success, rollback on error
        conn.execute("INSERT INTO stations (id, location) VALUES (?, ?)", (ws_id, location))


# Dodaje operatora (INSERT). Używana przez seed.py.
def add_operator(conn: sqlite3.Connection, op_id: str, name: str) -> None:
    with conn:
        conn.execute("INSERT INTO operators (id, name) VALUES (?, ?)", (op_id, name))


# Wiersz stacji albo None. server.authenticate sprawdza nią, czy monitor podał
# istniejące ID (AUTH#WS2).
def get_station(conn: sqlite3.Connection, ws_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM stations WHERE id = ?", (ws_id,)).fetchone()


# Wiersz operatora albo None. kafka_handler.check_request sprawdza nią, czy prośba
# pochodzi od znanego operatora.
def get_operator(conn: sqlite3.Connection, op_id: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM operators WHERE id = ?", (op_id,)).fetchone()


# Wszystkie stacje posortowane po ID. run() buduje z nich StationTable i sprawdza,
# czy baza jest pusta (wtedy seed).
def list_stations(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute("SELECT * FROM stations ORDER BY id").fetchall()
