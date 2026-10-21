"""Fill the database with sample data.  Run: uv run python -m wm_central.seed"""
import sqlite3

from wm_central import db

STATIONS = [
    ("WS1", "Błonia Południe"),
    ("WS2", "Błonia Północ"),
    ("WS3", "Kościół Mariacki"),
    ("WS4", "Planty"),
    ("WS5", "Park Jordana"),
    ("WS6", "Bulwary Wiślane"),
]

OPERATORS = [
    ("OP1", "Anna Kowalska"),
    ("OP2", "Piotr Nowak"),
    ("OP3", "Katarzyna Wiśniewska"),
]


def main() -> None:
    conn = db.connect()
    for ws_id, location in STATIONS:
        try:
            db.add_station(conn, ws_id, location)
            print(f"added station {ws_id}: {location}")
        except sqlite3.IntegrityError:  # PRIMARY KEY already exists
            print(f"station {ws_id} already exists, skipped")
    for op_id, name in OPERATORS:
        try:
            db.add_operator(conn, op_id, name)
            print(f"added operator {op_id}: {name}")
        except sqlite3.IntegrityError:
            print(f"operator {op_id} already exists, skipped")
    conn.close()


if __name__ == "__main__":
    main()
