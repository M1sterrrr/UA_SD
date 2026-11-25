# Wypełnia bazę przykładowymi stacjami i operatorami. Uruchamiane na trzy sposoby:
# ręcznie (uv run python -m wm_central.seed), przez usługę "seed" w docker-compose.yml
# oraz automatycznie z run() w __init__.py, gdy baza jest pusta (np. świeży kontener na AWS).
"""Fill the database with sample data.  Run: uv run python -m wm_central.seed"""
import sqlite3

from wm_central import db

# Dane przykładowe: (ID, lokalizacja) stacji i niżej (ID, imię i nazwisko) operatorów.
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


# Wstawia każdą stację i każdego operatora; istniejące ID są pomijane (IntegrityError),
# więc skrypt można bezpiecznie uruchamiać wiele razy.
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
