import argparse
import threading

from wm_common.cli import host_port
from wm_central import db, kafka_handler, server
from wm_central.stations import StationTable


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_Central")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_M")
    parser.add_argument(
        "broker", type=host_port, help="Kafka bootstrap server HOST:PORT"
    )
    args = parser.parse_args()

    conn = db.connect()
    table = StationTable(db.list_stations(conn))
    conn.close()
    print(f"[WM_Central] database {db.DB_PATH} loaded")
    table.print_table()

    # Two jobs at once: Kafka in a background thread, sockets in the main thread
    threading.Thread(
        target=kafka_handler.consume, args=(args.broker, table), daemon=True
    ).start()
    server.serve(args.port, table)
