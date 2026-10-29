import argparse
import threading

from wm_central import admin, db, kafka_handler, server
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km
from wm_common.cli import host_port


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

    # One producer for the whole of Central (confluent-kafka producers are thread-safe)
    producer = km.make_producer(args.broker)

    # Background threads: Kafka consumer, station broadcaster, admin console.
    # The main thread runs the socket server.
    threading.Thread(
        target=kafka_handler.consume, args=(args.broker, table, producer), daemon=True
    ).start()
    threading.Thread(
        target=kafka_handler.broadcast_stations, args=(table, producer), daemon=True
    ).start()
    threading.Thread(target=admin.console, args=(table, producer), daemon=True).start()
    server.serve(args.port, table, producer)
