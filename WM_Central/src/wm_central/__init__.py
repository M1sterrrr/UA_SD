import argparse
import os
import threading

from wm_central import admin, dashboard, db, kafka_handler, seed, server
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km
from wm_common.cli import host_port


def run() -> None:
    parser = argparse.ArgumentParser(prog="WM_Central")
    # Both may also come from the environment: AWS ECS starts the container with
    # environment variables, not with command-line arguments
    parser.add_argument(
        "port", type=int, nargs="?", default=os.environ.get("WM_SOCKET_PORT", "5500"),
        help="port of the socket server for WM_WS_M (default: $WM_SOCKET_PORT or 5500)",
    )
    parser.add_argument(
        "broker", type=host_port, nargs="?", default=os.environ.get("WM_KAFKA"),
        help="Kafka bootstrap server HOST:PORT (default: $WM_KAFKA)",
    )
    parser.add_argument(
        "--http-port",
        type=int,
        default=int(os.environ.get("PORT", "8080")),  # Railway gives the web port in $PORT
        help="port of the web dashboard (default: $PORT or 8080)",
    )
    args = parser.parse_args()
    if args.broker is None:
        parser.error("no Kafka broker: pass HOST:PORT or set WM_KAFKA")

    conn = db.connect()
    if not db.list_stations(conn):  # brand-new database (e.g. a fresh cloud container)
        seed.main()
    table = StationTable(db.list_stations(conn))
    conn.close()
    print(f"[WM_Central] database {db.DB_PATH} loaded")
    table.print_table()

    # One producer for the whole of Central (confluent-kafka producers are thread-safe)
    producer = km.make_producer(args.broker)

    # Background threads: Kafka consumer, station broadcaster, admin console, dashboard.
    # The main thread runs the socket server.
    threading.Thread(
        target=kafka_handler.consume, args=(args.broker, table, producer), daemon=True
    ).start()
    threading.Thread(
        target=kafka_handler.broadcast_stations, args=(table, producer), daemon=True
    ).start()
    threading.Thread(target=admin.console, args=(table, producer), daemon=True).start()
    threading.Thread(
        target=dashboard.serve, args=(args.http_port, table, producer), daemon=True
    ).start()
    server.serve(args.port, table, producer)


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("[WM_Central] stopped (Ctrl+C)")
