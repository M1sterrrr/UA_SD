# Punkt wejścia aplikacji WM_Central (komenda WM_Central z pyproject.toml).
# Central to "mózg" systemu: zna stacje i operatorów (SQLite), przyjmuje monitory przez
# gniazda, autoryzuje prośby o podlewanie przez Kafkę, pokazuje dashboard WWW
# i przyjmuje komendy administratora. Ten plik tylko wszystko uruchamia.
import argparse
import os
import threading

from wm_central import admin, dashboard, db, kafka_handler, seed, server
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km
from wm_common.cli import host_port


# Start Central: 1) argumenty (albo zmienne środowiskowe WM_SOCKET_PORT, WM_KAFKA,
# PORT – tak konfiguruje kontener AWS ECS), 2) baza – wypełniana przykładami (seed),
# jeśli jest pusta, 3) wspólna tabela stacji StationTable (wszystkie DISCONNECTED),
# 4) jeden producent Kafki dla całego programu, 5) 4 wątki w tle:
# kafka_handler.consume (prośby + telemetria), kafka_handler.broadcast_stations (lista
# stacji co 2 s), admin.console (komendy z klawiatury), dashboard.serve (strona WWW).
# Wątek główny obsługuje serwer gniazd dla monitorów (server.serve).
# daemon=True: wątki w tle kończą się razem z programem.
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


# Funkcja uruchamiana komendą WM_Central. Ctrl+C (albo `docker stop`, który wysyła
# SIGINT – patrz Dockerfile) kończy program krótkim komunikatem zamiast błędu.
def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("[WM_Central] stopped (Ctrl+C)")
