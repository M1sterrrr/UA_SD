import argparse

from wm_central import db, server
from wm_common.cli import host_port


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_Central")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_M")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    args = parser.parse_args()

    conn = db.connect()
    stations = db.list_stations(conn)
    conn.close()
    print(f"[WM_Central] database {db.DB_PATH}: {len(stations)} registered stations")
    for s in stations:
        print(f"[WM_Central]   {s['id']}  {s['location']}")

    server.serve(args.port)
