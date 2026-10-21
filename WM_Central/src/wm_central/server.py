"""CENTRAL's socket server: one thread per connected WM_WS_M."""
import socket
import sqlite3
import threading

from wm_central import db
from wm_central.stations import StationTable
from wm_common.messages import Message, OpCode, recv, send
from wm_common.states import WSState

AUTH_TIMEOUT = 10.0  # a monitor must authenticate within this many seconds


def authenticate(conn: socket.socket) -> sqlite3.Row | None:
    """First message must be AUTH#<ws_id> with an ID that exists in the DB.
    Returns the station's DB row if valid; sends KO and returns None if not."""
    msg = recv(conn)
    if msg.op != OpCode.AUTH or len(msg.fields) != 1:
        send(conn, Message(OpCode.KO, ["expected AUTH#<ws_id>"]))
        return None

    ws_id = msg.fields[0]
    db_conn = db.connect()  # own connection: sqlite3 objects can't be shared between threads
    try:
        station = db.get_station(db_conn, ws_id)
    finally:
        db_conn.close()

    if station is None:
        print(f"[WM_Central] REJECTED unknown station {ws_id!r}")
        send(conn, Message(OpCode.KO, ["unknown station"]))
    return station


def handle_monitor(conn: socket.socket, addr: tuple, table: StationTable) -> None:
    """Runs in its own thread for the whole life of one monitor connection."""
    with conn:
        try:
            conn.settimeout(AUTH_TIMEOUT)
            station = authenticate(conn)
            if station is None:
                return
            ws_id, location = station["id"], station["location"]

            if not table.connect(ws_id, location):
                print(f"[WM_Central] REJECTED {ws_id}: already connected")
                send(conn, Message(OpCode.KO, ["already connected"]))
                return

            try:  # from here on the station is marked connected in the table
                send(conn, Message(OpCode.OK, [location]))
                print(f"[WM_Central] station {ws_id} ({location}) authenticated")
                conn.settimeout(None)  # authenticated: from now on wait as long as needed
                while True:
                    msg = recv(conn)
                    print(f"[WM_Central] {ws_id}: {msg.to_text()}")
                    send(conn, Message(OpCode.OK))
            finally:
                table.set_state(ws_id, WSState.DISCONNECTED)
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_Central] connection from {addr} closed ({e})")


def serve(port: int, table: StationTable) -> None:
    """Accept monitors forever, each one in a new thread."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen()
    print(f"[WM_Central] waiting for monitors on port {port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor, args=(conn, addr, table), daemon=True).start()
