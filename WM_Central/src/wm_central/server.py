"""CENTRAL's socket server: one thread per connected WM_WS_M."""
import socket
import threading

from wm_central import db
from wm_common.messages import Message, OpCode, recv, send

AUTH_TIMEOUT = 10.0  # a monitor must authenticate within this many seconds


def authenticate(conn: socket.socket) -> str | None:
    """First message must be AUTH#<ws_id> with an ID that exists in the DB.
    Returns the ws_id if accepted, None if rejected."""
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
        return None

    send(conn, Message(OpCode.OK, [station["location"]]))
    print(f"[WM_Central] station {ws_id} ({station['location']}) authenticated")
    return ws_id


def handle_monitor(conn: socket.socket, addr: tuple) -> None:
    """Runs in its own thread for the whole life of one monitor connection."""
    ws_id = None
    with conn:
        try:
            conn.settimeout(AUTH_TIMEOUT)
            ws_id = authenticate(conn)
            if ws_id is None:
                return
            conn.settimeout(None)  # authenticated: from now on wait as long as needed

            while True:
                msg = recv(conn)
                print(f"[WM_Central] {ws_id}: {msg.to_text()}")
                send(conn, Message(OpCode.OK))
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_Central] {ws_id or addr} disconnected ({e})")


def serve(port: int) -> None:
    """Accept monitors forever, each one in a new thread."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen()
    print(f"[WM_Central] waiting for monitors on port {port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True).start()
