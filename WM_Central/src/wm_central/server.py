"""CENTRAL's socket server: one thread per connected WM_WS_M."""
import socket
import sqlite3
import threading

from confluent_kafka import Producer

from wm_central import db
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km
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


REPORT_STATES = {
    OpCode.LEAK: WSState.LEAK,
    OpCode.RECOVERED: WSState.AVAILABLE,
}


def handle_report(
    msg: Message, ws_id: str, table: StationTable, producer: Producer
) -> Message:
    """Apply one message from an authenticated monitor. Returns the reply to send."""
    if msg.op not in REPORT_STATES or msg.fields != [ws_id]:
        return Message(OpCode.KO, ["unexpected message"])
    previous = table.set_state(ws_id, REPORT_STATES[msg.op])
    if msg.op == OpCode.LEAK and previous == WSState.WATERING:
        # a fault during irrigation: make sure the engine closes the valve
        km.publish(producer, km.WS_COMMANDS, ws_id, km.MsgType.STOP,
                   ws_id=ws_id, reason=km.EndReason.LEAK)
        print(f"[WM_Central] {ws_id} leaked while watering: STOP sent")
    return Message(OpCode.OK)


def handle_monitor(
    conn: socket.socket, addr: tuple, table: StationTable, producer: Producer
) -> None:
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
                    send(conn, handle_report(msg, ws_id, table, producer))
            finally:
                table.set_state(ws_id, WSState.DISCONNECTED)
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_Central] connection from {addr} closed ({e})")


def serve(port: int, table: StationTable, producer: Producer) -> None:
    """Accept monitors forever, each one in a new thread."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", port))
    server.listen()
    print(f"[WM_Central] waiting for monitors on port {port}")

    while True:
        conn, addr = server.accept()
        threading.Thread(
            target=handle_monitor, args=(conn, addr, table, producer), daemon=True
        ).start()
