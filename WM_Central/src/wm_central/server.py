# Serwer gniazd Central dla monitorów (WM_WS_M); każdy monitor dostaje własny wątek.
# Przebieg połączenia: AUTH#<ws_id> -> sprawdzenie w bazie -> OK#<lokalizacja>,
# potem w pętli PING (co 2 s), LEAK albo RECOVERED. Cisza dłuższa niż 6 s
# = monitor zniknął -> stacja DISCONNECTED.
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
MONITOR_TIMEOUT = 6.0  # monitors PING every 2 s: ~3 missed in a row = disconnected


# Pierwsza wiadomość musi być AUTH#<ws_id>, a ID musi istnieć w bazie (db.get_station).
# Zwraca wiersz stacji albo wysyła KO i zwraca None. Wołana z handle_monitor().
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


# Która wiadomość monitora ustawia jaki stan stacji (używa tego handle_report).
REPORT_STATES = {
    OpCode.LEAK: WSState.LEAK,
    OpCode.RECOVERED: WSState.AVAILABLE,
}


# Obsługuje jedną wiadomość od uwierzytelnionego monitora i zwraca odpowiedź:
# PING -> OK (samo dotarcie dowodzi, że monitor żyje), LEAK/RECOVERED -> table.set_state.
# Jeśli awaria (LEAK) przyszła w trakcie podlewania, wysyła przez Kafkę STOP do
# silnika (wm.ws.commands), żeby na pewno zamknął zawór.
def handle_report(
    msg: Message, ws_id: str, table: StationTable, producer: Producer
) -> Message:
    """Apply one message from an authenticated monitor. Returns the reply to send."""
    if msg.op == OpCode.PING and msg.fields == [ws_id]:
        return Message(OpCode.OK)  # heartbeat: the recv() that got it already proved we're alive
    if msg.op not in REPORT_STATES or msg.fields != [ws_id]:
        return Message(OpCode.KO, ["unexpected message"])
    previous = table.set_state(ws_id, REPORT_STATES[msg.op])
    if msg.op == OpCode.LEAK and previous == WSState.WATERING:
        # a fault during irrigation: make sure the engine closes the valve
        km.publish(producer, km.WS_COMMANDS, ws_id, km.MsgType.STOP,
                   ws_id=ws_id, reason=km.EndReason.LEAK)
        print(f"[WM_Central] {ws_id} leaked while watering: STOP sent")
    return Message(OpCode.OK)


# Ciało wątku jednego monitora, od połączenia do rozłączenia: authenticate ->
# table.connect (odrzuca drugi monitor z tym samym ID) -> OK#lokalizacja -> pętla
# recv / handle_report / send. Blok finally zawsze ustawia DISCONNECTED, gdy połączenie
# się skończy (rozłączenie, błąd albo timeout).
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
                conn.settimeout(MONITOR_TIMEOUT)  # silence this long = monitor is gone
                while True:
                    msg = recv(conn)
                    if msg.op != OpCode.PING:  # heartbeats would flood the log
                        print(f"[WM_Central] {ws_id}: {msg.to_text()}")
                    send(conn, handle_report(msg, ws_id, table, producer))
            finally:
                table.set_state(ws_id, WSState.DISCONNECTED)
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_Central] connection from {addr} closed ({e})")


# Główna pętla Central (wątek główny): nasłuchuje na porcie (domyślnie 5500) i dla
# każdego nowego monitora startuje wątek handle_monitor. SO_REUSEADDR pozwala od razu
# ponownie zająć port po restarcie programu.
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
