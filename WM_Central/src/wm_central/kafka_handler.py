# Strona Kafki w Central. Wątek consume() czyta wm.requests (prośby od FO, silników
# i admina) oraz wm.ws.events (TELEMETRY i FINISHED od silników). Na prośby odpowiada
# w wm.fo.notifications (REQUEST_STATUS) i wysyła START do silnika w wm.ws.commands.
# Drugi wątek, broadcast_stations(), co 2 s rozsyła listę stacji.
"""CENTRAL's Kafka side: answers irrigation requests and follows station telemetry."""

import sqlite3
import time
from collections import deque

from confluent_kafka import Consumer, KafkaException, Producer

from wm_central import admin, db
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km

RETRY_DELAY = 3.0  # seconds between attempts to reach Kafka

# Ostatnie 10 próśb z werdyktem – tabela "Activation requests" na dashboardzie.
# The last activation requests and their verdicts, shown on the dashboard.
# deque(maxlen=...) drops the oldest entry by itself; appendleft() is thread-safe.
recent_requests: deque[dict] = deque(maxlen=10)


# Dopisuje prośbę i werdykt (AUTHORIZED albo DENIED: powód) do recent_requests.
# Wołana na końcu handle_request.
def remember(msg: dict, verdict: str) -> None:
    recent_requests.appendleft({
        "time": time.strftime("%H:%M:%S"), "request_id": msg["request_id"],
        "operator_id": msg["operator_id"], "ws_id": msg["ws_id"],
        "duration": msg["duration"], "verdict": verdict,
    })


# Wysyła REQUEST_STATUS (RECEIVED/AUTHORIZED/DENIED) do wm.fo.notifications.
# Czyta to FO, który wysłał prośbę (wait_for_verdict), oraz silnik tej stacji
# (handle_command wypisuje werdykt, np. dla prośby z menu "w").
def notify(producer: Producer, msg: dict, status: km.Status, reason: str = "") -> None:
    """Send a REQUEST_STATUS about request `msg` to its operator."""
    km.publish(
        producer,
        km.FO_NOTIFICATIONS,
        msg["operator_id"],
        km.MsgType.REQUEST_STATUS,
        request_id=msg["request_id"],
        operator_id=msg["operator_id"],
        ws_id=msg["ws_id"],
        status=status,
        reason=reason,
    )


# Sprawdzenia, które nie potrzebują tabeli stacji: znany operator (albo "CENTRAL"
# z komendy admina) i dodatni całkowity czas. Zwraca powód odmowy albo None.
def check_request(msg: dict, db_conn: sqlite3.Connection) -> str | None:
    """Checks that don't need the station table. Returns why the request
    must be denied, or None if it can go ahead."""
    operator_id = msg["operator_id"]
    if operator_id != admin.ADMIN_OPERATOR and db.get_operator(db_conn, operator_id) is None:
        return "unknown operator"
    duration = msg["duration"]
    if not isinstance(duration, int) or duration <= 0:
        return "invalid duration"
    return None


# Pełna obsługa IRRIGATION_REQUEST: 1) RECEIVED do operatora, 2) check_request,
# 3) table.reserve (stacja musi być AVAILABLE; staje się WATERING), 4) przy odmowie
# DENIED z powodem, przy zgodzie START do silnika (wm.ws.commands) i AUTHORIZED
# do operatora. Wynik trafia też na dashboard (remember).
def handle_request(
    msg: dict, table: StationTable, db_conn: sqlite3.Connection, producer: Producer
) -> None:
    rid = msg["request_id"]
    print(
        f"[WM_Central] request {rid}: operator {msg['operator_id']} "
        f"wants {msg['ws_id']} for {msg['duration']} s"
    )
    notify(producer, msg, km.Status.RECEIVED)

    # reserve() runs only if the other checks passed ("or" stops at the first reason)
    reason = check_request(msg, db_conn) or table.reserve(
        msg["ws_id"], rid, msg["operator_id"]
    )
    if reason:
        print(f"[WM_Central] request {rid} DENIED: {reason}")
        notify(producer, msg, km.Status.DENIED, reason)
        remember(msg, f"DENIED: {reason}")
        return

    # the station is reserved: tell its engine to start
    km.publish(
        producer,
        km.WS_COMMANDS,
        msg["ws_id"],
        km.MsgType.START,
        ws_id=msg["ws_id"],
        request_id=rid,
        operator_id=msg["operator_id"],
        duration=msg["duration"],
    )
    print(f"[WM_Central] request {rid} AUTHORIZED, START sent to {msg['ws_id']}")
    notify(producer, msg, km.Status.AUTHORIZED)
    remember(msg, "AUTHORIZED")


# TELEMETRY od silnika (co 1 s podczas podlewania): zapisuje przepływ i objętość
# w tabeli (widać je na dashboardzie) i wypisuje je w terminalu.
def handle_telemetry(msg: dict, table: StationTable) -> None:
    if table.set_telemetry(msg["ws_id"], msg["request_id"], msg["flow"], msg["volume"]):
        print(
            f"[WM_Central] {msg['ws_id']} watering: {msg['flow']} L/min, "
            f"{msg['volume']} L ({msg['elapsed']} s)"
        )


# FINISHED od silnika: zwalnia stację w tabeli (AVAILABLE, albo LEAK przy powodzie LEAK).
def handle_finished(msg: dict, table: StationTable) -> None:
    leaked = msg["reason"] == km.EndReason.LEAK
    if table.finish(msg["ws_id"], msg["request_id"], leaked):
        print(
            f"[WM_Central] {msg['ws_id']} finished request {msg['request_id']} "
            f"({msg['reason']}): {msg['volume']} L in {msg['duration']} s"
        )


BROADCAST_INTERVAL = 2.0  # seconds between station lists sent to the operators


# Wątek: co 2 s publikuje STATIONS (table.snapshot()) w wm.fo.notifications.
# FO pokazuje z tego listę stacji (przy starcie i po komendzie "list").
def broadcast_stations(table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: tell every operator the state of every station,
    so they can see which ones are available (PDF step 10)."""
    while True:
        km.publish(producer, km.FO_NOTIFICATIONS, "stations", km.MsgType.STATIONS,
                   stations=table.snapshot())
        time.sleep(BROADCAST_INTERVAL)


# Tworzy konsumenta Kafki dla Central i ponawia co 3 s, dopóki Kafka nie odpowie
# (np. broker jeszcze się uruchamia). Bez tego wątek by umarł, a Central byłby "głuchy".
def connect_consumer(broker: tuple[str, int]) -> Consumer:
    """Kafka may still be starting (or be unreachable for a while): keep trying,
    like the monitor does with Central. Without this the thread would die and
    Central would keep running deaf to every request."""
    while True:
        try:
            return km.make_consumer(broker, "central", [km.REQUESTS, km.WS_EVENTS])
        except (ConnectionError, KafkaException) as e:
            print(f"[WM_Central] Kafka not reachable ({e}), retrying in {RETRY_DELAY:.0f} s")
            time.sleep(RETRY_DELAY)


# Wątek: w nieskończonej pętli km.poll() i rozdział wiadomości po typie na
# handle_request / handle_telemetry / handle_finished. Ma własne połączenie z bazą.
# Brak pola w wiadomości (KeyError) nie zabija pętli – wiadomość jest pomijana.
def consume(broker: tuple[str, int], table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: read requests and station events forever."""
    db_conn = db.connect()  # this thread's own DB connection
    consumer = connect_consumer(broker)
    print(f"[WM_Central] listening on Kafka topics {km.REQUESTS}, {km.WS_EVENTS}")
    try:
        while True:
            msg = km.poll(consumer)
            if msg is None:
                continue
            try:
                if msg["type"] == km.MsgType.IRRIGATION_REQUEST:
                    handle_request(msg, table, db_conn, producer)
                elif msg["type"] == km.MsgType.TELEMETRY:
                    handle_telemetry(msg, table)
                elif msg["type"] == km.MsgType.FINISHED:
                    handle_finished(msg, table)
            except KeyError as e:  # a field our handlers need is missing
                print(f"[WM_Central] ignoring incomplete message ({e!r})")
    finally:
        consumer.close()
        db_conn.close()
