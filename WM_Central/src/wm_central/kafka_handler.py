"""CENTRAL's Kafka side: answers irrigation requests and follows station telemetry."""

import sqlite3
import time

from confluent_kafka import Producer

from wm_central import admin, db
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km


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


def handle_telemetry(msg: dict, table: StationTable) -> None:
    if table.set_telemetry(msg["ws_id"], msg["request_id"], msg["flow"], msg["volume"]):
        print(
            f"[WM_Central] {msg['ws_id']} watering: {msg['flow']} L/min, "
            f"{msg['volume']} L ({msg['elapsed']} s)"
        )


def handle_finished(msg: dict, table: StationTable) -> None:
    leaked = msg["reason"] == km.EndReason.LEAK
    if table.finish(msg["ws_id"], msg["request_id"], leaked):
        print(
            f"[WM_Central] {msg['ws_id']} finished request {msg['request_id']} "
            f"({msg['reason']}): {msg['volume']} L in {msg['duration']} s"
        )


BROADCAST_INTERVAL = 2.0  # seconds between station lists sent to the operators


def broadcast_stations(table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: tell every operator the state of every station,
    so they can see which ones are available (PDF step 10)."""
    while True:
        km.publish(producer, km.FO_NOTIFICATIONS, "stations", km.MsgType.STATIONS,
                   stations=table.snapshot())
        time.sleep(BROADCAST_INTERVAL)


def consume(broker: tuple[str, int], table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: read requests and station events forever."""
    db_conn = db.connect()  # this thread's own DB connection
    consumer = km.make_consumer(broker, "central", [km.REQUESTS, km.WS_EVENTS])
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
