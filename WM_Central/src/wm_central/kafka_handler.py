"""CENTRAL's Kafka side: consumes irrigation requests and answers them."""

import sqlite3

from confluent_kafka import Producer

from wm_central import db
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km
from wm_common.states import WSState


def notify(producer: Producer, msg: dict, status: km.Status, reason: str = "") -> None:
    """Send a REQUEST_STATUS about request `msg` to its operator."""
    producer.produce(
        km.FO_NOTIFICATIONS,
        key=msg["operator_id"],
        value=km.encode(
            km.MsgType.REQUEST_STATUS,
            request_id=msg["request_id"],
            operator_id=msg["operator_id"],
            ws_id=msg["ws_id"],
            status=status,
            reason=reason,
        ),
    )
    producer.flush()


def check_request(
    msg: dict, table: StationTable, db_conn: sqlite3.Connection
) -> str | None:
    """Returns why the request must be denied, or None if it can go ahead."""
    if db.get_operator(db_conn, msg["operator_id"]) is None:
        return "unknown operator"
    duration = msg["duration"]
    if not isinstance(duration, int) or duration <= 0:
        return "invalid duration"
    state = table.get_state(msg["ws_id"])
    if state is None:
        return "unknown station"
    if state != WSState.AVAILABLE:
        return f"station is {state}"
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

    reason = check_request(msg, table, db_conn)
    if reason:
        print(f"[WM_Central] request {rid} DENIED: {reason}")
        notify(producer, msg, km.Status.DENIED, reason)
    else:
        print(f"[WM_Central] request {rid} AUTHORIZED")
        notify(producer, msg, km.Status.AUTHORIZED)


def consume(broker: tuple[str, int], table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: read wm.requests forever."""
    db_conn = db.connect()  # this thread's own DB connection
    consumer = km.make_consumer(broker, "central", [km.REQUESTS])
    print(f"[WM_Central] listening on Kafka topic {km.REQUESTS}")
    try:
        while True:
            raw = consumer.poll(1.0)
            if raw is None:
                continue
            if raw.error():
                print(f"[WM_Central] Kafka error: {raw.error()}")
                continue
            value = raw.value()
            if value is None:  # message without a body: nothing to decode
                continue
            try:
                msg = km.decode(value)
                if msg["type"] == km.MsgType.IRRIGATION_REQUEST:
                    handle_request(msg, table, db_conn, producer)
            except (ValueError, KeyError) as e:  # bad JSON, unknown type, missing field
                print(f"[WM_Central] ignoring bad Kafka message ({e!r})")
    finally:
        consumer.close()
        db_conn.close()
