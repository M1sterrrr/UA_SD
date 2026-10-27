import argparse
import time

from confluent_kafka import Consumer, Producer

from wm_common import kafka_msgs as km
from wm_common.cli import host_port

ANSWER_TIMEOUT = 10.0  # seconds to wait for Central's verdict


def send_request(producer: Producer, operator_id: str, ws_id: str, duration: int) -> str:
    """Publish one IRRIGATION_REQUEST. Returns its request_id."""
    request_id = km.new_request_id()
    producer.produce(km.REQUESTS, key=ws_id,
                     value=km.encode(km.MsgType.IRRIGATION_REQUEST, request_id=request_id,
                                     operator_id=operator_id, ws_id=ws_id, duration=duration))
    producer.flush()  # wait until Kafka has stored it
    return request_id


def wait_for_verdict(consumer: Consumer, request_id: str) -> dict | None:
    """Print status updates for OUR request until AUTHORIZED or DENIED.
    Returns that final message, or None if Central doesn't answer in time."""
    deadline = time.monotonic() + ANSWER_TIMEOUT
    while time.monotonic() < deadline:
        raw = consumer.poll(0.5)
        if raw is None or raw.error():
            continue
        value = raw.value()
        if value is None:
            continue
        try:
            msg = km.decode(value)
        except (ValueError, KeyError):
            continue
        if msg["type"] != km.MsgType.REQUEST_STATUS or msg.get("request_id") != request_id:
            continue  # someone else's message
        status = km.Status(msg["status"])
        print(f"[WM_FO] {request_id}: {status}" + (f" ({msg['reason']})" if msg["reason"] else ""))
        if status != km.Status.RECEIVED:
            return msg
    print(f"[WM_FO] {request_id}: no answer from Central after {ANSWER_TIMEOUT:.0f} s")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_FO")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("operator_id", help="ID of this operator")
    parser.add_argument("file", nargs="?", help="optional file with irrigation requests")
    args = parser.parse_args()

    producer = km.make_producer(args.broker)
    consumer = km.make_consumer(args.broker, f"fo-{args.operator_id}", [km.FO_NOTIFICATIONS])
    print(f"[WM_FO] operator {args.operator_id}, Kafka at {args.broker}")
    print("[WM_FO] type: <ws_id> <seconds>   e.g. WS2 30   (empty line to quit)")

    try:
        while line := input("[WM_FO] > ").strip():
            try:
                ws_id, seconds = line.split()
                duration = int(seconds)
            except ValueError:
                print("[WM_FO] expected: <ws_id> <seconds>")
                continue
            request_id = send_request(producer, args.operator_id, ws_id, duration)
            print(f"[WM_FO] request {request_id} sent: {ws_id} for {duration} s")
            wait_for_verdict(consumer, request_id)
    finally:
        consumer.close()
