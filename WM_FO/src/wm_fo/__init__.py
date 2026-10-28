import argparse
import time

from confluent_kafka import Consumer

from wm_common import kafka_msgs as km
from wm_common.cli import host_port

ANSWER_TIMEOUT = 10.0  # seconds to wait for Central's verdict
SILENCE_TIMEOUT = 5.0  # seconds without telemetry before we stop watching


def next_for_request(consumer: Consumer, request_id: str, seconds: float) -> dict | None:
    """Wait up to `seconds` for the next message about OUR request.
    Every other operator's messages are skipped."""
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        msg = km.poll(consumer, 0.5)
        if msg is not None and msg.get("request_id") == request_id:
            return msg
    return None


def wait_for_verdict(consumer: Consumer, request_id: str) -> bool:
    """Print status updates until AUTHORIZED or DENIED. True if authorized."""
    while msg := next_for_request(consumer, request_id, ANSWER_TIMEOUT):
        if msg["type"] != km.MsgType.REQUEST_STATUS:
            continue
        reason = f" ({msg['reason']})" if msg["reason"] else ""
        print(f"[WM_FO] {request_id}: {msg['status']}{reason}")
        if msg["status"] != km.Status.RECEIVED:
            return msg["status"] == km.Status.AUTHORIZED
    print(f"[WM_FO] {request_id}: no answer from Central after {ANSWER_TIMEOUT:.0f} s")
    return False


def follow_irrigation(consumer: Consumer, request_id: str) -> None:
    """Print the station's telemetry for our request until it FINISHES
    (or goes quiet, if the station died)."""
    while msg := next_for_request(consumer, request_id, SILENCE_TIMEOUT):
        if msg["type"] == km.MsgType.TELEMETRY:
            print(
                f"[WM_FO] {msg['ws_id']}: {msg['flow']} L/min, "
                f"{msg['volume']} L ({msg['elapsed']} s)"
            )
        elif msg["type"] == km.MsgType.FINISHED:
            print(
                f"[WM_FO] {request_id} FINISHED ({msg['reason']}): "
                f"{msg['volume']} L in {msg['duration']} s"
            )
            return
    print(f"[WM_FO] {request_id}: no news from the station for {SILENCE_TIMEOUT:.0f} s")


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_FO")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("operator_id", help="ID of this operator")
    parser.add_argument("file", nargs="?", help="optional file with irrigation requests")
    args = parser.parse_args()

    producer = km.make_producer(args.broker)
    consumer = km.make_consumer(
        args.broker, f"fo-{args.operator_id}", [km.FO_NOTIFICATIONS, km.WS_EVENTS]
    )
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
            request_id = km.new_request_id()
            km.publish(producer, km.REQUESTS, ws_id, km.MsgType.IRRIGATION_REQUEST,
                       request_id=request_id, operator_id=args.operator_id,
                       ws_id=ws_id, duration=duration)
            print(f"[WM_FO] request {request_id} sent: {ws_id} for {duration} s")
            if wait_for_verdict(consumer, request_id):
                follow_irrigation(consumer, request_id)
    finally:
        consumer.close()
