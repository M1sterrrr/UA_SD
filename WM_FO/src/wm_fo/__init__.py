import argparse
import time

from confluent_kafka import Consumer, Producer

from wm_common import kafka_msgs as km
from wm_common.cli import host_port

ANSWER_TIMEOUT = 10.0  # seconds to wait for Central's verdict
SILENCE_TIMEOUT = 5.0  # seconds without telemetry before we stop watching
STATIONS_WAIT = 3.0  # Central broadcasts the station list every 2 s
FILE_PAUSE = 4.0  # PDF: wait 4 s after a request concludes before the next one


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


def run_request(producer: Producer, consumer: Consumer, operator_id: str,
                ws_id: str, duration: int) -> None:
    """One irrigation from start to end: request, verdict, telemetry, summary."""
    request_id = km.new_request_id()
    km.publish(producer, km.REQUESTS, ws_id, km.MsgType.IRRIGATION_REQUEST,
               request_id=request_id, operator_id=operator_id,
               ws_id=ws_id, duration=duration)
    print(f"[WM_FO] request {request_id} sent: {ws_id} for {duration} s")
    if wait_for_verdict(consumer, request_id):
        follow_irrigation(consumer, request_id)


def show_stations(consumer: Consumer) -> None:
    """Print the newest station list Central has broadcast."""
    stations = None
    deadline = time.monotonic() + STATIONS_WAIT
    while time.monotonic() < deadline:  # read for a while and keep the LAST list
        msg = km.poll(consumer, 0.5)
        if msg is not None and msg["type"] == km.MsgType.STATIONS:
            stations = msg["stations"]
    if stations is None:
        print("[WM_FO] no station list from Central (is it running?)")
        return
    print("[WM_FO] stations:")
    for s in stations:
        mark = "  <- available" if s["state"] == "AVAILABLE" else ""
        print(f"[WM_FO]   {s['id']:<5} {s['state']:<15} {s['location']}{mark}")


def parse_request(line: str) -> tuple[str, int]:
    """"WS2 30" -> ("WS2", 30). Raises ValueError if the line is malformed."""
    ws_id, seconds = line.split()
    return ws_id, int(seconds)


def run_file(path: str, producer: Producer, consumer: Consumer, operator_id: str) -> None:
    """Every line "<ws_id> <seconds>" is one request; blank lines and # comments skipped."""
    with open(path, encoding="utf-8") as f:
        lines = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    print(f"[WM_FO] {len(lines)} requests in {path}")
    for i, line in enumerate(lines, start=1):
        try:
            ws_id, duration = parse_request(line)
        except ValueError:
            print(f"[WM_FO] line {i}: {line!r} is not '<ws_id> <seconds>', skipped")
            continue
        print(f"[WM_FO] --- request {i}/{len(lines)} ---")
        run_request(producer, consumer, operator_id, ws_id, duration)
        if i < len(lines):
            print(f"[WM_FO] waiting {FILE_PAUSE:.0f} s before the next request")
            time.sleep(FILE_PAUSE)
    print("[WM_FO] all requests from the file done")


def run_interactive(producer: Producer, consumer: Consumer, operator_id: str) -> None:
    print("[WM_FO] type: <ws_id> <seconds> (e.g. WS2 30), list, or an empty line to quit")
    while line := input("[WM_FO] > ").strip():
        if line == "list":
            show_stations(consumer)
            continue
        try:
            ws_id, duration = parse_request(line)
        except ValueError:
            print("[WM_FO] expected: <ws_id> <seconds>, or list")
            continue
        run_request(producer, consumer, operator_id, ws_id, duration)


def run() -> None:
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
    try:
        show_stations(consumer)
        if args.file:
            run_file(args.file, producer, consumer, args.operator_id)
        else:
            run_interactive(producer, consumer, args.operator_id)
    finally:
        consumer.close()


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("[WM_FO] stopped (Ctrl+C)")
