import argparse

from wm_common import kafka_msgs as km
from wm_common.cli import host_port


def send_request(producer, operator_id: str, ws_id: str, duration: int) -> str:
    """Publish one IRRIGATION_REQUEST. Returns its request_id."""
    request_id = km.new_request_id()
    producer.produce(km.REQUESTS, key=ws_id,
                     value=km.encode(km.MsgType.IRRIGATION_REQUEST, request_id=request_id,
                                     operator_id=operator_id, ws_id=ws_id, duration=duration))
    producer.flush()  # wait until Kafka has stored it
    return request_id


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_FO")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("operator_id", help="ID of this operator")
    parser.add_argument("file", nargs="?", help="optional file with irrigation requests")
    args = parser.parse_args()

    producer = km.make_producer(args.broker)
    print(f"[WM_FO] operator {args.operator_id}, Kafka at {args.broker}")
    print("[WM_FO] type: <ws_id> <seconds>   e.g. WS2 30   (empty line to quit)")

    while line := input("[WM_FO] > ").strip():
        try:
            ws_id, seconds = line.split()
            duration = int(seconds)
        except ValueError:
            print("[WM_FO] expected: <ws_id> <seconds>")
            continue
        request_id = send_request(producer, args.operator_id, ws_id, duration)
        print(f"[WM_FO] request {request_id} sent: {ws_id} for {duration} s")
