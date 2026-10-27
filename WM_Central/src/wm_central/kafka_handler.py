"""CENTRAL's Kafka side: consumes irrigation requests."""
from wm_central.stations import StationTable
from wm_common import kafka_msgs as km


def handle_request(msg: dict, table: StationTable) -> None:
    print(f"[WM_Central] request {msg['request_id']}: operator {msg['operator_id']} "
          f"wants {msg['ws_id']} for {msg['duration']} s")


def consume(broker: tuple[str, int], table: StationTable) -> None:
    """Runs in its own thread: read wm.requests forever."""
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
            except (ValueError, KeyError) as e:  # not JSON, or unknown/missing type
                print(f"[WM_Central] ignoring bad Kafka message ({e})")
                continue
            if msg["type"] == km.MsgType.IRRIGATION_REQUEST:
                handle_request(msg, table)
    finally:
        consumer.close()
