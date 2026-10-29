"""Kafka side of the protocol: topics, message types, JSON encoding, client setup.

Topic               Key          Producer        Consumer            Message types
------------------  -----------  --------------  ------------------  ----------------------------
wm.requests         ws_id        WM_FO, WM_WS_E  WM_Central          IRRIGATION_REQUEST
wm.ws.commands      ws_id        WM_Central      WM_WS_E             START, STOP, BLOCK, ACTIVATE
wm.ws.events        ws_id        WM_WS_E         WM_Central, WM_FO   TELEMETRY, FINISHED
wm.fo.notifications operator_id  WM_Central      WM_FO, WM_WS_E      REQUEST_STATUS, STATIONS

The FO reads wm.ws.events directly and keeps only events with its own request_id.
If an engine cannot start (fault), it answers START with FINISHED (reason LEAK) at once.

Every message value is a JSON object with a "type" field, e.g.
    {"type": "IRRIGATION_REQUEST", "request_id": "a1b2", "operator_id": "OP1",
     "ws_id": "WS2", "duration": 30}

Consumers only care about LIVE messages: each run joins a brand-new consumer group
that starts at the end of the topic, so old commands are never replayed.
"""
import json
import time
import uuid
from enum import StrEnum

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

REQUESTS = "wm.requests"
WS_COMMANDS = "wm.ws.commands"
WS_EVENTS = "wm.ws.events"
FO_NOTIFICATIONS = "wm.fo.notifications"


class MsgType(StrEnum):
    # wm.requests
    IRRIGATION_REQUEST = "IRRIGATION_REQUEST"  # request_id, operator_id, ws_id, duration
    # wm.ws.commands
    START = "START"                            # ws_id, request_id, operator_id, duration
    STOP = "STOP"                              # ws_id, reason
    BLOCK = "BLOCK"                            # ws_id
    ACTIVATE = "ACTIVATE"                      # ws_id
    # wm.ws.events
    TELEMETRY = "TELEMETRY"                    # ws_id, request_id, flow, volume, elapsed
    FINISHED = "FINISHED"                      # ws_id, request_id, volume, duration, reason
    # wm.fo.notifications
    REQUEST_STATUS = "REQUEST_STATUS"          # request_id, operator_id, ws_id, status, reason
    STATIONS = "STATIONS"                      # stations: [{id, location, state}, ...]


class Status(StrEnum):
    """The "status" field of REQUEST_STATUS."""

    RECEIVED = "RECEIVED"      # Central got the request and is checking it
    AUTHORIZED = "AUTHORIZED"  # all checks passed
    DENIED = "DENIED"          # a check failed; "reason" says which


class EndReason(StrEnum):
    """The "reason" field of FINISHED."""

    TIME_LIMIT = "TIME_LIMIT"  # the requested duration was reached
    STOPPED = "STOPPED"        # the station's menu "s", or a STOP command from Central
    LEAK = "LEAK"              # a fault was detected while watering
    BLOCKED = "BLOCKED"        # Central put the station out of service


def encode(msg_type: MsgType, **fields) -> bytes:
    """encode(MsgType.STOP, ws_id="WS1") -> b'{"type": "STOP", "ws_id": "WS1"}'"""
    return json.dumps({"type": msg_type, **fields}).encode()


def decode(raw: bytes) -> dict:
    """The reverse of encode(). Raises ValueError on bad JSON or an unknown type."""
    msg = json.loads(raw)
    msg["type"] = MsgType(msg["type"])
    return msg


def publish(producer: Producer, topic: str, key: str, msg_type: MsgType, **fields) -> None:
    """Send one message and wait until Kafka has stored it."""
    producer.produce(topic, key=key, value=encode(msg_type, **fields))
    producer.flush()


def poll(consumer: Consumer, timeout: float = 1.0) -> dict | None:
    """Wait up to `timeout` s for the next message. Returns it decoded, or None if
    nothing arrived (empty, Kafka errors and garbage messages are skipped too)."""
    raw = consumer.poll(timeout)
    if raw is None or raw.error():
        return None
    value = raw.value()
    if value is None:
        return None
    try:
        return decode(value)
    except (ValueError, KeyError) as e:
        print(f"[kafka] ignoring bad message ({e!r})")
        return None


def new_request_id() -> str:
    """Short unique ID that links a request to all its replies."""
    return uuid.uuid4().hex[:8]


def make_producer(broker: tuple[str, int]) -> Producer:
    host, port = broker
    return Producer({"bootstrap.servers": f"{host}:{port}"})


def ensure_topics(broker: tuple[str, int]) -> None:
    """Create our topics if they don't exist yet. A consumer can't subscribe to a
    topic that was never created (only producers create topics automatically)."""
    host, port = broker
    admin = AdminClient({"bootstrap.servers": f"{host}:{port}"})
    wanted = [NewTopic(t, num_partitions=1, replication_factor=1)
              for t in (REQUESTS, WS_COMMANDS, WS_EVENTS, FO_NOTIFICATIONS)]
    for future in admin.create_topics(wanted).values():
        try:
            future.result()  # wait for the broker's answer
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def make_consumer(broker: tuple[str, int], name: str, topics: list[str],
                  timeout: float = 15.0) -> Consumer:
    """Consumer in a fresh group that only sees messages produced from now on.
    Returns once the broker has actually assigned the topics to it."""
    ensure_topics(broker)
    host, port = broker
    consumer = Consumer({
        "bootstrap.servers": f"{host}:{port}",
        "group.id": f"{name}-{uuid.uuid4().hex[:8]}",  # new group every run
        "auto.offset.reset": "latest",                 # ...starting at the end of the topic
        "enable.auto.commit": False,                   # throwaway group: nothing to remember
    })
    assigned = []
    consumer.subscribe(topics, on_assign=lambda c, partitions: assigned.append(True))
    deadline = time.monotonic() + timeout
    while not assigned:  # on_assign is called from inside poll()
        if time.monotonic() > deadline:
            raise ConnectionError(f"Kafka at {host}:{port} did not assign {topics}")
        consumer.poll(0.2)
    return consumer
