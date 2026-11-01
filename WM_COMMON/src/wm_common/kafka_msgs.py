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

from confluent_kafka import (
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)
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


def base_config(broker: tuple[str, int]) -> dict:
    """Settings every Kafka client of ours shares."""
    host, port = broker
    return {
        "bootstrap.servers": f"{host}:{port}",
        # only critical messages: while Kafka is down the library retries by itself
        # and logs every failed attempt as an error (level 3), ~20 lines a second.
        # Our own code reports what matters (timeouts, retries) through exceptions.
        "log_level": 2,
    }


def make_producer(broker: tuple[str, int]) -> Producer:
    return Producer(base_config(broker))


def ensure_topics(broker: tuple[str, int]) -> None:
    """Create our topics if they don't exist yet. A consumer can't subscribe to a
    topic that was never created (only producers create topics automatically)."""
    admin = AdminClient(base_config(broker))
    wanted = [NewTopic(t, num_partitions=1, replication_factor=1)
              for t in (REQUESTS, WS_COMMANDS, WS_EVENTS, FO_NOTIFICATIONS)]
    # without a timeout this waits silently for as long as Kafka is down
    for future in admin.create_topics(wanted, request_timeout=10).values():
        try:
            future.result()  # wait for the broker's answer
        except KafkaException as e:
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def make_consumer(broker: tuple[str, int], name: str, topics: list[str],
                  timeout: float = 15.0, durable: bool = False) -> Consumer:
    """durable=False: a fresh group every run, it only sees messages produced from now on.
    durable=True: the group is always `name`, so Kafka remembers where we stopped
    reading and, after a restart, first delivers everything we missed.
    Returns once the broker has actually assigned the topics to it."""
    ensure_topics(broker)
    host, port = broker
    config = {**base_config(broker), "auto.offset.reset": "latest"}  # first run: start at the end
    if durable:
        config |= {
            "group.id": name,
            # static membership: after a crash the restarted consumer takes its old place
            # at once, instead of waiting ~45 s for Kafka to declare the old one dead
            "group.instance.id": name,
            "enable.auto.commit": True,       # store our reading position...
            "auto.commit.interval.ms": 1000,  # ...every second
        }
    else:
        config |= {
            "group.id": f"{name}-{uuid.uuid4().hex[:8]}",  # new group every run
            "enable.auto.commit": False,                   # throwaway group: nothing to remember
        }
    consumer = Consumer(config)
    assigned = []
    consumer.subscribe(topics, on_assign=lambda c, partitions: assigned.append(True))
    deadline = time.monotonic() + timeout
    while not assigned:  # on_assign is called from inside poll()
        if time.monotonic() > deadline:
            consumer.close()  # the caller may retry: don't leak this one
            raise ConnectionError(f"Kafka at {host}:{port} did not assign {topics}")
        consumer.poll(0.2)
    if durable:
        # A brand-new group has no stored position yet: if we crashed before the first
        # auto-commit, the restart would jump to "latest" and skip what we missed.
        # So store "the end of the topic" as our position straight away.
        saved = consumer.committed(consumer.assignment(), timeout=10)
        fresh = [TopicPartition(p.topic, p.partition, consumer.get_watermark_offsets(p)[1])
                 for p in saved if p.offset < 0]  # negative offset = nothing stored
        if fresh:
            consumer.commit(offsets=fresh, asynchronous=False)
    return consumer
