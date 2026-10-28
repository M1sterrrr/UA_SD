import argparse
import random
import socket
import threading
import time

from confluent_kafka import Consumer, Producer

from wm_common import kafka_msgs as km
from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send


class Identity:
    """Who this engine is. Written once by the monitor thread (IDENTIFY),
    read by the Kafka thread to pick out our own commands."""

    ws_id: str | None = None
    location: str | None = None


def keyboard_loop(fault: threading.Event) -> None:
    """Runs in its own thread: ENTER toggles a simulated fault."""
    print("[WM_WS_E] press ENTER to toggle a simulated fault (KO)")
    while True:
        try:
            input()
        except EOFError:  # stdin closed (Ctrl+D): no more keyboard input
            return
        if fault.is_set():
            fault.clear()
            print("[WM_WS_E] fault cleared, answering OK")
        else:
            fault.set()
            print("[WM_WS_E] fault simulated, answering KO")


def water(producer: Producer, msg: dict) -> None:
    """Runs in its own thread: simulate the flow meter for `duration` seconds,
    sending a TELEMETRY reading every second."""
    ws_id, rid, duration = msg["ws_id"], msg["request_id"], msg["duration"]
    print(f"[WM_WS_E] VALVE OPEN: request {rid} by {msg['operator_id']}, {duration} s")
    volume = 0.0
    for elapsed in range(1, duration + 1):
        time.sleep(1)
        flow = round(random.uniform(18, 22), 1)  # L/min, a slightly noisy sensor
        volume += flow / 60  # litres that flowed during this one second
        km.publish(producer, km.WS_EVENTS, ws_id, km.MsgType.TELEMETRY,
                   ws_id=ws_id, request_id=rid, flow=flow,
                   volume=round(volume, 2), elapsed=elapsed)
        print(f"[WM_WS_E] {flow} L/min, {volume:.2f} L ({elapsed}/{duration} s)")
    print("[WM_WS_E] VALVE CLOSED: time limit reached")


def handle_command(msg: dict, producer: Producer) -> None:
    if msg["type"] == km.MsgType.START:
        # water in a separate thread, so we keep reading commands meanwhile
        threading.Thread(target=water, args=(producer, msg), daemon=True).start()
    else:
        print(f"[WM_WS_E] command {msg['type']} not implemented yet")


def command_loop(consumer: Consumer, producer: Producer, me: Identity,
                 stop: threading.Event) -> None:
    """Runs in its own thread: read wm.ws.commands, keep only ours.
    This thread owns the consumer, so it is also the one that closes it."""
    try:
        while not stop.is_set():
            msg = km.poll(consumer)
            if msg is None or me.ws_id is None or msg.get("ws_id") != me.ws_id:
                continue  # nothing, not identified yet, or another station's command
            try:
                handle_command(msg, producer)
            except KeyError as e:
                print(f"[WM_WS_E] ignoring incomplete command ({e!r})")
    finally:
        consumer.close()


def monitor_loop(sock: socket.socket, fault: threading.Event, me: Identity) -> None:
    """Answer the monitor's IDENTIFY and HEALTH requests until it disconnects."""
    while True:
        msg = recv(sock)
        if msg.op == OpCode.IDENTIFY:
            me.ws_id, me.location = msg.fields
            print(f"[WM_WS_E] I am station {me.ws_id} ({me.location})")
            send(sock, Message(OpCode.OK))
        elif msg.op == OpCode.HEALTH:
            send(sock, Message(OpCode.KO if fault.is_set() else OpCode.OK))
        else:
            send(sock, Message(OpCode.KO, ["unknown request"]))


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_E")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("monitor", type=host_port, help="WM_WS_M HOST:PORT")
    args = parser.parse_args()

    fault = threading.Event()  # shared between the keyboard thread and the main thread
    threading.Thread(target=keyboard_loop, args=(fault,), daemon=True).start()

    # Kafka first: by the time the monitor reports us healthy (and Central can send
    # START), our consumer is already listening.
    me = Identity()
    stop = threading.Event()  # tells the Kafka thread to finish
    producer = km.make_producer(args.broker)
    consumer = km.make_consumer(args.broker, "engine", [km.WS_COMMANDS])
    kafka_thread = threading.Thread(
        target=command_loop, args=(consumer, producer, me, stop), daemon=True
    )
    kafka_thread.start()
    print(f"[WM_WS_E] listening on Kafka topic {km.WS_COMMANDS}")

    sock = socket.create_connection(args.monitor)
    print(f"[WM_WS_E] connected to monitor at {args.monitor}")
    try:
        monitor_loop(sock, fault, me)
    except ConnectionError:
        print("[WM_WS_E] monitor closed the connection")
    finally:
        sock.close()
        stop.set()  # never close the consumer here: the Kafka thread may be inside poll()
        kafka_thread.join(timeout=3)
