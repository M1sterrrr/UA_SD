import argparse
import random
import socket
import threading

from confluent_kafka import Consumer, Producer

from wm_common import kafka_msgs as km
from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send


class EngineState:
    """Everything the engine's threads share."""

    def __init__(self) -> None:
        self.ws_id: str | None = None  # set once by the monitor (IDENTIFY)
        self.location: str | None = None
        self.fault = threading.Event()  # menu "f": simulated fault (answer KO)
        self.stop_watering = threading.Event()  # menu "s": end the irrigation now


MENU = "[WM_WS_E] menu: f = toggle simulated fault, s = stop watering"


def keyboard_loop(me: EngineState) -> None:
    """Runs in its own thread: the station's local menu."""
    print(MENU)
    while True:
        try:
            choice = input().strip().lower()
        except EOFError:  # stdin closed (Ctrl+D): no more keyboard input
            return
        if choice == "f":
            if me.fault.is_set():
                me.fault.clear()
                print("[WM_WS_E] fault cleared, answering OK")
            else:
                me.fault.set()
                me.stop_watering.set()  # a fault must end any irrigation immediately
                print("[WM_WS_E] fault simulated, answering KO")
        elif choice == "s":
            me.stop_watering.set()  # the watering thread notices within a second
        else:
            print(MENU)


def water(producer: Producer, msg: dict, me: EngineState) -> None:
    """Runs in its own thread: simulate the flow meter, sending a TELEMETRY
    reading every second, until the time limit, a stop or a fault."""
    ws_id, rid, duration = msg["ws_id"], msg["request_id"], msg["duration"]
    print(f"[WM_WS_E] VALVE OPEN: request {rid} by {msg['operator_id']}, {duration} s")
    me.stop_watering.clear()  # forget an "s" pressed while we weren't watering
    volume, elapsed = 0.0, 0

    while elapsed < duration and not me.fault.is_set():  # never water with a fault
        # wait() sleeps 1 s like time.sleep(1), but returns True at once if it is set
        if me.stop_watering.wait(1):
            break
        elapsed += 1
        flow = round(random.uniform(18, 22), 1)  # L/min, a slightly noisy sensor
        volume += flow / 60  # litres that flowed during this one second
        km.publish(producer, km.WS_EVENTS, ws_id, km.MsgType.TELEMETRY,
                   ws_id=ws_id, request_id=rid, flow=flow,
                   volume=round(volume, 2), elapsed=elapsed)
        print(f"[WM_WS_E] {flow} L/min, {volume:.2f} L ({elapsed}/{duration} s)")

    if me.fault.is_set():
        reason = km.EndReason.LEAK
    elif elapsed < duration:
        reason = km.EndReason.STOPPED
    else:
        reason = km.EndReason.TIME_LIMIT
    km.publish(producer, km.WS_EVENTS, ws_id, km.MsgType.FINISHED,
               ws_id=ws_id, request_id=rid, volume=round(volume, 2),
               duration=elapsed, reason=reason)
    print(f"[WM_WS_E] VALVE CLOSED ({reason}): {volume:.2f} L in {elapsed} s")


def handle_command(msg: dict, producer: Producer, me: EngineState) -> None:
    if msg["type"] == km.MsgType.START:
        # water in a separate thread, so we keep reading commands meanwhile
        threading.Thread(target=water, args=(producer, msg, me), daemon=True).start()
    elif msg["type"] == km.MsgType.STOP:
        print(f"[WM_WS_E] STOP from Central ({msg.get('reason', '')})")
        me.stop_watering.set()
    else:
        print(f"[WM_WS_E] command {msg['type']} not implemented yet")


def command_loop(consumer: Consumer, producer: Producer, me: EngineState,
                 stop: threading.Event) -> None:
    """Runs in its own thread: read wm.ws.commands, keep only ours.
    This thread owns the consumer, so it is also the one that closes it."""
    try:
        while not stop.is_set():
            msg = km.poll(consumer)
            if msg is None or me.ws_id is None or msg.get("ws_id") != me.ws_id:
                continue  # nothing, not identified yet, or another station's command
            try:
                handle_command(msg, producer, me)
            except KeyError as e:
                print(f"[WM_WS_E] ignoring incomplete command ({e!r})")
    finally:
        consumer.close()


def monitor_loop(sock: socket.socket, me: EngineState) -> None:
    """Answer the monitor's IDENTIFY and HEALTH requests until it disconnects."""
    while True:
        msg = recv(sock)
        if msg.op == OpCode.IDENTIFY:
            me.ws_id, me.location = msg.fields
            print(f"[WM_WS_E] I am station {me.ws_id} ({me.location})")
            send(sock, Message(OpCode.OK))
        elif msg.op == OpCode.HEALTH:
            send(sock, Message(OpCode.KO if me.fault.is_set() else OpCode.OK))
        else:
            send(sock, Message(OpCode.KO, ["unknown request"]))


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_E")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("monitor", type=host_port, help="WM_WS_M HOST:PORT")
    args = parser.parse_args()

    me = EngineState()
    threading.Thread(target=keyboard_loop, args=(me,), daemon=True).start()

    # Kafka first: by the time the monitor reports us healthy (and Central can send
    # START), our consumer is already listening.
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
        monitor_loop(sock, me)
    except ConnectionError:
        print("[WM_WS_E] monitor closed the connection")
    finally:
        sock.close()
        stop.set()  # never close the consumer here: the Kafka thread may be inside poll()
        kafka_thread.join(timeout=3)
