import argparse
import random
import socket
import threading
import time

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
        self.out_of_service = threading.Event()  # Central's BLOCK (until ACTIVATE)


RETRY_DELAY = 3.0  # seconds between attempts to reach the monitor

MENU = ("[WM_WS_E] menu: w <operator_id> <seconds> = water (e.g. w OP1 30), "
        "s = stop watering, f = toggle simulated fault")


def request_watering(producer: Producer, me: EngineState, words: list[str]) -> None:
    """Menu "w OP1 30": ask Central for an irrigation, exactly like the FO app does.
    Central checks it and, if authorized, sends us a normal START."""
    try:
        _, operator_id, seconds = words
        duration = int(seconds)
    except ValueError:
        print("[WM_WS_E] usage: w <operator_id> <seconds>")
        return
    if me.ws_id is None:
        print("[WM_WS_E] not identified by the monitor yet, try again in a moment")
        return
    request_id = km.new_request_id()
    km.publish(producer, km.REQUESTS, me.ws_id, km.MsgType.IRRIGATION_REQUEST,
               request_id=request_id, operator_id=operator_id,
               ws_id=me.ws_id, duration=duration)
    print(f"[WM_WS_E] request {request_id} sent to Central: {duration} s for {operator_id}")


def keyboard_loop(me: EngineState, producer: Producer) -> None:
    """Runs in its own thread: the station's local menu."""
    print(MENU)
    while True:
        try:
            words = input().split()
        except EOFError:  # stdin closed (Ctrl+D): no more keyboard input
            return
        choice = words[0].lower() if words else ""
        if choice == "w":
            request_watering(producer, me, words)
        elif choice == "f":
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
    reading every second, until the time limit, a stop, a fault or a block."""
    ws_id, rid, duration = msg["ws_id"], msg["request_id"], msg["duration"]
    print(f"[WM_WS_E] VALVE OPEN: request {rid} by {msg['operator_id']}, {duration} s")
    me.stop_watering.clear()  # forget an "s" pressed while we weren't watering
    volume, elapsed = 0.0, 0

    # never water with a fault or while Central has us out of service
    while elapsed < duration and not me.fault.is_set() and not me.out_of_service.is_set():
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
    elif me.out_of_service.is_set():
        reason = km.EndReason.BLOCKED
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
    elif msg["type"] == km.MsgType.BLOCK:
        me.out_of_service.set()
        me.stop_watering.set()  # end any irrigation in progress
        print("[WM_WS_E] *** OUT OF SERVICE *** (blocked by Central)")
    elif msg["type"] == km.MsgType.ACTIVATE:
        me.out_of_service.clear()
        print("[WM_WS_E] activated by Central: back in service")
    elif msg["type"] == km.MsgType.REQUEST_STATUS:  # Central's answers about our station
        reason = f" ({msg['reason']})" if msg["reason"] else ""
        print(f"[WM_WS_E] request {msg['request_id']} by {msg['operator_id']}: "
              f"{msg['status']}{reason}")
    else:
        print(f"[WM_WS_E] command {msg['type']} not implemented yet")


def command_loop(consumer: Consumer, producer: Producer, me: EngineState,
                 stop: threading.Event) -> None:
    """Runs in its own thread: read commands and request answers, keep only ours.
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


def run() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_E")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("monitor", type=host_port, help="WM_WS_M HOST:PORT")
    args = parser.parse_args()

    me = EngineState()
    producer = km.make_producer(args.broker)
    threading.Thread(target=keyboard_loop, args=(me, producer), daemon=True).start()

    # Kafka first: by the time the monitor reports us healthy (and Central can send
    # START), our consumer is already listening.
    stop = threading.Event()  # tells the Kafka thread to finish
    consumer = km.make_consumer(
        args.broker, "engine", [km.WS_COMMANDS, km.FO_NOTIFICATIONS]
    )
    kafka_thread = threading.Thread(
        target=command_loop, args=(consumer, producer, me, stop), daemon=True
    )
    kafka_thread.start()
    print(f"[WM_WS_E] listening on Kafka topics {km.WS_COMMANDS}, {km.FO_NOTIFICATIONS}")

    try:
        while True:  # reconnect forever: the monitor may restart
            try:
                sock = socket.create_connection(args.monitor)
            except OSError as e:
                print(f"[WM_WS_E] monitor not reachable ({e}), retrying in {RETRY_DELAY:.0f} s")
                time.sleep(RETRY_DELAY)
                continue
            print(f"[WM_WS_E] connected to monitor at {args.monitor}")
            try:
                monitor_loop(sock, me)
            except ConnectionError:
                print("[WM_WS_E] lost the monitor, reconnecting")
                me.stop_watering.set()  # nobody supervises us now: close the valve
            finally:
                sock.close()
    finally:
        stop.set()  # never close the consumer here: the Kafka thread may be inside poll()
        kafka_thread.join(timeout=3)


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("[WM_WS_E] stopped (Ctrl+C)")
