import argparse
import socket
import sys
import threading
import time

from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send

HEALTH_INTERVAL = 1.0  # seconds between health checks (PDF: every second)
HEALTH_TIMEOUT = 3.0   # seconds without an answer before we consider the engine dead
AUTH_TIMEOUT = 5.0     # seconds to connect to and get an answer from Central
PING_INTERVAL = 2.0    # heartbeat to Central (Central drops us after ~3 missed ones)
RETRY_DELAY = 3.0      # seconds between reconnection attempts


class CentralLink:
    """The monitor's connection to CENTRAL. It authenticates, sends a heartbeat,
    and reconnects by itself whenever the connection breaks. Two threads use it
    (heartbeat + engine watcher), so every exchange happens under one lock."""

    def __init__(self, address: tuple[str, int], ws_id: str) -> None:
        self.address = address
        self.ws_id = ws_id
        self.lock = threading.Lock()
        self.sock: socket.socket | None = None
        self.location = ""
        self.healthy: bool | None = None  # last engine health we know (None = no engine)

    def connect(self) -> None:
        """Keep trying until Central accepts us. Exits only if Central says this
        station doesn't exist (retrying would never help)."""
        while True:
            try:
                sock = socket.create_connection(self.address, timeout=AUTH_TIMEOUT)
                send(sock, Message(OpCode.AUTH, [self.ws_id]))
                reply = recv(sock)
            except (OSError, ValueError) as e:  # refused, timed out, or not our protocol
                print(f"[WM_WS_M] Central not reachable ({e}), retrying in {RETRY_DELAY:.0f} s")
                time.sleep(RETRY_DELAY)
                continue
            if reply.op == OpCode.OK:
                break
            sock.close()
            if reply.fields != ["already connected"]:
                sys.exit(f"[WM_WS_M] Central rejected station {self.ws_id}: {reply.fields}")
            # Central still remembers our old connection; it forgets it after a timeout
            print(f"[WM_WS_M] Central still sees the old connection, retrying in {RETRY_DELAY:.0f} s")
            time.sleep(RETRY_DELAY)

        sock.settimeout(AUTH_TIMEOUT)  # every later answer from Central must be quick too
        self.sock, self.location = sock, reply.fields[0]
        print(f"[WM_WS_M] authenticated with Central, location: {self.location}")
        if self.healthy:  # Central starts us as LEAK: tell it the engine is fine
            self._exchange(OpCode.RECOVERED)

    def _exchange(self, op: OpCode) -> Message:
        assert self.sock is not None
        send(self.sock, Message(op, [self.ws_id]))
        return recv(self.sock)

    def send(self, op: OpCode) -> None:
        """Send one message to Central; if the connection is broken, reconnect."""
        with self.lock:
            try:
                reply = self._exchange(op)
            except (OSError, ValueError) as e:
                print(f"[WM_WS_M] lost connection to Central ({e}), reconnecting")
                if self.sock:
                    self.sock.close()
                self.connect()  # also re-sends our current health
                return
            if reply.op != OpCode.OK:
                print(f"[WM_WS_M] Central did not accept {op}: {reply.fields}")

    def report_health(self, healthy: bool) -> None:
        self.healthy = healthy
        self.send(OpCode.RECOVERED if healthy else OpCode.LEAK)

    def heartbeat_loop(self) -> None:
        """Runs in its own thread: PING Central every few seconds. This is how a dead
        Central is noticed quickly, even when the station has nothing to report."""
        while True:
            time.sleep(PING_INTERVAL)
            self.send(OpCode.PING)


def check_engine(conn: socket.socket) -> bool:
    """Ask the engine "are you OK?". True if it answers OK, False if KO."""
    send(conn, Message(OpCode.HEALTH))
    reply = recv(conn)
    return reply.op == OpCode.OK


def update_health(old: bool | None, new: bool, central: CentralLink) -> bool:
    """Act only when the health CHANGES. None = no engine seen yet."""
    if new and old is not True:  # first OK ever, or OK after a fault
        print("[WM_WS_M] engine OK" if old is None else "[WM_WS_M] RECOVERED: engine OK again")
        central.report_health(True)  # Central marks us AVAILABLE
    elif not new and old is not False:
        print("[WM_WS_M] LEAK detected!")
        central.report_health(False)
    return new


def watch_engine(conn: socket.socket, healthy: bool | None, central: CentralLink) -> bool:
    """Tell the engine who it is, then health-check it every second
    until its connection breaks."""
    with conn:
        try:
            send(conn, Message(OpCode.IDENTIFY, [central.ws_id, central.location]))
            recv(conn)  # the engine's OK
            while True:
                ok = check_engine(conn)
                healthy = update_health(healthy, ok, central)
                time.sleep(HEALTH_INTERVAL)
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_WS_M] engine not responding correctly ({e})")
            return update_health(healthy, False, central)


def run() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_M")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_E")
    parser.add_argument("central", type=host_port, help="WM_Central HOST:PORT")
    parser.add_argument("ws_id", help="ID of this watering station")
    args = parser.parse_args()

    central = CentralLink(args.central, args.ws_id)
    central.connect()  # waits until Central is up
    threading.Thread(target=central.heartbeat_loop, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen()

    healthy = None
    while True:
        print(f"[WM_WS_M] station {args.ws_id}: waiting for engine on port {args.port}")
        conn, addr = server.accept()
        conn.settimeout(HEALTH_TIMEOUT)
        print(f"[WM_WS_M] engine connected from {addr}")
        healthy = watch_engine(conn, healthy, central)


def main() -> None:
    try:
        run()
    except KeyboardInterrupt:
        print("[WM_WS_M] stopped (Ctrl+C)")
