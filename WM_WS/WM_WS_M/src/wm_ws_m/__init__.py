import argparse
import socket
import sys
import time

from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send

HEALTH_INTERVAL = 1.0  # seconds between health checks (PDF: every second)
HEALTH_TIMEOUT = 3.0   # seconds without an answer before we consider the engine dead
AUTH_TIMEOUT = 5.0     # seconds to connect to and get an answer from Central


def authenticate(central_addr: tuple[str, int], ws_id: str) -> tuple[socket.socket, str]:
    """Connect to CENTRAL and send AUTH#<ws_id>. Returns (socket, location).
    Exits the program if rejected."""
    try:
        central = socket.create_connection(central_addr, timeout=AUTH_TIMEOUT)
        send(central, Message(OpCode.AUTH, [ws_id]))
        reply = recv(central)
        central.settimeout(None)
    except (OSError, ValueError) as e:  # refused, timed out, or not our protocol
        sys.exit(f"[WM_WS_M] cannot reach Central at {central_addr}: {e}")
    if reply.op != OpCode.OK:
        sys.exit(f"[WM_WS_M] Central rejected station {ws_id}: {reply.fields}")
    location = reply.fields[0]
    print(f"[WM_WS_M] authenticated with Central, location: {location}")
    return central, location


def check_engine(conn: socket.socket) -> bool:
    """Ask the engine "are you OK?". True if it answers OK, False if KO."""
    send(conn, Message(OpCode.HEALTH))
    reply = recv(conn)
    return reply.op == OpCode.OK


def report(central: socket.socket, op: OpCode, ws_id: str) -> None:
    """Tell CENTRAL about a health change. Exits if Central is gone."""
    try:
        send(central, Message(op, [ws_id]))
        reply = recv(central)
    except (OSError, ValueError) as e:
        sys.exit(f"[WM_WS_M] lost connection to Central ({e})")
    if reply.op != OpCode.OK:
        print(f"[WM_WS_M] Central did not accept {op}: {reply.fields}")


def update_health(old: bool | None, new: bool, central: socket.socket, ws_id: str) -> bool:
    """Act only when the health CHANGES. None = no engine seen yet."""
    if new and old is not True:  # first OK ever, or OK after a fault
        print("[WM_WS_M] engine OK" if old is None else "[WM_WS_M] RECOVERED: engine OK again")
        report(central, OpCode.RECOVERED, ws_id)  # Central marks us AVAILABLE
    elif not new and old is not False:
        print("[WM_WS_M] LEAK detected!")
        report(central, OpCode.LEAK, ws_id)
    return new


def watch_engine(conn: socket.socket, healthy: bool | None,
                 central: socket.socket, ws_id: str, location: str) -> bool:
    """Tell the engine who it is, then health-check it every second
    until its connection breaks."""
    with conn:
        try:
            send(conn, Message(OpCode.IDENTIFY, [ws_id, location]))
            recv(conn)  # the engine's OK
            while True:
                ok = check_engine(conn)
                healthy = update_health(healthy, ok, central, ws_id)
                time.sleep(HEALTH_INTERVAL)
        except (ConnectionError, TimeoutError, ValueError) as e:
            print(f"[WM_WS_M] engine not responding correctly ({e})")
            return update_health(healthy, False, central, ws_id)


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_M")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_E")
    parser.add_argument("central", type=host_port, help="WM_Central HOST:PORT")
    parser.add_argument("ws_id", help="ID of this watering station")
    args = parser.parse_args()

    central, location = authenticate(args.central, args.ws_id)  # socket kept open for reports

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
        healthy = watch_engine(conn, healthy, central, args.ws_id, location)
