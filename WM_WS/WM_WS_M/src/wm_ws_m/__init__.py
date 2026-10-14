import argparse
import socket
import time

from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send

HEALTH_INTERVAL = 1.0  # seconds between health checks (PDF: every second)
HEALTH_TIMEOUT = 3.0   # seconds without an answer before we consider the engine dead


def check_engine(conn: socket.socket) -> bool:
    """Ask the engine "are you OK?". True if it answers OK, False if KO."""
    send(conn, Message(OpCode.HEALTH))
    reply = recv(conn)
    return reply.op == OpCode.OK


def update_health(old: bool | None, new: bool) -> bool:
    """Print only when the health CHANGES. None = no engine seen yet."""
    if new and old is not True:
        print("[WM_WS_M] engine OK" if old is None else "[WM_WS_M] RECOVERED: engine OK again")
    elif not new and old is not False:
        print("[WM_WS_M] LEAK detected!")
    return new


def watch_engine(conn: socket.socket, healthy: bool | None) -> bool:
    """Health-check one engine every second until its connection breaks."""
    with conn:
        while True:
            try:
                ok = check_engine(conn)
            except (ConnectionError, TimeoutError) as e:
                print(f"[WM_WS_M] engine not responding ({e})")
                return update_health(healthy, False)
            healthy = update_health(healthy, ok)
            time.sleep(HEALTH_INTERVAL)


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_M")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_E")
    parser.add_argument("central", type=host_port, help="WM_Central HOST:PORT")
    parser.add_argument("ws_id", help="ID of this watering station")
    args = parser.parse_args()

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
        healthy = watch_engine(conn, healthy)
