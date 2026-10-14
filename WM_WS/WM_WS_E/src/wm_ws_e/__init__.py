import argparse
import socket
import threading

from wm_common.cli import host_port
from wm_common.messages import Message, OpCode, recv, send


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


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_E")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("monitor", type=host_port, help="WM_WS_M HOST:PORT")
    args = parser.parse_args()

    fault = threading.Event()  # shared between the keyboard thread and the main thread
    threading.Thread(target=keyboard_loop, args=(fault,), daemon=True).start()

    sock = socket.create_connection(args.monitor)
    print(f"[WM_WS_E] connected to monitor at {args.monitor}")
    try:
        while True:
            msg = recv(sock)
            if msg.op == OpCode.HEALTH:
                send(sock, Message(OpCode.KO if fault.is_set() else OpCode.OK))
    except ConnectionError:
        print("[WM_WS_E] monitor closed the connection")
    finally:
        sock.close()
