import argparse

from wm_common.cli import host_port


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_M")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_E")
    parser.add_argument("central", type=host_port, help="WM_Central HOST:PORT")
    parser.add_argument("ws_id", help="ID of this watering station")
    args = parser.parse_args()

    print(f"[WM_WS_M] listening port for engine: {args.port}")
    print(f"[WM_WS_M] Central: {args.central}")
    print(f"[WM_WS_M] station ID: {args.ws_id}")
