import argparse

from wm_common.cli import host_port


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_Central")
    parser.add_argument("port", type=int, help="port of the socket server for WM_WS_M")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    args = parser.parse_args()

    print(f"[WM_Central] listening port for monitors: {args.port}")
    print(f"[WM_Central] Kafka broker: {args.broker}")
