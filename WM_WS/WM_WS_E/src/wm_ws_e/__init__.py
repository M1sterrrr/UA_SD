import argparse

from wm_common.cli import host_port


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_WS_E")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("monitor", type=host_port, help="WM_WS_M HOST:PORT")
    args = parser.parse_args()

    print(f"[WM_WS_E] Kafka broker: {args.broker}")
    print(f"[WM_WS_E] Monitor: {args.monitor}")
