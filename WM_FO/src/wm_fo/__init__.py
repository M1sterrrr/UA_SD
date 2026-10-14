import argparse

from wm_common.cli import host_port


def main() -> None:
    parser = argparse.ArgumentParser(prog="WM_FO")
    parser.add_argument("broker", type=host_port, help="Kafka bootstrap server HOST:PORT")
    parser.add_argument("operator_id", help="ID of this operator")
    parser.add_argument("file", nargs="?", help="optional file with irrigation requests")
    args = parser.parse_args()

    print(f"[WM_FO] Kafka broker: {args.broker}")
    print(f"[WM_FO] operator ID: {args.operator_id}")
    print(f"[WM_FO] requests file: {args.file}")
