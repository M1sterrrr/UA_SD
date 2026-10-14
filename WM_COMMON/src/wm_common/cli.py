import argparse


def host_port(text: str) -> tuple[str, int]:
    """Parse "host:port" into ("host", port). Used as an argparse type."""
    host, sep, port = text.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected HOST:PORT, got {text!r}")
    return host, int(port)
