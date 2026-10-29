"""CENTRAL's web dashboard: one HTML page + a JSON endpoint it polls every second."""

import json
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from wm_central import kafka_handler
from wm_central.stations import StationTable
from wm_common.states import WSState

PAGE = Path(__file__).with_name("dashboard.html")  # lives next to this file


def make_handler(table: StationTable, started: float) -> type[BaseHTTPRequestHandler]:
    """Build the request handler class. The server creates one handler object per
    request, so the shared things (table, start time) come in through this closure."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/":
                self.reply(200, "text/html; charset=utf-8", PAGE.read_bytes())
            elif self.path == "/api/state":
                stations = table.details()
                state = {
                    "central": {
                        "uptime": int(time.monotonic() - started),
                        "connected": sum(s["state"] != WSState.DISCONNECTED
                                         for s in stations),
                    },
                    "stations": stations,
                    "requests": list(kafka_handler.recent_requests),
                }
                self.reply(200, "application/json", json.dumps(state).encode())
            else:
                self.reply(404, "text/plain", b"not found")

        def reply(self, code: int, content_type: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            pass  # don't print a line for every request (the page asks every second)

    return Handler


def serve(port: int, table: StationTable) -> None:
    """Runs in its own thread. ThreadingHTTPServer answers each request in a new thread."""
    httpd = ThreadingHTTPServer(("0.0.0.0", port), make_handler(table, time.monotonic()))
    print(f"[WM_Central] dashboard on http://localhost:{port}")
    httpd.serve_forever()
