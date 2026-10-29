"""CENTRAL's live view of every station, shared by all threads."""
import sqlite3
import threading
from dataclasses import dataclass

from wm_common.states import WSState


@dataclass
class Station:
    id: str
    location: str
    state: WSState = WSState.DISCONNECTED
    request_id: str | None = None   # the irrigation it is serving (WATERING only)
    operator_id: str | None = None
    flow: float = 0.0               # L/min, latest telemetry
    volume: float = 0.0             # L since the irrigation started


class StationTable:
    """Thread-safe table of stations. Every access goes through the lock."""

    def __init__(self, rows: list[sqlite3.Row]) -> None:
        self._lock = threading.Lock()
        # PDF step 1: known from the DB, but DISCONNECTED until they connect
        self._stations = {r["id"]: Station(r["id"], r["location"]) for r in rows}

    def connect(self, ws_id: str, location: str) -> bool:
        """Mark a station as connected. False if it is already connected.
        It starts as LEAK: its engine has not answered yet. The monitor sends
        RECOVERED once the engine's first health check is OK."""
        with self._lock:
            current = self._stations.get(ws_id)
            if current is not None and current.state != WSState.DISCONNECTED:
                return False
            self._stations[ws_id] = Station(ws_id, location, WSState.LEAK)
        self.print_table()
        return True

    def reserve(self, ws_id: str, request_id: str, operator_id: str) -> str | None:
        """Check AND claim in one locked step: AVAILABLE -> WATERING.
        Returns why the station can't be used, or None if it is now ours."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None:
                return "unknown station"
            if station.state != WSState.AVAILABLE:
                return f"station is {station.state}"
            station.state = WSState.WATERING
            station.request_id = request_id
            station.operator_id = operator_id
            station.flow = station.volume = 0.0
        self.print_table()
        return None

    def set_telemetry(self, ws_id: str, request_id: str, flow: float, volume: float) -> bool:
        """Store the latest readings. False if the station isn't watering that request
        (e.g. a late message from an irrigation that already ended)."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None or station.request_id != request_id:
                return False
            station.flow, station.volume = flow, volume
            return True

    def finish(self, ws_id: str, request_id: str, leaked: bool) -> bool:
        """The irrigation `request_id` ended: WATERING -> AVAILABLE, or -> LEAK if it
        ended because of a fault. False if the station wasn't serving that request."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None or station.request_id != request_id:
                return False
            if station.state == WSState.WATERING:  # a LEAK meanwhile stays a LEAK
                station.state = WSState.LEAK if leaked else WSState.AVAILABLE
            station.request_id = station.operator_id = None
            station.flow = 0.0
        self.print_table()
        return True

    def set_state(self, ws_id: str, state: WSState) -> WSState:
        """Change a station's state. Returns the state it had before."""
        with self._lock:
            station = self._stations[ws_id]
            previous, station.state = station.state, state
        self.print_table()
        return previous

    def snapshot(self) -> list[dict]:
        """A copy of every station's public info, safe to send to other apps."""
        with self._lock:
            return [{"id": s.id, "location": s.location, "state": s.state}
                    for s in sorted(self._stations.values(), key=lambda s: s.id)]

    def print_table(self) -> None:
        with self._lock:
            lines = [f"[WM_Central]   {s.id:<5} {s.state:<15} {s.location}"
                     + (f"  [request {s.request_id} by {s.operator_id}]"
                        if s.state == WSState.WATERING else "")
                     for s in sorted(self._stations.values(), key=lambda s: s.id)]
        print("\n".join(["[WM_Central] stations:", *lines]))
