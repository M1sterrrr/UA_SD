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

    def get_state(self, ws_id: str) -> WSState | None:
        """Current state of a station, or None if Central doesn't know it."""
        with self._lock:
            station = self._stations.get(ws_id)
            return station.state if station else None

    def set_state(self, ws_id: str, state: WSState) -> None:
        with self._lock:
            self._stations[ws_id].state = state
        self.print_table()

    def print_table(self) -> None:
        with self._lock:
            lines = [f"[WM_Central]   {s.id:<5} {s.state:<15} {s.location}"
                     for s in sorted(self._stations.values(), key=lambda s: s.id)]
        print("\n".join(["[WM_Central] stations:", *lines]))
