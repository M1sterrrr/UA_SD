# Stan "na żywo" wszystkich stacji, trzymany w pamięci Central. Jedna StationTable jest
# dzielona przez WSZYSTKIE wątki (serwer gniazd, Kafka, admin, dashboard), dlatego każdy
# dostęp idzie przez blokadę (self._lock) – inaczej np. dwie prośby naraz mogłyby
# zarezerwować tę samą stację.
"""CENTRAL's live view of every station, shared by all threads."""
import sqlite3
import threading
from dataclasses import dataclass

from wm_common.states import WSState


# Dane jednej stacji: stałe (id, lokalizacja) i zmienne (stan, bieżąca prośba, przepływ,
# objętość, flaga blokady). blocked to rozkaz Central, niezależny od zdrowia stacji:
# zablokowana stacja po naprawie pokazuje OUT_OF_SERVICE, a nie AVAILABLE.
@dataclass
class Station:
    id: str
    location: str
    state: WSState = WSState.DISCONNECTED
    request_id: str | None = None   # the irrigation it is serving (WATERING only)
    operator_id: str | None = None
    flow: float = 0.0               # L/min, latest telemetry
    volume: float = 0.0             # L since the irrigation started
    blocked: bool = False           # Central's order "out of service" (not a health state)


# Bezpieczna wątkowo tabela stacji. Metody zwracają powód odmowy (str) albo None/True.
# print_table() jest wołane PO wyjściu z bloku `with self._lock`, bo samo bierze blokadę,
# a threading.Lock nie jest wielowejściowy (drugie acquire w tym samym wątku = zakleszczenie).
class StationTable:
    """Thread-safe table of stations. Every access goes through the lock."""

    # Buduje tabelę z wierszy bazy (db.list_stations); na starcie każda stacja jest
    # DISCONNECTED, dopóki jej monitor się nie połączy.
    def __init__(self, rows: list[sqlite3.Row]) -> None:
        self._lock = threading.Lock()
        # PDF step 1: known from the DB, but DISCONNECTED until they connect
        self._stations = {r["id"]: Station(r["id"], r["location"]) for r in rows}

    # Wołana przez server.handle_monitor po udanym AUTH. Stacja startuje jako LEAK
    # (silnik jeszcze nie potwierdził zdrowia); monitor potem wyśle RECOVERED -> AVAILABLE.
    # False, gdy stacja ma już połączony monitor. Blokada (blocked) przetrwa ponowne połączenie.
    def connect(self, ws_id: str, location: str) -> bool:
        """Mark a station as connected. False if it is already connected.
        It starts as LEAK: its engine has not answered yet. The monitor sends
        RECOVERED once the engine's first health check is OK."""
        with self._lock:
            current = self._stations.get(ws_id)
            if current is not None and current.state != WSState.DISCONNECTED:
                return False
            blocked = current.blocked if current else False  # a block survives reconnects
            self._stations[ws_id] = Station(ws_id, location, WSState.LEAK, blocked=blocked)
        self.print_table()
        return True

    # Sprawdza i rezerwuje stację w JEDNYM kroku pod blokadą (AVAILABLE -> WATERING),
    # więc dwie prośby naraz nie dostaną tej samej stacji. Wołana z
    # kafka_handler.handle_request. Zwraca powód odmowy albo None.
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

    # Zapisuje najnowszy przepływ i objętość (TELEMETRY od silnika, przez
    # kafka_handler.handle_telemetry). Ignoruje spóźnione wiadomości z innej prośby.
    def set_telemetry(self, ws_id: str, request_id: str, flow: float, volume: float) -> bool:
        """Store the latest readings. False if the station isn't watering that request
        (e.g. a late message from an irrigation that already ended)."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None or station.request_id != request_id:
                return False
            station.flow, station.volume = flow, volume
            return True

    # Koniec podlewania (FINISHED, przez kafka_handler.handle_finished):
    # WATERING -> AVAILABLE, albo -> LEAK, gdy powodem była awaria.
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

    # Ustawia stan wprost: LEAK/RECOVERED od monitora (server.handle_report) oraz
    # DISCONNECTED po zerwaniu połączenia (server.handle_monitor). Zwraca poprzedni stan,
    # żeby handle_report wiedział, czy awaria wydarzyła się w trakcie podlewania.
    def set_state(self, ws_id: str, state: WSState) -> WSState:
        """Change a station's state. Returns the state it had before.
        A blocked station that becomes healthy shows OUT_OF_SERVICE, not AVAILABLE."""
        with self._lock:
            station = self._stations[ws_id]
            if state == WSState.AVAILABLE and station.blocked:
                state = WSState.OUT_OF_SERVICE
            previous, station.state = station.state, state
        self.print_table()
        return previous

    # Rozkaz "wyłącz z użytku" z admin.block. Niepołączonej stacji nie da się zablokować.
    def block(self, ws_id: str) -> str | None:
        """Central's "out of service" order. Returns why it can't be done, or None."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None:
                return "unknown station"
            if station.state == WSState.DISCONNECTED:
                return "not connected"
            station.blocked = True
            if station.state in (WSState.AVAILABLE, WSState.WATERING):
                station.state = WSState.OUT_OF_SERVICE  # a LEAK stays LEAK (more urgent)
        self.print_table()
        return None

    # Cofnięcie blokady z admin.activate: OUT_OF_SERVICE -> AVAILABLE.
    def activate(self, ws_id: str) -> str | None:
        """Undo a block. Returns why it can't be done, or None."""
        with self._lock:
            station = self._stations.get(ws_id)
            if station is None:
                return "unknown station"
            if station.state == WSState.DISCONNECTED:
                return "not connected"
            station.blocked = False
            if station.state == WSState.OUT_OF_SERVICE:
                station.state = WSState.AVAILABLE
        self.print_table()
        return None

    # Posortowane ID wszystkich stacji – admin.run_command używa ich dla celu "all".
    def ids(self) -> list[str]:
        with self._lock:
            return sorted(self._stations)

    # Publiczne dane stacji (id, lokalizacja, stan) do wiadomości STATIONS, którą
    # kafka_handler.broadcast_stations wysyła operatorom (komenda "list" w FO).
    def snapshot(self) -> list[dict]:
        """A copy of every station's public info, safe to send to other apps."""
        with self._lock:
            return [{"id": s.id, "location": s.location, "state": s.state}
                    for s in sorted(self._stations.values(), key=lambda s: s.id)]

    # Pełne dane stacji (z operatorem, przepływem, objętością) dla dashboardu
    # (GET /api/state w dashboard.py).
    def details(self) -> list[dict]:
        """Everything about every station, for Central's own dashboard."""
        with self._lock:
            return [{"id": s.id, "location": s.location, "state": s.state,
                     "operator_id": s.operator_id, "request_id": s.request_id,
                     "flow": s.flow, "volume": s.volume}
                    for s in sorted(self._stations.values(), key=lambda s: s.id)]

    # Wypisuje tabelę stacji w terminalu Central – wołana po każdej zmianie stanu.
    def print_table(self) -> None:
        with self._lock:
            lines = [f"[WM_Central]   {s.id:<5} {s.state:<15} {s.location}"
                     + (f"  [request {s.request_id} by {s.operator_id}]"
                        if s.state == WSState.WATERING else "")
                     for s in sorted(self._stations.values(), key=lambda s: s.id)]
        print("\n".join(["[WM_Central] stations:", *lines]))
