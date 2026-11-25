# Stany stacji podlewającej – jedno źródło prawdy dla całego systemu.
from enum import StrEnum


# Enum z 5 stanami stacji. Używa go Central: StationTable (stations.py) trzyma stan
# każdej stacji, server.py zamienia LEAK/RECOVERED od monitora na stany, dashboard.py
# liczy połączone stacje. Nazwy stanów są też klasami CSS w dashboard.html (kolor karty)
# i trafiają do FO w wiadomości STATIONS. StrEnum = wartość jest zwykłym stringiem,
# więc można ją wprost wysłać w JSON-ie i porównywać z tekstem.
class WSState(StrEnum):
    """Possible states of a watering station (see the CENTRAL panel in the PDF)."""

    AVAILABLE = "AVAILABLE"            # green: working, waiting for a request
    WATERING = "WATERING"              # blinking green: valve open, water flowing
    LEAK = "LEAK"                      # red: fault detected, irrigation not allowed
    OUT_OF_SERVICE = "OUT_OF_SERVICE"  # orange: blocked by CENTRAL on purpose
    DISCONNECTED = "DISCONNECTED"      # gray: no connection to CENTRAL
