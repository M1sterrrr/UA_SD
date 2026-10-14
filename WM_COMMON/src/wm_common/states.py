from enum import StrEnum


class WSState(StrEnum):
    """Possible states of a watering station (see the CENTRAL panel in the PDF)."""

    AVAILABLE = "AVAILABLE"            # green: working, waiting for a request
    WATERING = "WATERING"              # blinking green: valve open, water flowing
    LEAK = "LEAK"                      # red: fault detected, irrigation not allowed
    OUT_OF_SERVICE = "OUT_OF_SERVICE"  # orange: blocked by CENTRAL on purpose
    DISCONNECTED = "DISCONNECTED"      # gray: no connection to CENTRAL
