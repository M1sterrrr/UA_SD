# Komendy administratora Central: start / block / activate dla jednej stacji albo "all".
# Ten sam kod obsługuje dwa wejścia: konsolę w terminalu Central (console) i przyciski
# na dashboardzie (dashboard.py -> run_command).
"""CENTRAL's admin commands (PDF point 11): start, block, activate one or all stations."""

from confluent_kafka import Producer

from wm_central.stations import StationTable
from wm_common import kafka_msgs as km

# ID "operatora" używane, gdy podlewanie zleca sam Central; check_request
# w kafka_handler.py przepuszcza je, choć nie ma go w bazie.
ADMIN_OPERATOR = "CENTRAL"  # operator_id used when Central itself starts an irrigation
HELP = (
    "commands: start <ws_id|all> <seconds>, block <ws_id|all>, "
    "activate <ws_id|all>, help"
)


# Komenda "start": Central nie uruchamia stacji bezpośrednio – publikuje zwykłą prośbę
# IRRIGATION_REQUEST w wm.requests (tak jak FO). Odbiera ją jego własny wątek
# kafka_handler.consume i sprawdza jak każdą inną (stacja musi być AVAILABLE).
def start(producer: Producer, ws_id: str, duration: int) -> str:
    """Behave exactly like an operator: send a normal IRRIGATION_REQUEST, which our own
    Kafka thread then checks like any other (the station must be AVAILABLE)."""
    request_id = km.new_request_id()
    km.publish(producer, km.REQUESTS, ws_id, km.MsgType.IRRIGATION_REQUEST,
               request_id=request_id, operator_id=ADMIN_OPERATOR,
               ws_id=ws_id, duration=duration)
    return f"{ws_id}: irrigation request {request_id} sent"


# Komenda "block": oznacza stację w tabeli (table.block -> OUT_OF_SERVICE) i wysyła
# BLOCK do silnika przez wm.ws.commands. Silnik przerywa podlewanie (FINISHED
# z powodem BLOCKED) i nie podlewa, dopóki nie dostanie ACTIVATE.
def block(table: StationTable, producer: Producer, ws_id: str) -> str:
    """OUT_OF_SERVICE: ends any irrigation in progress (the engine gets BLOCK)."""
    if reason := table.block(ws_id):
        return f"{ws_id}: can't block ({reason})"
    km.publish(producer, km.WS_COMMANDS, ws_id, km.MsgType.BLOCK, ws_id=ws_id)
    return f"{ws_id}: blocked"


# Komenda "activate": cofa blokadę w tabeli (OUT_OF_SERVICE -> AVAILABLE) i wysyła
# ACTIVATE do silnika, który znów może podlewać.
def activate(table: StationTable, producer: Producer, ws_id: str) -> str:
    if reason := table.activate(ws_id):
        return f"{ws_id}: can't activate ({reason})"
    km.publish(producer, km.WS_COMMANDS, ws_id, km.MsgType.ACTIVATE, ws_id=ws_id)
    return f"{ws_id}: activated"


# Parsuje jedną linię komendy, np. "block WS2" albo "start all 30", i woła
# start/block/activate dla każdej stacji docelowej ("all" = table.ids()).
# Zwraca listę komunikatów. Wołana przez console() i dashboard (POST /api/command).
def run_command(line: str, table: StationTable, producer: Producer) -> list[str]:
    """Execute one admin command line. Returns one result message per station."""
    words = line.split()
    if not words or words[0] == "help":
        return [HELP]
    cmd, args = words[0].lower(), words[1:]
    if not args:
        return [HELP]
    # "all" means every station Central knows; disconnected ones will just say so
    targets = table.ids() if args[0].lower() == "all" else [args[0]]

    if cmd == "start" and len(args) == 2 and args[1].isdigit():
        return [start(producer, ws_id, int(args[1])) for ws_id in targets]
    if cmd == "block" and len(args) == 1:
        return [block(table, producer, ws_id) for ws_id in targets]
    if cmd == "activate" and len(args) == 1:
        return [activate(table, producer, ws_id) for ws_id in targets]
    return [HELP]


# Wątek konsoli: czyta linie z klawiatury terminala Central i wykonuje je przez
# run_command(). Gdy nie ma terminala (np. kontener na AWS ECS), input() rzuca
# EOFError i wątek po prostu się kończy – reszta Central działa dalej.
def console(table: StationTable, producer: Producer) -> None:
    """Runs in its own thread: admin commands typed into Central's terminal.
    (On Railway there is no terminal: stdin is closed and this thread just ends.)"""
    print(f"[WM_Central] admin {HELP}")
    while True:
        try:
            line = input()
        except EOFError:
            return
        for result in run_command(line, table, producer):
            print(f"[WM_Central] admin: {result}")
