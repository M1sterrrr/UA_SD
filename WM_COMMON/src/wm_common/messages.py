# Druga warstwa protokołu gniazd: CO jest w środku ramki. Dane to tekst OPCODE#pole#pole,
# np. "AUTH#WS2". Moduł zamienia taki tekst na obiekt Message i z powrotem, a funkcje
# send()/recv() łączą to z ramkami z protocol.py. Używają go: Central (server.py),
# WM_WS_M (rozmowa z Central i z silnikiem) oraz WM_WS_E (odpowiedzi dla monitora).
"""Structure of the DATA part of a socket frame: OPCODE#field1#field2..."""
import socket
from dataclasses import dataclass, field
from enum import StrEnum

from wm_common.protocol import recv_msg, send_msg

SEPARATOR = "#"


# Wszystkie operacje przesyłane gniazdami; komentarze obok mówią, kto do kogo wysyła.
# Każda wiadomość dostaje odpowiedź OK albo KO (wzorzec zapytanie–odpowiedź).
class OpCode(StrEnum):
    """Every operation that can travel over our sockets."""

    # WM_WS_M -> WM_Central
    AUTH = "AUTH"            # AUTH#<ws_id>          "I am station <ws_id>"
    LEAK = "LEAK"            # LEAK#<ws_id>          "my station has a fault"
    RECOVERED = "RECOVERED"  # RECOVERED#<ws_id>     "the fault is fixed"
    PING = "PING"            # PING#<ws_id>          "still alive" (heartbeat every few s)
    # WM_WS_M -> WM_WS_E
    IDENTIFY = "IDENTIFY"    # IDENTIFY#<ws_id>#<location>   "this is who you are"
    HEALTH = "HEALTH"        # HEALTH                "are you OK?"
    # Replies (any direction)
    OK = "OK"                # OK[#<info>]
    KO = "KO"                # KO[#<reason>]


# Jedna wiadomość: kod operacji + lista pól tekstowych. @dataclass sam generuje
# __init__, __repr__ i porównywanie (==).
@dataclass
class Message:
    op: OpCode
    fields: list[str] = field(default_factory=list)

    # Message -> tekst "OP#pole#pole". Pilnuje, żeby żadne pole nie zawierało "#"
    # (zepsułoby podział). Używana w send() i w logach Central (server.py).
    def to_text(self) -> str:
        """Message(AUTH, ["WS1"]) -> "AUTH#WS1"."""
        for f in self.fields:
            if SEPARATOR in f:
                raise ValueError(f"field {f!r} must not contain {SEPARATOR!r}")
        return SEPARATOR.join([self.op, *self.fields])

    # Tekst -> Message. Nieznany kod operacji rzuca ValueError (przez OpCode(op)) – łapią go
    # pętle w server.py, WM_WS_M i WM_WS_E. Wołana przez recv().
    @classmethod
    def from_text(cls, text: str) -> "Message":
        """"AUTH#WS1" -> Message(AUTH, ["WS1"]). Raises ValueError on unknown opcode."""
        op, *fields = text.split(SEPARATOR)
        return cls(OpCode(op), fields)


# Wysyła obiekt Message: to_text() + send_msg() (ramka i czekanie na ACK).
# Główna funkcja wysyłania w Central, monitorze i silniku.
def send(sock: socket.socket, message: Message) -> None:
    """Send a Message as a framed, ACKed socket message."""
    send_msg(sock, message.to_text())


# Odbiera jedną wiadomość: recv_msg() (ramka + ACK) + from_text(). Blokuje, dopóki coś
# nie przyjdzie albo nie minie timeout gniazda (wtedy TimeoutError).
def recv(sock: socket.socket) -> Message:
    """Receive one framed socket message and parse it into a Message."""
    return Message.from_text(recv_msg(sock))
