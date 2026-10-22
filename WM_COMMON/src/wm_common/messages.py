"""Structure of the DATA part of a socket frame: OPCODE#field1#field2..."""
import socket
from dataclasses import dataclass, field
from enum import StrEnum

from wm_common.protocol import recv_msg, send_msg

SEPARATOR = "#"


class OpCode(StrEnum):
    """Every operation that can travel over our sockets."""

    # WM_WS_M -> WM_Central
    AUTH = "AUTH"            # AUTH#<ws_id>          "I am station <ws_id>"
    LEAK = "LEAK"            # LEAK#<ws_id>          "my station has a fault"
    RECOVERED = "RECOVERED"  # RECOVERED#<ws_id>     "the fault is fixed"
    # WM_WS_M -> WM_WS_E
    IDENTIFY = "IDENTIFY"    # IDENTIFY#<ws_id>#<location>   "this is who you are"
    HEALTH = "HEALTH"        # HEALTH                "are you OK?"
    # Replies (any direction)
    OK = "OK"                # OK[#<info>]
    KO = "KO"                # KO[#<reason>]


@dataclass
class Message:
    op: OpCode
    fields: list[str] = field(default_factory=list)

    def to_text(self) -> str:
        """Message(AUTH, ["WS1"]) -> "AUTH#WS1"."""
        for f in self.fields:
            if SEPARATOR in f:
                raise ValueError(f"field {f!r} must not contain {SEPARATOR!r}")
        return SEPARATOR.join([self.op, *self.fields])

    @classmethod
    def from_text(cls, text: str) -> "Message":
        """"AUTH#WS1" -> Message(AUTH, ["WS1"]). Raises ValueError on unknown opcode."""
        op, *fields = text.split(SEPARATOR)
        return cls(OpCode(op), fields)


def send(sock: socket.socket, message: Message) -> None:
    """Send a Message as a framed, ACKed socket message."""
    send_msg(sock, message.to_text())


def recv(sock: socket.socket) -> Message:
    """Receive one framed socket message and parse it into a Message."""
    return Message.from_text(recv_msg(sock))
