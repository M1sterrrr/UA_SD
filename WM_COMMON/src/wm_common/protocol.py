"""Socket framing: <STX><DATA><ETX><LRC> (recommended in the PDF, page 12)."""
import socket

STX = 0x02  # Start of TeXt: "a message begins here"
ETX = 0x03  # End of TeXt: "the data ends here"
ACK = 0x06  # ACKnowledge: "frame received correctly"
NACK = 0x15  # Negative ACKnowledge: "frame was corrupted, send it again"

MAX_RETRIES = 3


def lrc(data: bytes) -> int:
    """Longitudinal Redundancy Check: XOR of every byte of the data."""
    result = 0
    for byte in data:
        result ^= byte
    return result


def encode_frame(text: str) -> bytes:
    """"REGISTER#WS1" -> b"\\x02REGISTER#WS1\\x03<lrc>"."""
    data = text.encode()
    return bytes([STX]) + data + bytes([ETX]) + bytes([lrc(data)])


def _recv_byte(sock: socket.socket) -> int:
    """Read exactly one byte, or raise if the other side closed the connection."""
    b = sock.recv(1)
    if not b:
        raise ConnectionError("connection closed by peer")
    return b[0]


def recv_frame(sock: socket.socket) -> tuple[str, bool]:
    """Read ONE frame from the stream. Returns (text, lrc_ok)."""
    # 1. skip anything until the start of a frame
    while _recv_byte(sock) != STX:
        pass
    # 2. collect data bytes until ETX
    data = bytearray()
    while (b := _recv_byte(sock)) != ETX:
        data.append(b)
    # 3. the byte after ETX is the LRC the sender calculated
    received_lrc = _recv_byte(sock)
    return data.decode(), received_lrc == lrc(bytes(data))


def send_msg(sock: socket.socket, text: str) -> None:
    """Send a frame and wait for ACK. On NACK resend, up to MAX_RETRIES times."""
    frame = encode_frame(text)
    for _ in range(MAX_RETRIES):
        sock.sendall(frame)
        answer = _recv_byte(sock)
        if answer == ACK:
            return
        if answer != NACK:
            raise ConnectionError(f"expected ACK/NACK, got byte {answer:#04x}")
    raise ConnectionError(f"message rejected {MAX_RETRIES} times: {text!r}")


def recv_msg(sock: socket.socket) -> str:
    """Receive frames until one has a correct LRC; ACK it and return its text."""
    while True:
        text, ok = recv_frame(sock)
        if ok:
            sock.sendall(bytes([ACK]))
            return text
        sock.sendall(bytes([NACK]))  # corrupted: ask the sender to repeat
