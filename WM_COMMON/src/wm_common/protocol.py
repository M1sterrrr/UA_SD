# NAJNIŻSZA warstwa komunikacji przez gniazda TCP. TCP to strumień bajtów bez granic
# wiadomości, więc każdą wiadomość pakujemy w ramkę <STX>dane<ETX><LRC>. Odbiorca
# sprawdza LRC i odpowiada jednym bajtem: ACK (dotarła cała) albo NACK (uszkodzona,
# wyślij jeszcze raz). Z tego modułu korzysta tylko messages.py; wyżej (server.py
# w Central, WM_WS_M, WM_WS_E) nikt nie dotyka bajtów bezpośrednio.
"""Socket framing: <STX><DATA><ETX><LRC> (recommended in the PDF, page 12)."""

import socket

STX = 0x02  # Start of TeXt: "a message begins here"
ETX = 0x03  # End of TeXt: "the data ends here"
ACK = 0x06  # ACKnowledge: "frame received correctly"
NACK = 0x15  # Negative ACKnowledge: "frame was corrupted, send it again"

MAX_RETRIES = 3


# Suma kontrolna LRC = XOR wszystkich bajtów danych. Nadawca dokleja ją
# w encode_frame(), odbiorca liczy ją ponownie w recv_frame() i porównuje.
def lrc(data: bytes) -> int:
    """Longitudinal Redundancy Check: XOR of every byte of the data."""
    result = 0
    for byte in data:
        result ^= byte
    return result


# Buduje gotową ramkę bajtów z tekstu: STX + dane + ETX + LRC. Wołana przez send_msg().
def encode_frame(text: str) -> bytes:
    """ "REGISTER#WS1" -> b"\\x02REGISTER#WS1\\x03<lrc>"."""
    data = text.encode()
    return bytes([STX]) + data + bytes([ETX]) + bytes([lrc(data)])


# Czyta dokładnie 1 bajt z gniazda. Pusty wynik recv() znaczy, że druga strona zamknęła
# połączenie -> ConnectionError (łapią go pętle w Central, monitorze i silniku).
# Używana przez recv_frame() (bajty ramki) i send_msg() (czekanie na ACK/NACK).
def _recv_byte(sock: socket.socket) -> int:
    """Read exactly one byte, or raise if the other side closed the connection."""
    b = sock.recv(1)
    if not b:
        raise ConnectionError("connection closed by peer")
    return b[0]


# Czyta JEDNĄ ramkę: pomija śmieci aż do STX, zbiera dane do ETX, potem czyta LRC.
# Zwraca (tekst, czy_LRC_się_zgadza). Wołana tylko przez recv_msg().
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


# Wysyła tekst jako ramkę i czeka na ACK. Po NACK wysyła ponownie (max MAX_RETRIES = 3
# razy), potem rzuca ConnectionError. Wołana przez messages.send(). Uwaga: ACK mówi
# tylko "ramka doszła cała" – odpowiedź aplikacji (OK/KO) to osobna, kolejna wiadomość.
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


# Odbiera ramki, aż trafi się poprawna (zgodne LRC); wtedy odsyła ACK i zwraca tekst.
# Na uszkodzoną ramkę odsyła NACK, a nadawca (send_msg) ją powtórzy.
# Wołana przez messages.recv().
def recv_msg(sock: socket.socket) -> str:
    """Receive frames until one has a correct LRC; ACK it and return its text."""
    while True:
        text, ok = recv_frame(sock)
        if ok:
            sock.sendall(bytes([ACK]))
            return text
        sock.sendall(bytes([NACK]))  # corrupted: ask the sender to repeat
