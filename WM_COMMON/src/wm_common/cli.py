# Pomocnicze funkcje dla argumentów linii poleceń, wspólne dla wszystkich aplikacji.
import argparse


# Zamienia tekst "host:port" na krotkę ("host", port). Podawana jako type= w argparse
# w każdym programie: WM_Central (broker), WM_WS_M (adres Central), WM_WS_E (broker
# i monitor), WM_FO (broker). Wynik trafia potem do km.make_producer/make_consumer
# albo do socket.create_connection. rpartition(":") dzieli po OSTATNIM dwukropku.
# Zły format -> ArgumentTypeError, a argparse wypisuje czytelny błąd.
def host_port(text: str) -> tuple[str, int]:
    """Parse "host:port" into ("host", port). Used as an argparse type."""
    host, sep, port = text.rpartition(":")
    if not sep or not host or not port.isdigit():
        raise argparse.ArgumentTypeError(f"expected HOST:PORT, got {text!r}")
    return host, int(port)
