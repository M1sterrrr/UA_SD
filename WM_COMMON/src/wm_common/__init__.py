# Pakiet wm_common – kod wspólny dla WSZYSTKICH aplikacji (Central, Monitor, Engine, FO).
# protocol.py   – ramki gniazd <STX>dane<ETX><LRC> + potwierdzenia ACK/NACK
# messages.py   – format danych w ramce: OPCODE#pole#pole (np. AUTH#WS2)
# kafka_msgs.py – tematy Kafki, typy wiadomości JSON, tworzenie producentów i konsumentów
# states.py     – stany stacji (AVAILABLE, WATERING, LEAK, OUT_OF_SERVICE, DISCONNECTED)
# cli.py        – zamiana argumentu "host:port" na krotkę
# Ten plik celowo jest pusty: wystarczy, że istnieje, żeby katalog był pakietem Pythona.
