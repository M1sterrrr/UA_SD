# WaterManagement (SD 26/27, Universidad de Alicante)

A distributed simulation of a city irrigation network. Watering stations (WS) are
monitored and controlled by a central system; field operators (FO) request irrigation.

Author: Dawid Kałucki

## Components

| Module | Package | Role | Talks to |
|---|---|---|---|
| **WM_Central** | `WM_Central/` | SQLite DB of stations and operators, authorizes irrigation, web dashboard, admin commands | WS_M via **sockets** (server); WS_E and FO via **Kafka** |
| **WM_WS_M** (Monitor) | `WM_WS/WM_WS_M/` | Authenticates the station with Central, health-checks its Engine every second, reports leaks | Central (socket client), Engine (socket server) |
| **WM_WS_E** (Engine) | `WM_WS/WM_WS_E/` | Simulates the valve and flow meter, sends telemetry every second while watering | Monitor (socket client), Central (Kafka) |
| **WM_FO** (Operator) | `WM_FO/` | Requests irrigation by hand or from a file, shows progress and the final summary | Central (Kafka) |
| **WM_COMMON** | `WM_COMMON/` | Shared code: socket framing, message formats, Kafka helpers, station states | imported by all |

Station states: `AVAILABLE`, `WATERING`, `LEAK`, `OUT_OF_SERVICE`, `DISCONNECTED`.

## Protocols

**Sockets** (`wm_common/protocol.py`, `wm_common/messages.py`): every message is framed as
`<STX>data<ETX><LRC>` (LRC = XOR of the data bytes) and answered with `ACK`/`NACK`
(resent up to 3 times). The data is `OPCODE#field#field`, e.g. `AUTH#WS2`, `LEAK#WS2`, `PING#WS2`.

**Kafka** (`wm_common/kafka_msgs.py`): JSON messages on four topics.

| Topic | Written by | Read by | Messages |
|---|---|---|---|
| `wm.requests` | FO, WS_E | Central | `IRRIGATION_REQUEST` |
| `wm.ws.commands` | Central | WS_E | `START`, `STOP`, `BLOCK`, `ACTIVATE` |
| `wm.ws.events` | WS_E | Central, FO | `TELEMETRY`, `FINISHED` |
| `wm.fo.notifications` | Central | FO, WS_E | `REQUEST_STATUS`, `STATIONS` |

## Running

Requirements: Docker. For running without containers also [uv](https://docs.astral.sh/uv/) (Python 3.12).

### Everything in Docker (one machine)

```bash
docker compose up -d --build                 # Kafka, Central, stations WS2 and WS3
docker compose run --rm fo                   # an operator (OP1), interactive
docker compose run --rm fo kafka:19092 OP1 WM_FO/requests_example.txt   # from a file
docker compose attach engine-ws2             # engine menu: w / s / f  (leave: Ctrl+P Ctrl+Q)
docker compose down                          # stop (the database volume is kept)
```

Dashboard: <http://localhost:8080>

### Without containers (uv)

```bash
docker compose up -d kafka                   # only the broker
uv sync
uv run python -m wm_central.seed             # create and fill wm_central.db
uv run WM_Central 5500 localhost:9092        # each in its own terminal
uv run WM_WS_M    6001 localhost:5500 WS2
uv run WM_WS_E    localhost:9092 localhost:6001
uv run WM_FO      localhost:9092 OP1 [requests_file]
```

### Command-line arguments

| Module | Arguments |
|---|---|
| WM_Central | `<socket_port> <kafka_host:port> [--http-port N]` (default `$PORT` or 8080) |
| WM_WS_M | `<engine_port> <central_host:port> <ws_id>` |
| WM_WS_E | `<kafka_host:port> <monitor_host:port>` |
| WM_FO | `<kafka_host:port> <operator_id> [requests_file]` |

Requests file: one `<ws_id> <seconds>` per line; blank lines and `#` comments are skipped.

Central's admin console (and the dashboard buttons): `start <ws|all> <secs>`, `block <ws|all>`,
`activate <ws|all>`, `help`.

## AI usage

The prompts used with AI assistants are in [`Prompts.md`](Prompts.md).
