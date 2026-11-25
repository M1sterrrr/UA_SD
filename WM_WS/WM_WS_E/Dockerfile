# Obraz Dockera dla WM_WS_E (silnika stacji). Ten sam schemat co WM_Central: kopiujemy cały workspace,
# instalujemy tylko ten pakiet (uv sync --package) i ustawiamy jego komendę jako ENTRYPOINT.
# Jeden obraz dla wszystkich stacji – adres Kafki i monitora podaje się jako argumenty (command: w compose).
# Build from the repo root:  docker build -f WM_WS/WM_WS_E/Dockerfile -t wm-ws-e .
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY . .
RUN uv sync --frozen --no-dev --package wm-ws-e

STOPSIGNAL SIGINT
ENTRYPOINT ["WM_WS_E"]
