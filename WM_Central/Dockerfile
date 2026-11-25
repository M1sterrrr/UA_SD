# Przepis na obraz Dockera dla WM_Central (budowany z katalogu głównego repo, bo potrzebuje
# też WM_COMMON i uv.lock). Ten sam obraz buduje GitHub Actions (deploy.yml), wysyła na
# Docker Hub, a stamtąd pobiera go AWS ECS.
# Build from the repo root:  docker build -f WM_Central/Dockerfile -t wm-central .
FROM python:3.12-slim
# python:3.12-slim = mały Linux z Pythonem; program uv kopiujemy z jego oficjalnego obrazu.
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app
# PYTHONUNBUFFERED=1 -> print() od razu widać w logach; PATH -> komendy z .venv
# (np. WM_Central) działają bez "uv run"; WM_DB_PATH -> baza na wolumenie /data.
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    WM_DB_PATH=/data/wm_central.db

# The whole workspace: wm-central needs wm-common, and uv.lock pins every version
COPY . .
# Instaluje tylko wm-central (+ wm-common) w wersjach z uv.lock (--frozen), bez narzędzi dev.
RUN uv sync --frozen --no-dev --package wm-central && mkdir -p /data

# The database lives in a volume, so it survives when the container is replaced
VOLUME /data
EXPOSE 5500 8080
# `docker stop` sends Ctrl+C, so our KeyboardInterrupt handler runs
STOPSIGNAL SIGINT
# Kontener uruchamia komendę WM_Central. Argumenty (np. "5500 kafka:19092") dopisuje
# docker-compose (command:), a na AWS ECS biorą się ze zmiennych środowiskowych (WM_KAFKA).
ENTRYPOINT ["WM_Central"]
