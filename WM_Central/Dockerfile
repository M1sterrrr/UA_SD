# Build from the repo root:  docker build -f WM_Central/Dockerfile -t wm-central .
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    WM_DB_PATH=/data/wm_central.db

# The whole workspace: wm-central needs wm-common, and uv.lock pins every version
COPY . .
RUN uv sync --frozen --no-dev --package wm-central

# The database lives in a volume, so it survives when the container is replaced
VOLUME /data
EXPOSE 5500 8080
# `docker stop` sends Ctrl+C, so our KeyboardInterrupt handler runs
STOPSIGNAL SIGINT
ENTRYPOINT ["WM_Central"]
