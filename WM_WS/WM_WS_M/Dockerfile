# Build from the repo root:  docker build -f WM_WS/WM_WS_M/Dockerfile -t wm-ws-m .
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

COPY . .
RUN uv sync --frozen --no-dev --package wm-ws-m

STOPSIGNAL SIGINT
ENTRYPOINT ["WM_WS_M"]
