FROM ghcr.io/astral-sh/uv:0.11.8 AS uv

FROM python:3.14-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM python:3.14-slim AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN groupadd --system gateway \
    && useradd --system --gid gateway --home-dir /app gateway

COPY --from=builder --chown=gateway:gateway /app/.venv /app/.venv
COPY --chown=gateway:gateway app ./app
COPY --chown=gateway:gateway config/gateway.example.yaml ./config/gateway.example.yaml
COPY --chown=gateway:gateway gateway.py ./gateway.py

USER gateway

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=10s --retries=6 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/v1/models', timeout=2).close()"]

CMD ["uvicorn", "gateway:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
