FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.29 /uv /bin/uv

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY app ./app
COPY worker ./worker
COPY alembic ./alembic
COPY alembic.ini ./

RUN uv sync --frozen --no-dev && mkdir -p /app/logs

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

CMD ["flint", "api"]
