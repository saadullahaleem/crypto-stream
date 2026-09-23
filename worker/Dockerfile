FROM python:3.12-slim
WORKDIR /app
# Dependencies come from pyproject.toml only.
COPY pyproject.toml .
RUN --mount=from=ghcr.io/astral-sh/uv:0.5,source=/uv,target=/bin/uv uv pip install --system --no-cache -r pyproject.toml
COPY *.py .
