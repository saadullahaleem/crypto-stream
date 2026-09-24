FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
RUN --mount=from=ghcr.io/astral-sh/uv:0.5,source=/uv,target=/bin/uv uv pip install --system --no-cache -r pyproject.toml
COPY server.py fluss_source.py ./
COPY static ./static
CMD ["python", "-u", "server.py"]
