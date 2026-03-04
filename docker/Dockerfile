FROM python:3.12-slim as builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install python dependencies builder
COPY pyproject.toml .
RUN pip install hatchling && pip install .[dev]

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy python dependencies
COPY --from=builder /usr/local/lib/python3.12/site-packages/ /usr/local/lib/python3.12/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

COPY src/ src/

EXPOSE 8000

CMD ["uvicorn", "src.compliance_gate.main:app", "--host", "0.0.0.0", "--port", "8000"]
