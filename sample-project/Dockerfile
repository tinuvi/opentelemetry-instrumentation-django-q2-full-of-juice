# Build context MUST be the repo root so we can install the library from source.
# See sample-project/docker-compose.yml for the wiring.
FROM python:3.14-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /lib

# --- Install the library (editable) from the workspace source ---
RUN pip install --upgrade pip && \
    pip install poetry==2.4.1 && \
    poetry config virtualenvs.create false --local

COPY pyproject.toml poetry.lock LICENSE README.md ./
COPY opentelemetry_instrumentation_django_q2 ./opentelemetry_instrumentation_django_q2

# The library no longer depends on any `django_q` provider at runtime (issue #3),
# so `--only main` installs zero of them — the sample must pick one explicitly.
# This upstream image chooses `django-q2`; the juice image (Dockerfile.juice)
# chooses the fork instead.
RUN poetry install --no-root --only main && \
    pip install "django-q2~=1.10" && \
    pip install --no-deps -e .

# --- Sample-only runtime deps (OTLP gRPC exporter to Jaeger) ---
# Kept out of the library's pyproject — it's the sample that needs to ship spans,
# not the library itself.
RUN pip install "opentelemetry-exporter-otlp-proto-grpc~=1.30"

# --- Add the sample project on top ---
WORKDIR /app
COPY sample-project ./

RUN chmod +x ./scripts/*.sh
