"""Minimal Django settings for the django-q2 OTel sample project."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("SAMPLE_DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DEBUG = os.environ.get("DJANGO_DEBUG", "1") == "1"
SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", uuid.uuid4().hex)
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.auth",
    "django_q",
    "tasks_app",
]

MIDDLEWARE: list[str] = []

ROOT_URLCONF = "sample.urls"
WSGI_APPLICATION = "sample.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "APP_DIRS": True,
        "OPTIONS": {},
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(DATA_DIR / "db.sqlite3"),
        "OPTIONS": {
            # WAL lets the web and worker processes read/write the same file safely.
            "init_command": "PRAGMA journal_mode=WAL;",
            "timeout": 20,
        },
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
USE_TZ = True
TIME_ZONE = "UTC"

Q_CLUSTER = {
    "name": os.environ.get("Q_CLUSTER_NAME", "sample-cluster"),
    "workers": int(os.environ.get("Q_CLUSTER_WORKERS", "2")),
    "timeout": 60,
    "retry": 90,
    "orm": "default",
    "sync": False,
    "catch_up": False,
    "save_limit": 200,
}

OTEL_SERVICE_NAME = os.environ.get("OTEL_SERVICE_NAME", "sample-project")
# OTLP endpoint env vars (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, etc.)
# are read directly by the OTLP exporter — they live in docker-compose.yml, not here.

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s %(levelname)s %(name)s %(message)s"},
    },
    "handlers": {
        "console": {"class": "logging.StreamHandler", "formatter": "default"},
    },
    "loggers": {
        "django_q": {"handlers": ["console"], "level": "INFO", "propagate": False},
        "opentelemetry_instrumentation_django_q2": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False,
        },
        "tasks_app": {"handlers": ["console"], "level": "INFO", "propagate": False},
    },
    "root": {"handlers": ["console"], "level": "INFO"},
}
