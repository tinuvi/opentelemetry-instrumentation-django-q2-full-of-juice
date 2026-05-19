"""OpenTelemetry instrumentation for django-q2."""

from opentelemetry_instrumentation_django_q2.instrumentor import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.version import __version__

__all__ = ["DjangoQ2Instrumentor", "__version__"]
