"""Bootstrap OpenTelemetry and the django-q2 instrumentor inside every process."""

from __future__ import annotations

import logging

from django.apps import AppConfig
from django.conf import settings

_logger = logging.getLogger("tasks_app")


class TasksAppConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "tasks_app"

    def ready(self) -> None:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor

        from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor

        if getattr(trace.get_tracer_provider(), "_sample_initialized", False):
            return

        resource = Resource.create({"service.name": settings.OTEL_SERVICE_NAME})
        provider = TracerProvider(resource=resource)
        # SimpleSpanProcessor: synchronous flush, no background threads — survives
        # django-q2's fork model without re-init. Swap for BatchSpanProcessor +
        # a post_spawn re-init handler if you copy this into production.
        provider.add_span_processor(SimpleSpanProcessor(OTLPSpanExporter()))
        provider._sample_initialized = True
        trace.set_tracer_provider(provider)

        DjangoQ2Instrumentor().instrument()
        _logger.info("OpenTelemetry initialized for service %s", settings.OTEL_SERVICE_NAME)
