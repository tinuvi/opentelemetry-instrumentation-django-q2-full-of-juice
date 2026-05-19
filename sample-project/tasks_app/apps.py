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
        from opentelemetry import metrics, trace
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
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

        # Short export interval so Playwright doesn't sit waiting for the next
        # tick — production deployments would use the default 60 s.
        meter_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=1_000,
        )
        meter_provider = MeterProvider(resource=resource, metric_readers=[meter_reader])
        metrics.set_meter_provider(meter_provider)

        DjangoQ2Instrumentor().instrument()
        _logger.info("OpenTelemetry initialized for service %s", settings.OTEL_SERVICE_NAME)
