"""Instrumentor entry point — wires django-q2 signals to OpenTelemetry spans."""

import logging
from collections.abc import Collection

from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor

from opentelemetry_instrumentation_django_q2.package import _instruments
from opentelemetry_instrumentation_django_q2.version import __version__

_logger = logging.getLogger("opentelemetry_instrumentation_django_q2")


class DjangoQ2Instrumentor(BaseInstrumentor):
    """Connect django-q2's signals to OpenTelemetry spans (producer / consumer)."""

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs) -> None:
        tracer_provider = kwargs.get("tracer_provider")
        self._tracer = trace.get_tracer(__name__, __version__, tracer_provider)
        # Signal wiring will be implemented here:
        #   - django_q.signals.pre_enqueue           -> start PRODUCER span, inject carrier into task dict, end span
        #   - django_q.signals.post_spawn            -> per-worker SDK init hook
        #   - django_q.signals.pre_execute           -> extract carrier, start CONSUMER span, attach as current
        #   - django_q.signals.post_execute_in_worker-> set status, end CONSUMER span, detach context
        _logger.debug("DjangoQ2Instrumentor: instrumented (signal wiring pending).")

    def _uninstrument(self, **kwargs) -> None:
        _logger.debug("DjangoQ2Instrumentor: uninstrumented (signal disconnect pending).")
