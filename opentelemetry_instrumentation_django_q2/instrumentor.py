"""Instrumentor entry point — wires django-q2 signals to OpenTelemetry spans."""

from __future__ import annotations

import logging
from collections.abc import Collection
from typing import Any

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION,
    MESSAGING_SYSTEM,
)
from opentelemetry.trace.status import Status, StatusCode

from opentelemetry_instrumentation_django_q2 import utils
from opentelemetry_instrumentation_django_q2.package import _instruments
from opentelemetry_instrumentation_django_q2.utils import (
    OTEL_CARRIER_KEY,
    attach_task_context,
    clear_task_context,
    describe_func,
    detach_task_context,
)
from opentelemetry_instrumentation_django_q2.version import __version__

_logger = logging.getLogger("opentelemetry_instrumentation_django_q2")

_MESSAGING_SYSTEM_NAME = "django_q2"
_DEFAULT_DESTINATION = "default"
_PRODUCER_OP = "publish"
_CONSUMER_OP = "process"


class DjangoQ2Instrumentor(BaseInstrumentor):
    """Connect django-q2's signals to OpenTelemetry spans (producer / consumer)."""

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _instrument(self, **kwargs) -> None:
        from django_q.signals import (
            post_execute_in_worker,
            post_spawn,
            pre_enqueue,
            pre_execute,
        )

        tracer_provider = kwargs.get("tracer_provider")
        self._tracer = trace.get_tracer(__name__, __version__, tracer_provider)

        pre_enqueue.connect(self._on_pre_enqueue, weak=False)
        pre_execute.connect(self._on_pre_execute, weak=False)
        post_execute_in_worker.connect(self._on_post_execute_in_worker, weak=False)
        post_spawn.connect(self._on_post_spawn, weak=False)
        _logger.debug("DjangoQ2Instrumentor instrumented")

    def _uninstrument(self, **kwargs) -> None:
        from django_q.signals import (
            post_execute_in_worker,
            post_spawn,
            pre_enqueue,
            pre_execute,
        )

        pre_enqueue.disconnect(self._on_pre_enqueue)
        pre_execute.disconnect(self._on_pre_execute)
        post_execute_in_worker.disconnect(self._on_post_execute_in_worker)
        post_spawn.disconnect(self._on_post_spawn)
        clear_task_context()
        _logger.debug("DjangoQ2Instrumentor uninstrumented")

    def _on_pre_enqueue(self, sender: Any, task: dict, **_: Any) -> None:
        func_repr = describe_func(task.get("func"))
        span = self._tracer.start_span(
            f"async_task/{func_repr}",
            kind=trace.SpanKind.PRODUCER,
        )
        try:
            if span.is_recording():
                span.set_attribute(MESSAGING_SYSTEM, _MESSAGING_SYSTEM_NAME)
                span.set_attribute(MESSAGING_OPERATION, _PRODUCER_OP)
                span.set_attribute(MESSAGING_DESTINATION_NAME, task.get("cluster") or _DEFAULT_DESTINATION)
                task_id = task.get("id")
                if task_id is not None:
                    span.set_attribute(MESSAGING_MESSAGE_ID, task_id)
                if task.get("name"):
                    span.set_attribute("django_q2.task.name", task["name"])
                span.set_attribute("django_q2.func", func_repr)
                if task.get("group"):
                    span.set_attribute("django_q2.group", task["group"])

            with trace.use_span(span, end_on_exit=False):
                carrier = task.setdefault(OTEL_CARRIER_KEY, {})
                inject(carrier)
        except Exception:
            _logger.exception("Failed to record PRODUCER span for task %s", task.get("id"))
        finally:
            span.end()

    def _on_pre_execute(self, sender: Any, func: Any, task: dict, **_: Any) -> None:
        task_id = task.get("id")
        if task_id is None:
            _logger.debug("pre_execute received task without id; skipping")
            return

        carrier = task.get(OTEL_CARRIER_KEY) or {}
        tracectx = extract(carrier) if carrier else None
        token = context_api.attach(tracectx) if tracectx else None

        func_repr = describe_func(task.get("func") if task.get("func") is not None else func)
        span = self._tracer.start_span(
            f"run/{func_repr}",
            context=tracectx,
            kind=trace.SpanKind.CONSUMER,
        )
        if span.is_recording():
            span.set_attribute(MESSAGING_SYSTEM, _MESSAGING_SYSTEM_NAME)
            span.set_attribute(MESSAGING_OPERATION, _CONSUMER_OP)
            span.set_attribute(MESSAGING_DESTINATION_NAME, task.get("cluster") or _DEFAULT_DESTINATION)
            span.set_attribute(MESSAGING_MESSAGE_ID, task_id)
            if task.get("name"):
                span.set_attribute("django_q2.task.name", task["name"])
            span.set_attribute("django_q2.func", func_repr)
            if task.get("group"):
                span.set_attribute("django_q2.group", task["group"])

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()
        attach_task_context(task_id, span, activation, token)

    def _on_post_execute_in_worker(self, sender: Any, func: Any, task: dict, **_: Any) -> None:
        task_id = task.get("id")
        if task_id is None:
            return

        ctx = detach_task_context(task_id)
        if ctx is None:
            _logger.debug("post_execute_in_worker had no stored span for task %s", task_id)
            return

        span, activation, token = ctx
        try:
            if span.is_recording() and task.get("success") is False:
                result = task.get("result")
                description = _extract_error_description(result)
                span.set_status(Status(StatusCode.ERROR, description=description))
        finally:
            activation.__exit__(None, None, None)
            if token is not None:
                context_api.detach(token)

    def _on_post_spawn(self, sender: Any, proc_name: str, **_: Any) -> None:
        _logger.debug("django-q2 worker process spawned: %s", proc_name)


def _extract_error_description(result: Any) -> str | None:
    if result is None:
        return None
    text = result if isinstance(result, str) else str(result)
    head, sep, _tail = text.partition(" : ")
    return head if sep else text


__all__ = ["DjangoQ2Instrumentor", "utils"]
