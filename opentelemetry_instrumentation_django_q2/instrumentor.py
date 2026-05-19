"""Instrumentor entry point — wires django-q2 signals to OpenTelemetry spans."""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Collection
from typing import Any

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_ID,
    MESSAGING_OPERATION,
    MESSAGING_OPERATION_TYPE,
    MESSAGING_SYSTEM,
    MessagingOperationTypeValues,
)
from opentelemetry.semconv.attributes.exception_attributes import (
    EXCEPTION_MESSAGE,
    EXCEPTION_STACKTRACE,
    EXCEPTION_TYPE,
)
from opentelemetry.trace.status import Status, StatusCode
from wrapt import wrap_function_wrapper

from opentelemetry_instrumentation_django_q2 import utils
from opentelemetry_instrumentation_django_q2.package import _instruments
from opentelemetry_instrumentation_django_q2.utils import (
    OTEL_CARRIER_KEY,
    attach_task_context,
    clear_task_context,
    describe_func,
    detach_task_context,
    parse_worker_result,
)
from opentelemetry_instrumentation_django_q2.version import __version__

_logger = logging.getLogger("opentelemetry_instrumentation_django_q2")

_MESSAGING_SYSTEM_NAME = "django_q2"
_DEFAULT_DESTINATION = "default"
_PRODUCER_OP = MessagingOperationTypeValues.PUBLISH.value
_CONSUMER_OP = MessagingOperationTypeValues.PROCESS.value

# Set by _wrap_async_task while it runs, read by _on_pre_enqueue. Lets the signal
# handler tell "wrap is live, enrich its span" from "wrap was bypassed (caller
# pre-imported async_task before instrument() ran), fall back to a tiny span".
_ACTIVE_PRODUCER_SPAN: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "_otel_django_q2_active_producer_span",
    default=None,
)


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
        self._tracer = trace.get_tracer(
            __name__,
            __version__,
            tracer_provider,
            # Pin to the messaging spec version we target — collectors can use this
            # to translate / validate the attributes we emit.
            schema_url="https://opentelemetry.io/schemas/1.28.0",
        )

        # Wrap the public entry point so the PRODUCER span brackets the full call,
        # including `broker.enqueue(...)`. pre_enqueue alone would give us a span
        # with ~0 duration since django-q2 has no post_enqueue signal.
        wrap_function_wrapper("django_q.tasks", "async_task", self._wrap_async_task)

        pre_enqueue.connect(self._on_pre_enqueue, weak=False)
        pre_execute.connect(self._on_pre_execute, weak=False)
        post_execute_in_worker.connect(self._on_post_execute_in_worker, weak=False)
        post_spawn.connect(self._on_post_spawn, weak=False)
        _logger.debug("DjangoQ2Instrumentor instrumented")

    def _uninstrument(self, **kwargs) -> None:
        import django_q.tasks
        from django_q.signals import (
            post_execute_in_worker,
            post_spawn,
            pre_enqueue,
            pre_execute,
        )

        unwrap(django_q.tasks, "async_task")
        pre_enqueue.disconnect(self._on_pre_enqueue)
        pre_execute.disconnect(self._on_pre_execute)
        post_execute_in_worker.disconnect(self._on_post_execute_in_worker)
        post_spawn.disconnect(self._on_post_spawn)
        clear_task_context()
        _logger.debug("DjangoQ2Instrumentor uninstrumented")

    def _wrap_async_task(self, wrapped, instance, args, kwargs):
        # args[0] is `func` — see django_q.tasks.async_task signature.
        func = args[0] if args else kwargs.get("func")
        func_repr = describe_func(func)
        span = self._tracer.start_span(
            f"async_task/{func_repr}",
            kind=trace.SpanKind.PRODUCER,
        )
        self._set_producer_attrs(span, func_repr, kwargs.get("cluster"))
        # use_span makes this PRODUCER the current context so pre_enqueue's inject()
        # captures it as the carrier's traceparent; end_on_exit closes the span after
        # broker.enqueue returns (or _sync finishes in sync mode) — that's the win
        # over the old approach: real broker-publish duration on the span.
        token = _ACTIVE_PRODUCER_SPAN.set(span)
        try:
            with trace.use_span(span, end_on_exit=True):
                return wrapped(*args, **kwargs)
        finally:
            _ACTIVE_PRODUCER_SPAN.reset(token)

    def _on_pre_enqueue(self, sender: Any, task: dict, **_: Any) -> None:
        active = _ACTIVE_PRODUCER_SPAN.get()
        if active is not None:
            # Wrap is live — enrich the long-lived PRODUCER span with task-derived
            # bits that only exist now (id, name, group, resolved cluster).
            self._enrich_producer_span(active, task)
            self._inject_carrier(task)
            return

        # Fallback path: caller imported async_task before instrument() ran, so the
        # wrap was bypassed for this call. We still emit a PRODUCER span (with
        # near-zero duration) so cascading and trace shape stay correct.
        func_repr = describe_func(task.get("func"))
        span = self._tracer.start_span(
            f"async_task/{func_repr}",
            kind=trace.SpanKind.PRODUCER,
        )
        try:
            self._set_producer_attrs(span, func_repr, task.get("cluster"))
            self._enrich_producer_span(span, task)
            with trace.use_span(span, end_on_exit=False):
                self._inject_carrier(task)
        finally:
            span.end()

    def _set_producer_attrs(self, span, func_repr: str, cluster: str | None) -> None:
        if not span.is_recording():
            return
        span.set_attribute(MESSAGING_SYSTEM, _MESSAGING_SYSTEM_NAME)
        span.set_attribute(MESSAGING_OPERATION_TYPE, _PRODUCER_OP)
        # Deprecated key kept for older collectors that still look at messaging.operation.
        span.set_attribute(MESSAGING_OPERATION, _PRODUCER_OP)
        span.set_attribute(MESSAGING_DESTINATION_NAME, cluster or _DEFAULT_DESTINATION)
        span.set_attribute("django_q2.func", func_repr)

    def _enrich_producer_span(self, span, task: dict) -> None:
        if not span.is_recording():
            return
        task_id = task.get("id")
        if task_id is not None:
            span.set_attribute(MESSAGING_MESSAGE_ID, task_id)
        if task.get("name"):
            span.set_attribute("django_q2.task.name", task["name"])
        if task.get("group"):
            span.set_attribute("django_q2.group", task["group"])
        cluster = task.get("cluster")
        if cluster:
            span.set_attribute(MESSAGING_DESTINATION_NAME, cluster)

    def _inject_carrier(self, task: dict) -> None:
        try:
            carrier = task.setdefault(OTEL_CARRIER_KEY, {})
            inject(carrier)
        except Exception:
            _logger.exception("Failed to inject OTel carrier for task %s", task.get("id"))

    def _on_pre_execute(self, sender: Any, func: Any, task: dict, **_: Any) -> None:
        task_id = task.get("id")
        if task_id is None:
            _logger.debug("pre_execute received task without id; skipping")
            return

        carrier = task.get(OTEL_CARRIER_KEY) or {}
        tracectx = extract(carrier) if carrier else None
        # Attach the extracted context first so baggage from the producer is in
        # scope while the task runs. The CONSUMER span below becomes a child of
        # the same tracectx via the explicit `context=` argument.
        token = context_api.attach(tracectx) if tracectx else None

        func_repr = describe_func(task.get("func") if task.get("func") is not None else func)
        span = self._tracer.start_span(
            f"run/{func_repr}",
            context=tracectx,
            kind=trace.SpanKind.CONSUMER,
        )
        if span.is_recording():
            span.set_attribute(MESSAGING_SYSTEM, _MESSAGING_SYSTEM_NAME)
            span.set_attribute(MESSAGING_OPERATION_TYPE, _CONSUMER_OP)
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
                _record_failure(span, task.get("result"))
        finally:
            activation.__exit__(None, None, None)
            if token is not None:
                context_api.detach(token)

    def _on_post_spawn(self, sender: Any, proc_name: str, **_: Any) -> None:
        _logger.debug("django-q2 worker process spawned: %s", proc_name)


def _record_failure(span, result: Any) -> None:
    # django-q2 only hands us a string — `f"{e} : {traceback.format_exc()}"` from
    # worker.py — so the live exception object is gone. We parse the string into
    # type / message / stacktrace and emit a standard OTel `exception` event;
    # backends like Jaeger/Tempo render that event as the span's error details.
    message, exception_type, stacktrace = parse_worker_result(result)
    span.set_status(Status(StatusCode.ERROR, description=message))
    event_attrs: dict[str, str] = {}
    if exception_type:
        event_attrs[EXCEPTION_TYPE] = exception_type
    if message:
        event_attrs[EXCEPTION_MESSAGE] = message
    if stacktrace:
        event_attrs[EXCEPTION_STACKTRACE] = stacktrace
    if event_attrs:
        span.add_event("exception", attributes=event_attrs)


__all__ = ["DjangoQ2Instrumentor", "utils"]
