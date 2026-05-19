"""Instrumentor entry point — wires django-q2 signals to OpenTelemetry spans."""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Collection
from timeit import default_timer
from typing import Any

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CLIENT_ID,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_CONVERSATION_ID,
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
_SCHEMA_URL = "https://opentelemetry.io/schemas/1.28.0"
_TASK_DURATION_METRIC = "django_q2.task.duration"

# Set by _wrap_async_task while it runs, read by _on_pre_enqueue. Lets the signal
# handler tell "wrap is live, enrich its span" from "wrap was bypassed (caller
# pre-imported async_task before instrument() ran), fall back to a tiny span".
_ACTIVE_PRODUCER_SPAN: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "_otel_django_q2_active_producer_span",
    default=None,
)


class DjangoQ2Instrumentor(BaseInstrumentor):
    """Connect django-q2's signals to OpenTelemetry spans (producer / consumer)."""

    def __init__(self):
        super().__init__()
        # Per-task start time keyed by task id. Populated in _on_pre_execute and
        # drained in _on_post_execute_in_worker so the histogram records real
        # consumer-side wall time (the user's function call, not the worker loop).
        self._task_start_times: dict[str, float] = {}
        # Set once per worker process from `post_spawn`. Stamped on every CONSUMER
        # span as `django_q2.worker` / `messaging.client.id`. Producers don't
        # know which worker will pick a task up, so they don't get this.
        self._worker_name: str | None = None

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
            schema_url=_SCHEMA_URL,
        )

        meter_provider = kwargs.get("meter_provider")
        meter = get_meter(__name__, __version__, meter_provider, schema_url=_SCHEMA_URL)
        # One histogram, time in seconds — same shape as Celery's runtime metric
        # so dashboards built for "task duration by queue" port directly. We pick
        # `django_q2.task.duration` over Celery's `flower.task.runtime.seconds`
        # because the latter name leaks the Flower UI's lineage, not the framework.
        self._task_duration_histogram = meter.create_histogram(
            name=_TASK_DURATION_METRIC,
            unit="s",
            description="Wall-clock time spent running a django-q2 task in the worker.",
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
        self._task_start_times.clear()
        self._worker_name = None
        _logger.debug("DjangoQ2Instrumentor uninstrumented")

    def _wrap_async_task(self, wrapped, instance, args, kwargs):
        # args[0] is `func` — see django_q.tasks.async_task signature.
        func = args[0] if args else kwargs.get("func")
        func_repr = describe_func(func)
        span = self._tracer.start_span(
            f"async_task/{func_repr}",
            kind=trace.SpanKind.PRODUCER,
        )
        # Open with the static bits — pre_enqueue (or the fallback path below)
        # later fills in task-dict-derived attributes via _apply_task_attributes.
        self._set_messaging_basics(span, _PRODUCER_OP, kwargs.get("cluster"), func_repr)
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
            # bits that only exist now (id, name, group, resolved cluster, ...).
            self._apply_task_attributes(active, task)
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
            self._set_messaging_basics(span, _PRODUCER_OP, task.get("cluster"), func_repr)
            self._apply_task_attributes(span, task)
            with trace.use_span(span, end_on_exit=False):
                self._inject_carrier(task)
        finally:
            span.end()

    def _set_messaging_basics(self, span, op_type: str, destination: str | None, func_repr: str) -> None:
        """Fields known at span-open time: system / op_type / destination / func."""
        if not span.is_recording():
            return
        span.set_attribute(MESSAGING_SYSTEM, _MESSAGING_SYSTEM_NAME)
        span.set_attribute(MESSAGING_OPERATION_TYPE, op_type)
        # Deprecated key kept for older collectors that still look at messaging.operation.
        span.set_attribute(MESSAGING_OPERATION, op_type)
        span.set_attribute(MESSAGING_DESTINATION_NAME, destination or _DEFAULT_DESTINATION)
        span.set_attribute("django_q2.func", func_repr)

    def _apply_task_attributes(self, span, task: dict) -> None:
        """
        Attributes derived from the django-q2 task dict.

        Shared between producer (pre_enqueue) and consumer (pre_execute) so the
        attribute pack stays in lockstep on both ends of a trace.
        """
        if not span.is_recording():
            return
        task_id = task.get("id")
        if task_id is not None:
            span.set_attribute(MESSAGING_MESSAGE_ID, task_id)
        if task.get("name"):
            span.set_attribute("django_q2.task.name", task["name"])
        if task.get("group"):
            span.set_attribute("django_q2.group", task["group"])
            # Standard semconv: gives generic messaging dashboards a single key to
            # group related messages on. Mirrors Celery's `correlation_id` usage.
            span.set_attribute(MESSAGING_MESSAGE_CONVERSATION_ID, task["group"])
        cluster = task.get("cluster")
        if cluster:
            span.set_attribute(MESSAGING_DESTINATION_NAME, cluster)
        if task.get("cached"):
            span.set_attribute("django_q2.cached", True)
        if task.get("sync"):
            span.set_attribute("django_q2.sync", True)
        if task.get("ack_failure"):
            span.set_attribute("django_q2.ack_failure", True)
        hook = task.get("hook")
        # django-q2 accepts either a dotted-path string or a callable. Only the
        # string form makes a useful attribute; repr-ing a function pointer leaks
        # a memory address that's useless for filtering or grouping.
        if isinstance(hook, str) and hook:
            span.set_attribute("django_q2.hook", hook)
        iter_count = task.get("iter_count")
        if isinstance(iter_count, int) and not isinstance(iter_count, bool) and iter_count > 0:
            span.set_attribute("django_q2.iter_count", iter_count)
        chain = task.get("chain")
        if isinstance(chain, list):
            span.set_attribute("django_q2.chain_length", len(chain))

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

        # Stamp the start time before we touch tracing — the histogram measures
        # wall-clock spent in the worker, including OTel context-attach overhead.
        self._task_start_times[task_id] = default_timer()

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
        self._set_messaging_basics(span, _CONSUMER_OP, task.get("cluster"), func_repr)
        self._apply_task_attributes(span, task)
        if self._worker_name and span.is_recording():
            # Captured once by `post_spawn` in this worker process. Both keys are
            # set so dashboards keyed on either the django-q2-specific name or
            # the semconv `messaging.client.id` can filter by worker.
            span.set_attribute("django_q2.worker", self._worker_name)
            span.set_attribute(MESSAGING_CLIENT_ID, self._worker_name)

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()
        attach_task_context(task_id, span, activation, token)

    def _on_post_execute_in_worker(self, sender: Any, func: Any, task: dict, **_: Any) -> None:
        task_id = task.get("id")
        if task_id is None:
            return

        ctx = detach_task_context(task_id)
        start_time = self._task_start_times.pop(task_id, None)
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
            self._record_task_duration(task, func, start_time)

    def _record_task_duration(self, task: dict, func: Any, start_time: float | None) -> None:
        if start_time is None:
            # post_execute fired without a matching pre_execute — nothing to record.
            return
        duration = max(default_timer() - start_time, 0.0)
        func_repr = describe_func(task.get("func") if task.get("func") is not None else func)
        # Keep cardinality bounded: destination + func + status only. Task name and
        # task id are deliberately excluded — they would explode cardinality on any
        # non-trivial workload.
        attributes = {
            MESSAGING_DESTINATION_NAME: task.get("cluster") or _DEFAULT_DESTINATION,
            "django_q2.func": func_repr,
            "status": "error" if task.get("success") is False else "success",
        }
        self._task_duration_histogram.record(duration, attributes=attributes)

    def _on_post_spawn(self, sender: Any, proc_name: str, **_: Any) -> None:
        # post_spawn fires inside each forked worker exactly once, before the
        # first task runs. Capture the name so every later CONSUMER span knows
        # which worker process produced it.
        self._worker_name = proc_name
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
