"""Instrumentor entry point — wires django-q2 signals to OpenTelemetry spans."""

from __future__ import annotations

import contextvars
import logging
from collections.abc import Collection
from timeit import default_timer
from typing import Any

from opentelemetry import context as context_api
from opentelemetry import trace
from opentelemetry.instrumentation.dependencies import (
    DependencyConflict,
    get_dependency_conflicts,
)
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.instrumentation.utils import unwrap
from opentelemetry.metrics import get_meter
from opentelemetry.propagate import extract, inject
from opentelemetry.semconv._incubating.attributes.messaging_attributes import (
    MESSAGING_CLIENT_ID,
    MESSAGING_DESTINATION_NAME,
    MESSAGING_MESSAGE_CONVERSATION_ID,
    MESSAGING_MESSAGE_ID,
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
from opentelemetry_instrumentation_django_q2.package import (
    _instruments,
    _instruments_any,
)
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
_SCHEMA_URL = "https://opentelemetry.io/schemas/1.34.0"
_TASK_DURATION_METRIC = "django_q2.task.duration"
_PUBLISH_DURATION_METRIC = "django_q2.publish.duration"
_STATE_SUCCESS = "success"
_STATE_ERROR = "error"
# Private task-dict key. django-q2's worker pops `task["timeout"]` before
# firing `pre_execute` (it consumes it as the Sentinel's per-task kill budget,
# see `django_q/worker.py`), so the consumer signal handler can't read the
# caller's per-task override directly. We stash a copy under this key at
# pre_enqueue time — it survives the pickle/unpickle round-trip through the
# broker and lets the consumer span report the same value the producer span did.
_TIMEOUT_STASH_KEY = "_otel_timeout"

# Set by _wrap_async_task while it runs, read by _on_pre_enqueue. Lets the signal
# handler tell "wrap is live, enrich its span" from "wrap was bypassed (caller
# pre-imported async_task before instrument() ran), fall back to a tiny span".
_ACTIVE_PRODUCER_SPAN: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "_otel_django_q2_active_producer_span",
    default=None,
)

# Mirrors the destination value `_on_pre_enqueue` writes onto the span so the
# wrap's finally block can label the publish histogram with the *resolved*
# cluster (django-q2 fills task["cluster"] in from Q_CLUSTER defaults when the
# caller didn't pass `cluster=`). Without this we'd record histogram samples
# under `default` while the span carries `sample-cluster` — same task, two
# stories. Reset alongside _ACTIVE_PRODUCER_SPAN to stay symmetric.
_RESOLVED_DESTINATION: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "_otel_django_q2_resolved_destination",
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
        # Resolved once from django-q2's broker-selection precedence (Conf.BROKER_CLASS
        # → IRON_MQ → SQS → ORM → MONGO → redis default) at `_instrument()` time.
        # Stamped on every PRODUCER and CONSUMER span as `django_q2.broker.type` so
        # operators can split observability by backend without out-of-band metadata
        # (e.g. distinguishing orm-broker latency from redis-broker latency in a
        # multi-cluster fleet, or proving an orm→redis migration is taking effect).
        # Kept off the metric labels deliberately: single-broker fleets — the
        # common case — would carry a constant column on every histogram series
        # forever, and removing a label later is a breaking change for downstream
        # dashboards. Adding the dimension to spans is non-breaking.
        self._broker_type: str | None = None
        # Per-chain attach tokens keyed by the just-finished task["id"]. Populated
        # by `_on_pre_chain_progress` (juice-fork only) and drained by
        # `_on_post_chain_progress`. Holds the consumer-context attach token so
        # the next link's PRODUCER span lands as a child of the previous link's
        # CONSUMER span — matching the in-task async_task cascade shape.
        self._chain_progress_tokens: dict[str, object] = {}
        # Flipped True in `_instrument()` iff the juice fork's chain-progress
        # signals are importable. Lets `_uninstrument()` skip a disconnect that
        # would raise on upstream django-q2 (where the signals don't exist).
        self._chain_signals_connected: bool = False

    def instrumentation_dependencies(self) -> Collection[str]:
        return _instruments

    def _check_dependency_conflicts(self) -> DependencyConflict | None:
        # `BaseInstrumentor`'s default `_check_dependency_conflicts` checks
        # only `instrumentation_dependencies()` (an "all-of" list). Our case
        # needs "any-of" semantics: upstream `django-q2` and the fork
        # `django-q2-full-of-juice` ship the same `django_q` import package
        # under different PyPI distribution names — only one is ever installed,
        # and either one is enough for the instrumentor to function. Without
        # this override, installing the fork instead of upstream would surface
        # as `DependencyConflict: requested "django-q2 >= 1.10.0" but found
        # "None"` and `instrument()` would silently return without wiring any
        # signals.
        return get_dependency_conflicts((), deps_any=_instruments_any)

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
        # Producer-side counterpart. Recorded from the async_task wrap so it sees
        # the same span the PRODUCER kind covers — broker.enqueue + signing roundtrip
        # in async mode, full inline run in sync mode. Celery records only the
        # consumer-side; we expose both so operators can spot a slow broker
        # (publish.duration high, task.duration normal) vs slow workers.
        self._task_publish_duration_histogram = meter.create_histogram(
            name=_PUBLISH_DURATION_METRIC,
            unit="s",
            description="Wall-clock time spent publishing a django-q2 task from the producer.",
        )

        # Wrap the public entry point so the PRODUCER span brackets the full call,
        # including `broker.enqueue(...)`. pre_enqueue alone would give us a span
        # with ~0 duration since django-q2 has no post_enqueue signal.
        wrap_function_wrapper("django_q.tasks", "async_task", self._wrap_async_task)

        # Resolve the broker backend once. django-q2 has a single broker per
        # cluster, so this never changes after Q_CLUSTER is loaded.
        self._broker_type = _resolve_broker_type()

        pre_enqueue.connect(self._on_pre_enqueue, weak=False)
        pre_execute.connect(self._on_pre_execute, weak=False)
        post_execute_in_worker.connect(self._on_post_execute_in_worker, weak=False)
        post_spawn.connect(self._on_post_spawn, weak=False)

        # `tinuvi/django-q2-full-of-juice` adds two signals on top of upstream
        # so chain progression carries a trace context across links. Upstream
        # django-q2 doesn't ship these — `ImportError` is the expected outcome
        # there and degrades cleanly to the existing "chain links 2..N start
        # fresh traces" behavior. Static type checkers only see upstream stubs,
        # hence the suppression — the runtime guard above is the real check.
        try:
            # pyrefly: ignore  # missing-module-attribute
            from django_q.signals import post_chain_progress, pre_chain_progress
        except ImportError:
            _logger.debug(
                "django-q2 build does not expose chain-progress signals; "
                "chain continuity past the first link will not be propagated"
            )
        else:
            pre_chain_progress.connect(self._on_pre_chain_progress, weak=False)
            post_chain_progress.connect(self._on_post_chain_progress, weak=False)
            self._chain_signals_connected = True
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
        if self._chain_signals_connected:
            # Defensive: the juice fork's signals were importable at
            # `_instrument()` time; in principle the module could be re-imported
            # against upstream between then and now, so swallow ImportError.
            try:
                # pyrefly: ignore  # missing-module-attribute
                from django_q.signals import post_chain_progress, pre_chain_progress

                pre_chain_progress.disconnect(self._on_pre_chain_progress)
                post_chain_progress.disconnect(self._on_post_chain_progress)
            except ImportError:
                pass
            self._chain_signals_connected = False
        # Detach any tokens stranded by an interrupted chain — leaving them
        # attached would leak the consumer context into unrelated work.
        for token in self._chain_progress_tokens.values():
            try:
                context_api.detach(token)
            except Exception:
                _logger.debug("Failed to detach a stranded chain-progress token", exc_info=True)
        self._chain_progress_tokens.clear()
        clear_task_context()
        self._task_start_times.clear()
        self._worker_name = None
        self._broker_type = None
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
        span_token = _ACTIVE_PRODUCER_SPAN.set(span)
        # Seed the resolved destination with the caller's kwarg — pre_enqueue may
        # overwrite this with the task-dict-resolved cluster once it fires.
        dest_token = _RESOLVED_DESTINATION.set(kwargs.get("cluster") or _DEFAULT_DESTINATION)
        publish_status = _STATE_SUCCESS
        start_time = default_timer()
        try:
            with trace.use_span(span, end_on_exit=True):
                try:
                    return wrapped(*args, **kwargs)
                except BaseException:
                    # Broker publish failed (signing error, DB unreachable, validation,
                    # ...). Tag the histogram before re-raising so dashboards can split
                    # publish failures from publish successes.
                    publish_status = _STATE_ERROR
                    raise
        finally:
            _ACTIVE_PRODUCER_SPAN.reset(span_token)
            # Read before resetting so we observe the value pre_enqueue stored, not
            # the default we started with.
            destination = _RESOLVED_DESTINATION.get() or _DEFAULT_DESTINATION
            _RESOLVED_DESTINATION.reset(dest_token)
            duration = max(default_timer() - start_time, 0.0)
            self._task_publish_duration_histogram.record(
                duration,
                attributes={
                    MESSAGING_DESTINATION_NAME: destination,
                    "django_q2.func": func_repr,
                    "status": publish_status,
                },
            )

    def _on_pre_enqueue(self, sender: Any, task: dict, **_: Any) -> None:
        active = _ACTIVE_PRODUCER_SPAN.get()
        if active is not None:
            # Wrap is live — enrich the long-lived PRODUCER span with task-derived
            # bits that only exist now (id, name, group, resolved cluster, ...).
            self._apply_task_attributes(active, task)
            # Mirror the cluster the consumer side will see (django-q2's pusher.py
            # stamps `task["cluster"] = Conf.CLUSTER_NAME` server-side after the
            # broker pop). If the caller didn't pass `cluster=` and Q_CLUSTER is
            # configured, look it up so the producer span + publish histogram and
            # the consumer span + task histogram all carry the same destination.
            resolved_cluster = task.get("cluster") or _read_configured_cluster_name()
            if resolved_cluster:
                active.set_attribute(MESSAGING_DESTINATION_NAME, resolved_cluster)
                _RESOLVED_DESTINATION.set(resolved_cluster)
            _stash_timeout(task)
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
            _stash_timeout(task)
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
        span.set_attribute(MESSAGING_DESTINATION_NAME, destination or _DEFAULT_DESTINATION)
        span.set_attribute("django_q2.func", func_repr)
        if self._broker_type:
            span.set_attribute("django_q2.broker.type", self._broker_type)

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
        # `task["attempt"]` is stamped by the juice fork's pusher on every
        # dequeue: 1 on first delivery, 2..N on re-deliveries (the broker
        # re-pops when the worker didn't ack before `Conf.RETRY` elapsed).
        # Upstream `django-q2 1.10.x` doesn't populate this field — absent
        # then stamps no attribute, which is the cleanest "no instrumentation
        # signal for retries" indicator we can give dashboards.
        #
        # Stamped on first delivery too: gating on `>1` would make
        # "field absent" ambiguous (no retries vs no instrumentation). We pay
        # one extra constant column on attempt-1 spans for the disambiguation.
        #
        # Deliberately not added to histogram labels — most tasks succeed on
        # attempt 1, so the column would be a constant on every series.
        # Removing a metric label later is a breaking change for downstream
        # dashboards; adding it is not. Same argument as `django_q2.broker.type`.
        #
        # No semconv-aligned mirror today: opentelemetry-semantic-conventions
        # ships only vendor-specific keys (GCP Pub/Sub, Azure Service Bus) at
        # the 1.34.0 pin. A general `messaging.message.delivery.attempt` would
        # be the natural mirror — revisit when the semconv-python package
        # exposes one (likely the messaging.message.* incubating namespace).
        attempt = task.get("attempt")
        if _is_positive_int(attempt):
            span.set_attribute("django_q2.attempt", attempt)
        # django-q2's `timeout` accepts an integer number of seconds. We stamp
        # only positive ints so the attribute always represents a real budget
        # and not a `null`/`0` sentinel — dashboards can express "duration / timeout"
        # ratios and "timeout-vs-error" filters without a NULL-handling clause.
        #
        # Prefer the live `task["timeout"]` if present; fall back to the private
        # `_otel_timeout` stash. The consumer side will only ever see the stash
        # because django-q2's worker pops `task["timeout"]` before pre_execute
        # fires (see _TIMEOUT_STASH_KEY's docstring). On the producer side, the
        # stash hasn't been set yet so we'll be reading the live key.
        timeout_value = task.get("timeout")
        if not _is_positive_int(timeout_value):
            timeout_value = task.get(_TIMEOUT_STASH_KEY)
        if _is_positive_int(timeout_value):
            span.set_attribute("django_q2.timeout", timeout_value)

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
        # Resolve the destination: prefer the cluster pusher.py stamped on the task,
        # otherwise fall back to Q_CLUSTER's name (matches the producer side and
        # keeps sync-mode runs — where the broker pusher never fires — consistent
        # with async-mode runs).
        consumer_destination = task.get("cluster") or _read_configured_cluster_name()
        self._set_messaging_basics(span, _CONSUMER_OP, consumer_destination, func_repr)
        self._apply_task_attributes(span, task)
        # Conf.TIMEOUT fallback. The producer can't see worker config, so its
        # span only ever carries a timeout when the caller passed `timeout=`.
        # On the consumer we additionally fall back to Conf.TIMEOUT — that's
        # the budget the Sentinel actually enforces when neither the caller's
        # override (via the `_otel_timeout` stash) nor a live `task["timeout"]`
        # is present. Stamp only if positive: the default Conf.TIMEOUT is `None`
        # (no enforcement), and `None`/`0` on the span would mislead
        # "duration ≥ timeout" alerting queries.
        live_timeout = task.get("timeout") if _is_positive_int(task.get("timeout")) else None
        stashed_timeout = task.get(_TIMEOUT_STASH_KEY) if _is_positive_int(task.get(_TIMEOUT_STASH_KEY)) else None
        if live_timeout is None and stashed_timeout is None and span.is_recording():
            fallback_timeout = _read_configured_timeout()
            if fallback_timeout is not None:
                span.set_attribute("django_q2.timeout", fallback_timeout)
        if self._worker_name and span.is_recording():
            # Captured once by `post_spawn` in this worker process. Both keys are
            # set so dashboards keyed on either the django-q2-specific name or
            # the semconv `messaging.client.id` can filter by worker.
            span.set_attribute("django_q2.worker", self._worker_name)
            span.set_attribute(MESSAGING_CLIENT_ID, self._worker_name)

        activation = trace.use_span(span, end_on_exit=True)
        activation.__enter__()
        attach_task_context(task_id, span, activation, token)

    def _on_post_execute_in_worker(
        self,
        sender: Any,
        func: Any,
        task: dict,
        exc_info: Any = None,
        **_: Any,
    ) -> None:
        # `exc_info` is the juice fork's `sys.exc_info()` triple, forwarded from
        # `django_q/worker.py`'s except block (the fork sends `None` on success).
        # Upstream `django-q2 1.10.x` doesn't send the kwarg at all — the default
        # keeps the handler signature-compatible there. We prefer the live
        # exception when it's present (richer event shape: cause chains, notes,
        # subclass-specific repr) and fall back to parsing `task["result"]`
        # otherwise — that fallback is the only source of data on upstream and
        # in the rare juice case where the kwarg is malformed.
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
            success = task.get("success")
            if span.is_recording():
                # Mirror of Celery's `celery.state` — lets dashboards filter on the
                # terminal state without parsing the OTel status code. Deliberately
                # left absent in the sync-error branch (success is None), matching
                # the existing "no error event, no error status" behaviour there.
                if success is True:
                    span.set_attribute("django_q2.state", _STATE_SUCCESS)
                elif success is False:
                    span.set_attribute("django_q2.state", _STATE_ERROR)
                    _record_failure(span, task.get("result"), exc_info)
            # Overwrite the carrier with a traceparent that points at the CONSUMER
            # span we're about to close. Done unconditionally — on upstream
            # django-q2 nothing reads the carrier post-task and the extra dict
            # write is cheap; on the juice fork it's load-bearing for chain
            # continuity (the monitor's chain-progress signal will re-extract
            # this so the next link's PRODUCER span parents under this CONSUMER).
            self._inject_carrier(task)
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
        # non-trivial workload. Destination falls through the same resolution as
        # the consumer span so both telemetry channels agree.
        destination = task.get("cluster") or _read_configured_cluster_name() or _DEFAULT_DESTINATION
        attributes = {
            MESSAGING_DESTINATION_NAME: destination,
            "django_q2.func": func_repr,
            "status": _STATE_ERROR if task.get("success") is False else _STATE_SUCCESS,
        }
        self._task_duration_histogram.record(duration, attributes=attributes)

    def _on_pre_chain_progress(self, sender: Any, task: dict, **_: Any) -> None:
        # Juice-fork-only path. django-q2's monitor process fires this around
        # `async_chain(...)` after a link completes. The just-finished task's
        # carrier was re-injected by `_on_post_execute_in_worker` with the
        # CONSUMER span as current — so attaching it here parents the next
        # link's PRODUCER span under that CONSUMER span (matching the in-task
        # async_task cascade shape).
        carrier = task.get(OTEL_CARRIER_KEY) or {}
        if not carrier:
            return
        tracectx = extract(carrier)
        if tracectx is None:
            return
        token = context_api.attach(tracectx)
        task_id = task.get("id")
        if task_id is None:
            # Without an id we can't pair this attach with the matching
            # `post_chain_progress` — detach immediately so the context doesn't
            # leak into subsequent monitor work.
            context_api.detach(token)
            return
        self._chain_progress_tokens[task_id] = token

    def _on_post_chain_progress(self, sender: Any, task: dict, **_: Any) -> None:
        task_id = task.get("id")
        if task_id is None:
            return
        token = self._chain_progress_tokens.pop(task_id, None)
        if token is not None:
            context_api.detach(token)

    def _on_post_spawn(self, sender: Any, proc_name: str, **_: Any) -> None:
        # post_spawn fires inside each forked worker exactly once, before the
        # first task runs. Capture the name so every later CONSUMER span knows
        # which worker process produced it.
        self._worker_name = proc_name
        _logger.debug("django-q2 worker process spawned: %s", proc_name)


def _is_positive_int(value: Any) -> bool:
    # bool is a subclass of int in Python — defensively exclude it so a stray
    # `timeout=True` doesn't land as a 1-second budget.
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _stash_timeout(task: dict) -> None:
    """
    Copy a positive `task["timeout"]` into the private `_otel_timeout` slot.

    django-q2's worker pops `task["timeout"]` before firing `pre_execute` (it
    uses the value as the Sentinel's per-task kill budget), so without this
    copy the consumer signal handler would never see the caller's override.
    The stash key survives the pickle/unpickle round-trip through the broker.
    """
    timeout_value = task.get("timeout")
    if _is_positive_int(timeout_value):
        task[_TIMEOUT_STASH_KEY] = timeout_value


def _read_configured_cluster_name() -> str | None:
    """Return Q_CLUSTER's configured name if django-q2 is loadable, else None."""
    try:
        # Imported lazily — Django settings may not be fully bootstrapped yet when
        # the BaseInstrumentor runs, and tests sometimes drive signals directly
        # without a populated Q_CLUSTER (we don't want a fragile import to surface
        # as an instrumentation failure on those paths).
        from django_q.conf import Conf
    except Exception:
        return None
    name = getattr(Conf, "CLUSTER_NAME", None)
    return name if name else None


def _read_configured_timeout() -> int | None:
    """Return Q_CLUSTER's configured timeout (positive int seconds) if available."""
    try:
        from django_q.conf import Conf
    except Exception:
        return None
    value = getattr(Conf, "TIMEOUT", None)
    return value if _is_positive_int(value) else None


def _resolve_broker_type() -> str | None:
    """
    Mirror django_q.brokers.get_broker's precedence to produce a stable short label.

    Order (per django-q2 master, django_q/brokers/__init__.py): BROKER_CLASS →
    IRON_MQ → SQS → ORM → MONGO → redis (the default fallback). Returns the
    dotted path verbatim when a custom BROKER_CLASS is set so users can identify
    their backend exactly.
    """
    try:
        from django_q.conf import Conf
    except Exception:
        return None
    if getattr(Conf, "BROKER_CLASS", None):
        return Conf.BROKER_CLASS
    if getattr(Conf, "IRON_MQ", None):
        return "iron_mq"
    if isinstance(getattr(Conf, "SQS", None), dict):
        return "sqs"
    if getattr(Conf, "ORM", None):
        return "orm"
    if getattr(Conf, "MONGO", None):
        return "mongo"
    return "redis"


def _record_failure(span, result: Any, exc_info: Any = None) -> None:
    # Two paths, picked by which information the producer of the signal had:
    #
    # 1. Juice-fork path — `django_q/worker.py` captures `sys.exc_info()` inside
    #    the except block and forwards it as a signal kwarg. The live exception
    #    is reachable, so we call `span.record_exception(exc)` per cause link:
    #    one `exception` event per link in the `__cause__` / `__context__`
    #    chain, each surfacing Python 3.11+ `add_note()` notes and any
    #    subclass-specific repr the SDK can resolve — none of which we can
    #    recover from a string. `record_exception` does NOT set status on its
    #    own, so we still set `ERROR` ourselves. Description = `str(exc)` on
    #    the outermost exception so chained exceptions surface the top-level
    #    message (not the innermost cause's).
    #
    # 2. Upstream / fallback path — `django-q2 1.10.x` doesn't pass exc_info
    #    and only hands us a string (`f"{e} : {traceback.format_exc()}"` from
    #    worker.py). We regex-parse that into type / message / stacktrace and
    #    emit one OTel `exception` event manually. Also the path the juice
    #    code lands on if `exc_info` is malformed (defensive isinstance check
    #    in `_live_exception_from`).
    exc = _live_exception_from(exc_info)
    if exc is not None:
        _record_exception_chain(span, exc)
        span.set_status(Status(StatusCode.ERROR, description=str(exc) or None))
        return
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


def _record_exception_chain(span, exc: BaseException) -> None:
    """
    Emit one OTel `exception` event per link in `exc`'s cause/context chain.

    OpenTelemetry Python's `Span.record_exception()` only emits a single event;
    the chain context shows up as text inside `exception.stacktrace`, not as
    separate structured events. For dashboards that filter on
    `exception.type=X` to surface chained causes, that single event loses the
    inner exception's type — Jaeger / Tempo can't show "RuntimeError caused
    by ValueError" without parsing the stacktrace string.

    We walk the chain ourselves (matching Python's own `__cause__` /
    `__context__` precedence — `raise X from Y` sets `__cause__` and suppresses
    `__context__`, otherwise we follow `__context__`) and emit one event per
    link so each cause stays addressable. The `seen` set guards the (rare)
    case of a self-referential chain.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        span.record_exception(current)
        next_link = current.__cause__
        if next_link is None and not getattr(current, "__suppress_context__", False):
            next_link = current.__context__
        current = next_link


def _live_exception_from(exc_info: Any) -> BaseException | None:
    """
    Return the live exception out of a sys.exc_info()-style triple, or None.

    Guards against the upstream case (no kwarg ⇒ None) and the defensive case
    where a third party forwards a malformed triple (e.g. a stringified type in
    the value slot). Either of those falls back to the string-parsing path.
    """
    if not exc_info:
        return None
    try:
        exc = exc_info[1]
    except TypeError, IndexError:
        return None
    return exc if isinstance(exc, BaseException) else None


__all__ = ["DjangoQ2Instrumentor", "utils"]
