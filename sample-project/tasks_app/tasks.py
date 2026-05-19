"""Task functions enqueued by the sample API. Mirror the in-repo test fixtures."""

from __future__ import annotations

import logging
import time

from django_q.tasks import async_task
from opentelemetry import baggage, trace

_logger = logging.getLogger("tasks_app")


def noop(*args, **kwargs) -> dict:
    return {"args": list(args), "kwargs": kwargs}


def add(x: int, y: int) -> int:
    return x + y


def boom() -> None:
    raise RuntimeError("boom!")


def cascade_two(payload: str) -> str:
    """Enqueues a child task while running — exercises one level of cascade."""
    async_task("tasks_app.tasks.noop", payload)
    return payload


def cascade_three(payload: str) -> str:
    """Enqueues a middle task that itself enqueues a leaf — 3 levels deep."""
    async_task("tasks_app.tasks.cascade_two", payload)
    return payload


def fan_out(payload: str, count: int) -> dict:
    """
    Enqueue `count` sibling noops in a single shot.

    Models the dispatcher pattern (e.g. "process batch of N users"). Each sibling
    becomes its own PRODUCER → CONSUMER pair, all parented under this task's
    CONSUMER span — proves the per-task OTel context survives multiple inner
    async_task calls in a row.
    """
    for index in range(count):
        async_task("tasks_app.tasks.noop", payload, index)
    return {"payload": payload, "count": count}


def cascade_with_failure(payload: str) -> str:
    """
    Enqueue a child that succeeds *and* a child that raises, then return cleanly.

    The contract being pinned: a child's failure must not taint this task's
    consumer span (parent stays Status(UNSET) / state="success"), the failing
    child's consumer span carries the error, and the surviving sibling lands
    on the same trace with a clean status.
    """
    async_task("tasks_app.tasks.noop", payload)
    async_task("tasks_app.tasks.boom")
    return payload


def slow_noop(payload: str, seconds: float) -> str:
    # Sleep gives the CONSUMER span a measurable duration so E2E tests can assert
    # against it without relying on tiny clock differences.
    time.sleep(seconds)
    return payload


def slow_cascade_two(payload: str, seconds: float) -> str:
    """Sleeps, then enqueues a slow_noop that also sleeps — two layers of duration."""
    time.sleep(seconds)
    async_task("tasks_app.tasks.slow_noop", payload, seconds)
    return payload


def slow_cascade_three(payload: str, seconds: float) -> str:
    """Sleeps, then enqueues slow_cascade_two — three layers of duration."""
    time.sleep(seconds)
    async_task("tasks_app.tasks.slow_cascade_two", payload, seconds)
    return payload


def read_baggage() -> dict:
    """
    Surface OTel baggage entries on the current consumer span as `baggage.<key>` attributes.

    The instrumentor's `pre_execute` handler attaches the extracted carrier
    context before this function runs, so `baggage.get_all()` returns whatever
    the PRODUCER process set before calling `async_task(...)`. Stamping the
    entries on the span gives Playwright a Jaeger-visible signal it can assert
    against — that's the regression contract: a future carrier-handling refactor
    that silently dropped baggage propagation would make these attributes vanish.
    """
    span = trace.get_current_span()
    items = baggage.get_all()
    for key, value in items.items():
        # Cast to str defensively — baggage values are strings per the W3C spec,
        # but the API surfaces `Any` so a third-party setter could pass anything.
        span.set_attribute(f"baggage.{key}", str(value))
    return {"baggage_keys": sorted(items.keys()), "baggage_count": len(items)}


TASK_REGISTRY = {
    "noop": "tasks_app.tasks.noop",
    "add": "tasks_app.tasks.add",
    "boom": "tasks_app.tasks.boom",
    "cascade_two": "tasks_app.tasks.cascade_two",
    "cascade_three": "tasks_app.tasks.cascade_three",
    "fan_out": "tasks_app.tasks.fan_out",
    "cascade_with_failure": "tasks_app.tasks.cascade_with_failure",
    "slow_noop": "tasks_app.tasks.slow_noop",
    "slow_cascade_two": "tasks_app.tasks.slow_cascade_two",
    "slow_cascade_three": "tasks_app.tasks.slow_cascade_three",
    "read_baggage": "tasks_app.tasks.read_baggage",
}
