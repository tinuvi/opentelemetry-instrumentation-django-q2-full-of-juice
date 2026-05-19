"""Task functions enqueued by the sample API. Mirror the in-repo test fixtures."""

from __future__ import annotations

import logging
import time

from django_q.tasks import async_task

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


TASK_REGISTRY = {
    "noop": "tasks_app.tasks.noop",
    "add": "tasks_app.tasks.add",
    "boom": "tasks_app.tasks.boom",
    "cascade_two": "tasks_app.tasks.cascade_two",
    "cascade_three": "tasks_app.tasks.cascade_three",
    "slow_noop": "tasks_app.tasks.slow_noop",
    "slow_cascade_two": "tasks_app.tasks.slow_cascade_two",
    "slow_cascade_three": "tasks_app.tasks.slow_cascade_three",
}
