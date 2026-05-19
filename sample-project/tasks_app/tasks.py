"""Task functions enqueued by the sample API. Mirror the in-repo test fixtures."""

from __future__ import annotations

import logging

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


TASK_REGISTRY = {
    "noop": "tasks_app.tasks.noop",
    "add": "tasks_app.tasks.add",
    "boom": "tasks_app.tasks.boom",
    "cascade_two": "tasks_app.tasks.cascade_two",
    "cascade_three": "tasks_app.tasks.cascade_three",
}
