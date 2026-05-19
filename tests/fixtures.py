"""Task fixtures used by integration tests. Must be importable by django-q2 worker."""

from __future__ import annotations

from django_q.tasks import async_task


def noop(*args, **kwargs):
    return args, kwargs


def add(x: int, y: int) -> int:
    return x + y


def boom():
    raise RuntimeError("boom!")


def cascade_two(payload: str) -> str:
    """Enqueues a deeper task while running, exercising cascading propagation."""
    async_task("tests.fixtures.noop", payload, sync=True)
    return payload


def cascade_three(payload: str) -> str:
    """Enqueues a middle task which itself enqueues a leaf — 3 levels deep."""
    async_task("tests.fixtures.cascade_two", payload, sync=True)
    return payload
