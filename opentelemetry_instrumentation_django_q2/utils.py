"""Helpers for OTel carrier handling and per-task context storage."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from contextvars import Token

    from opentelemetry.context import Context
    from opentelemetry.trace import Span

_logger = logging.getLogger("opentelemetry_instrumentation_django_q2")

OTEL_CARRIER_KEY = "otel_carrier"

# Per-task context storage. Keyed by task["id"]. Each entry holds the span,
# the use_span activation context manager, and the detach token.
_TASK_CONTEXT: dict[str, tuple[Span, Any, Token[Context] | None]] = {}


def attach_task_context(task_id: str, span: Span, activation: Any, token: Token[Context] | None) -> None:
    _TASK_CONTEXT[task_id] = (span, activation, token)


def retrieve_task_context(task_id: str) -> tuple[Span, Any, Token[Context] | None] | None:
    return _TASK_CONTEXT.get(task_id)


def detach_task_context(task_id: str) -> tuple[Span, Any, Token[Context] | None] | None:
    return _TASK_CONTEXT.pop(task_id, None)


def clear_task_context() -> None:
    _TASK_CONTEXT.clear()


def describe_func(func: Any) -> str:
    if isinstance(func, str):
        return func
    module = getattr(func, "__module__", None)
    qualname = getattr(func, "__qualname__", None) or getattr(func, "__name__", None)
    if module and qualname:
        return f"{module}.{qualname}"
    if qualname:
        return qualname
    return repr(func)
