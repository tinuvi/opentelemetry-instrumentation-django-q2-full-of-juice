"""Helpers for OTel carrier handling and per-task context storage."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.context import Context
    from opentelemetry.trace import Span

_logger = logging.getLogger("opentelemetry_instrumentation_django_q2")

OTEL_CARRIER_KEY = "otel_carrier"

# Per-task context storage. Keyed by task["id"]. Each entry holds the span,
# the use_span activation context manager, and the detach token.
_TASK_CONTEXT: dict[str, tuple["Span", Any, "Context | None"]] = {}


def attach_task_context(task_id: str, span: "Span", activation: Any, token: "Context | None") -> None:
    _TASK_CONTEXT[task_id] = (span, activation, token)


def retrieve_task_context(task_id: str) -> tuple["Span", Any, "Context | None"] | None:
    return _TASK_CONTEXT.get(task_id)


def detach_task_context(task_id: str) -> tuple["Span", Any, "Context | None"] | None:
    return _TASK_CONTEXT.pop(task_id, None)
