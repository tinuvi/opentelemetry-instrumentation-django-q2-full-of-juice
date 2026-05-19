"""Helpers for OTel carrier handling and per-task context storage."""

from __future__ import annotations

import logging
import re
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


# Format produced by django_q/worker.py: f"{e} : {traceback.format_exc()}".
# Splitting on the first " : " separates the message from the formatted traceback,
# and the traceback's last non-empty line is `ExceptionClassName: message`.
_WORKER_RESULT_SEP = " : "
_EXCEPTION_TYPE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_.]*)\s*:")


def parse_worker_result(result: Any) -> tuple[str | None, str | None, str | None]:
    """
    Return (message, exception_type, stacktrace) from a worker failure result.

    Defensive: any of the fields can be None when the input doesn't parse cleanly
    (e.g. when post_execute_in_worker fires in the sync-error branch before the
    formatted result is stored).
    """
    if result is None:
        return None, None, None
    text = result if isinstance(result, str) else str(result)
    head, sep, tail = text.partition(_WORKER_RESULT_SEP)
    if not sep:
        return text or None, None, None
    message = head or None
    stacktrace = tail or None
    exception_type = _extract_exception_type(tail)
    return message, exception_type, stacktrace


def _extract_exception_type(stacktrace: str) -> str | None:
    for line in reversed(stacktrace.splitlines()):
        stripped = line.strip()
        if not stripped:
            continue
        match = _EXCEPTION_TYPE_RE.match(stripped)
        if match:
            return match.group(1)
    return None
