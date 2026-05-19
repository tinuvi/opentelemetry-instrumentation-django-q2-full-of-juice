"""HTTP surface used by the Playwright E2E suite."""

from __future__ import annotations

import json
import logging

from django.http import HttpRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django_q.tasks import async_chain, async_iter, async_task
from opentelemetry import baggage, trace
from opentelemetry import context as context_api

from tasks_app.tasks import TASK_REGISTRY

_logger = logging.getLogger("tasks_app")
_tracer = trace.get_tracer("tasks_app")


def health(_request: HttpRequest) -> JsonResponse:
    return JsonResponse({"status": "ok"})


@csrf_exempt
@require_http_methods(["POST"])
def enqueue(request: HttpRequest) -> JsonResponse:
    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)

    name = body.get("task")
    if name not in TASK_REGISTRY:
        return JsonResponse({"error": "unknown_task", "available": sorted(TASK_REGISTRY)}, status=400)

    args = body.get("args", [])
    kwargs = body.get("kwargs", {})
    # Playwright passes a unique trigger_span per scenario so it can query Jaeger
    # for exactly the trace it just created (Jaeger search: operation = trigger_span).
    span_name = body.get("trigger_span", "HTTP POST /api/enqueue/")

    with _tracer.start_as_current_span(span_name) as span:
        task_id = async_task(TASK_REGISTRY[name], *args, **kwargs)
        trace_id_hex = format(span.get_span_context().trace_id, "032x")

    return JsonResponse({"task_id": task_id, "task": name, "trace_id": trace_id_hex})


@csrf_exempt
@require_http_methods(["POST"])
def enqueue_chain(request: HttpRequest) -> JsonResponse:
    """Enqueue a sequential chain via django-q2's async_chain — exercises django_q2.chain_length."""
    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)

    raw_chain = body.get("chain", [])
    if not isinstance(raw_chain, list) or not raw_chain:
        return JsonResponse({"error": "chain_must_be_non_empty_list"}, status=400)

    chain: list[tuple] = []
    for entry in raw_chain:
        name = entry.get("task")
        if name not in TASK_REGISTRY:
            return JsonResponse({"error": "unknown_task", "name": name, "available": sorted(TASK_REGISTRY)}, status=400)
        # django-q2's async_chain expects (func, args, kwargs) tuples.
        chain.append((TASK_REGISTRY[name], tuple(entry.get("args", [])), entry.get("kwargs", {})))

    span_name = body.get("trigger_span", "HTTP POST /api/enqueue-chain/")
    # Capture length BEFORE calling async_chain — django-q2's implementation
    # pops entries off the list as it enqueues them, so a post-call `len(chain)`
    # is misleading.
    chain_length = len(chain)
    with _tracer.start_as_current_span(span_name) as span:
        group_id = async_chain(chain, sync=False)
        trace_id_hex = format(span.get_span_context().trace_id, "032x")

    return JsonResponse({"group_id": group_id, "trace_id": trace_id_hex, "chain_length": chain_length})


@csrf_exempt
@require_http_methods(["POST"])
def enqueue_iter(request: HttpRequest) -> JsonResponse:
    """Enqueue an iterable batch via async_iter — exercises django_q2.iter_count."""
    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)

    name = body.get("task")
    if name not in TASK_REGISTRY:
        return JsonResponse({"error": "unknown_task", "available": sorted(TASK_REGISTRY)}, status=400)

    args_iter = body.get("args_iter", [])
    if not isinstance(args_iter, list) or not args_iter:
        return JsonResponse({"error": "args_iter_must_be_non_empty_list"}, status=400)

    # async_iter expects an iterable of arg-tuples for the same target function.
    arg_tuples = [tuple(entry) for entry in args_iter]

    span_name = body.get("trigger_span", "HTTP POST /api/enqueue-iter/")
    with _tracer.start_as_current_span(span_name) as span:
        # `sync` defaults to False in async_iter; pass it explicitly so the test
        # exercises the same code path as a real worker.
        task_id = async_iter(TASK_REGISTRY[name], arg_tuples, sync=False)
        trace_id_hex = format(span.get_span_context().trace_id, "032x")

    return JsonResponse({"task_id": task_id, "trace_id": trace_id_hex, "iter_count": len(arg_tuples)})


@csrf_exempt
@require_http_methods(["POST"])
def enqueue_detached(request: HttpRequest) -> JsonResponse:
    """
    Enqueue a task with no ambient OTel context so the PRODUCER span becomes the trace root.

    Models the production cases where `async_task(...)` is called outside any
    ambient span — django-q2 schedulers, management commands, cron-triggered
    backfills, or one worker enqueuing follow-up work after its own task has
    ended. The endpoint detaches the request's context, calls async_task, then
    restores it: the producer wrap opens its span with no parent, making it a
    valid trace root.

    Returns `task_id` only. The trace_id is unknown to this endpoint by design
    (we deliberately don't peek inside the wrap's contextvar) — Playwright
    looks the trace up by `messaging.message.id = task_id` via Jaeger's tag
    search, the same way an operator would correlate from an enqueued task.
    """
    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)

    name = body.get("task")
    if name not in TASK_REGISTRY:
        return JsonResponse({"error": "unknown_task", "available": sorted(TASK_REGISTRY)}, status=400)

    args = body.get("args", [])
    kwargs = body.get("kwargs", {})

    # Detach to a fresh empty context so the producer wrap sees no parent span.
    # `attach(Context())` does NOT replay any active span — that's the whole
    # point: we want the producer to be a trace root, exactly as it would be
    # under a scheduler thread or `manage.py` invocation.
    token = context_api.attach(context_api.Context())
    try:
        task_id = async_task(TASK_REGISTRY[name], *args, **kwargs)
    finally:
        context_api.detach(token)

    return JsonResponse({"task_id": task_id, "task": name})


@csrf_exempt
@require_http_methods(["POST"])
def enqueue_with_baggage(request: HttpRequest) -> JsonResponse:
    """
    Set OTel baggage in the request context, then enqueue a task that surfaces it on its consumer span.

    Pins the contract that baggage set at the HTTP edge survives the
    producer → carrier → worker round-trip. Future carrier-handling refactors
    that silently swap the propagator to TraceContext-only would break this.
    """
    try:
        body = json.loads(request.body or b"{}")
    except json.JSONDecodeError:
        return JsonResponse({"error": "invalid_json"}, status=400)

    baggage_items = body.get("baggage", {})
    if not isinstance(baggage_items, dict) or not baggage_items:
        return JsonResponse({"error": "baggage_must_be_non_empty_dict"}, status=400)

    # Layer each baggage value into the current context. `set_baggage` returns a
    # NEW context (it's immutable); attach the last one so subsequent code sees
    # all entries. Detach in `finally` to keep the request-handler context clean.
    new_context = context_api.get_current()
    for key, value in baggage_items.items():
        new_context = baggage.set_baggage(key, str(value), context=new_context)
    token = context_api.attach(new_context)
    try:
        span_name = body.get("trigger_span", "HTTP POST /api/enqueue-with-baggage/")
        with _tracer.start_as_current_span(span_name) as span:
            task_id = async_task("tasks_app.tasks.read_baggage")
            trace_id_hex = format(span.get_span_context().trace_id, "032x")
    finally:
        context_api.detach(token)

    return JsonResponse(
        {
            "task_id": task_id,
            "trace_id": trace_id_hex,
            "baggage_keys": sorted(baggage_items.keys()),
        }
    )
