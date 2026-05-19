"""HTTP surface used by the Playwright E2E suite."""

from __future__ import annotations

import json
import logging

from django.http import HttpRequest, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from django_q.tasks import async_task
from opentelemetry import trace

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
