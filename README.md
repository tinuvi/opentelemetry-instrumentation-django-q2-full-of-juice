# OpenTelemetry instrumentation for django-q2

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice&metric=coverage)](https://sonarcloud.io/summary/new_code?id=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice)

Transparent OpenTelemetry instrumentation for [`django-q2`](https://github.com/django-q2/django-q2). Propagates trace context through the producer → broker → worker chain so cascading task graphs (HTTP request → task A → task B → task C) appear as one continuous distributed trace.

## Installation

```bash
pip install opentelemetry-instrumentation-django-q2-full-of-juice
```

Or, with Poetry:

```bash
poetry add opentelemetry-instrumentation-django-q2-full-of-juice
```

Requires Python ≥ 3.12, Django ≥ 5.2.11, and django-q2 ≥ 1.10.0.

## Quick start

```python
from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor

DjangoQ2Instrumentor().instrument()
```

Call this once before workers fork (e.g. in your project's `AppConfig.ready()`, or via the `opentelemetry-instrument` CLI bootstrap).

## How it works

The instrumentor connects to django-q2's signal lifecycle:

| Signal | Process | Role |
|---|---|---|
| `pre_enqueue(task)` | Producer | Start PRODUCER span, inject trace context into `task["otel_carrier"]`, end span. |
| `post_spawn(proc_name)` | Worker | Per-worker SDK init hook (background threads don't survive `fork`). |
| `pre_execute(func, task)` | Worker | Extract carrier, start CONSUMER span as child of the extracted context, attach as the current OTel context. |
| `post_execute_in_worker(func, task)` | Worker | Set span status from `task["success"]`, end CONSUMER span, detach context. |

Because the consumer span is the current OTel context **during** task execution, any nested `async_task(...)` call inside a task automatically parents under it — that's how the cascading chain composes.

The carrier travels inside the pickled, signed payload (not in broker headers), so it's confidentiality-bound to producers/workers that share `Q_CLUSTER`'s `SECRET_KEY`. Fine for django-q2↔django-q2 propagation; not suitable for non-django-q2 observers reading the broker directly.

## Status

Early scaffolding. The signal wiring described above is the design target — see `opentelemetry_instrumentation_django_q2/instrumentor.py` for the current state.
