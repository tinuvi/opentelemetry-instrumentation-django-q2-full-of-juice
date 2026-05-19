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

## Span attributes

Every emitted span carries OpenTelemetry messaging semantic-convention attributes:

| Attribute | Value | Notes |
|---|---|---|
| `messaging.system` | `"django_q2"` | |
| `messaging.operation.type` | `"publish"` (producer) / `"process"` (consumer) | |
| `messaging.operation` | same as `operation.type` | kept for collectors on the deprecated key |
| `messaging.destination.name` | `task["cluster"]` or `"default"` | |
| `messaging.message.id` | `task["id"]` | |
| `messaging.message.conversation_id` | `task["group"]` | when set; mirrors Celery's `correlation_id` |
| `messaging.client.id` | django-q2 worker `proc_name` | consumer span only; populated after `post_spawn` |
| `django_q2.func` | dotted path or `repr` of the callable | |
| `django_q2.task.name` | `task["name"]` | |
| `django_q2.group` | `task["group"]` | when set |
| `django_q2.worker` | django-q2 worker `proc_name` | consumer span only; populated after `post_spawn` |
| `django_q2.cached` | `True` | only when `task["cached"]` is truthy |
| `django_q2.sync` | `True` | only when `task["sync"]` is truthy |
| `django_q2.ack_failure` | `True` | only when `task["ack_failure"]` is truthy |
| `django_q2.hook` | dotted-path string | only when `task["hook"]` is a string (callable hooks are skipped — see caveats) |
| `django_q2.iter_count` | positive int | only when `task["iter_count"] > 0` |
| `django_q2.chain_length` | int | when `task["chain"]` is a list — `len(chain)` |
| `django_q2.timeout` | positive int (seconds) | per-task budget the Sentinel will enforce. Producer side: only when caller passed `timeout=`. Consumer side: caller value if present, otherwise `Conf.TIMEOUT` from Q_CLUSTER. Absent when neither source has a positive value — `None`/`0` are never stamped. |
| `django_q2.broker.type` | `"orm"` / `"redis"` / `"mongo"` / `"sqs"` / `"iron_mq"` / dotted path | resolved once at `instrument()` from `Conf.BROKER_CLASS` → `IRON_MQ` → `SQS` → `ORM` → `MONGO` → `redis` default. Span-side only — see "Metrics" notes for why it's not a histogram label. |
| `django_q2.state` | `"success"` / `"error"` | consumer span only; absent in the sync-error branch where `task["success"]` is unset — mirror of Celery's `celery.state` |

Consumer spans inherit `Status(ERROR)` with the underlying error message when `task["success"]` is `False`, and gain a standard `exception` event whose `exception.type` / `exception.message` / `exception.stacktrace` attributes are parsed out of the `"{e} : {traceback}"` string django-q2 stashes in `task["result"]`. Backends like Jaeger, Tempo, and Grafana render that event as the span's error details.

## Metrics

| Metric | Type | Unit | Labels | Recorded by |
|---|---|---|---|---|
| `django_q2.task.duration` | histogram | `s` (seconds) | `messaging.destination.name`, `django_q2.func`, `status` (`"success"` / `"error"`) | Consumer — wall-clock time inside the worker (the user's function). |
| `django_q2.publish.duration` | histogram | `s` (seconds) | same as above | Producer — wall-clock time inside the `async_task` call (`broker.enqueue` + signing in async mode; full inline run in sync mode). |

Plumb a meter provider with `DjangoQ2Instrumentor().instrument(meter_provider=...)`, or rely on the global one set by `opentelemetry.metrics.set_meter_provider(...)`. Cardinality is bounded intentionally: task name and task id are deliberately **not** labels — they would explode any non-trivial workload. Operators can split a slow broker (`publish.duration` rising, `task.duration` flat) from slow workers (the inverse) without leaving the same dashboard.

`django_q2.broker.type` is also deliberately **not** a metric label. django-q2 has a single broker per cluster, so most fleets would carry a constant value on every histogram series — pure noise with no analytical payoff. Adding a label later is a backward-compatible change; removing one is breaking. The attribute is still emitted on every PRODUCER and CONSUMER span, so operators running multiple cluster types can split traces by backend via span queries.

## Caveats

- The PRODUCER span is opened by a `wrapt` wrapper around `django_q.tasks.async_task` so it brackets `broker.enqueue` and reports real publish latency. If user code does `from django_q.tasks import async_task` at module-import time **before** `DjangoQ2Instrumentor().instrument()` runs, that reference bypasses the wrapper; in that case the `pre_enqueue` handler falls back to emitting a near-zero-duration PRODUCER span so the trace shape stays correct. Calling `instrument()` from `AppConfig.ready()` (or bootstrapping with `opentelemetry-instrument`) avoids this — Django's URL/views imports happen after `ready()`.
- django-q2 forks workers; OpenTelemetry SDK background threads (e.g. `BatchSpanProcessor`) do not survive `os.fork`. Either bootstrap with the `opentelemetry-instrument` CLI (each worker initializes its own SDK on import) or configure your tracer provider from a `post_spawn` handler.
- `task["hook"]` is only stamped as `django_q2.hook` when it's a dotted-path string. django-q2 also accepts a callable hook, but `repr`-ing a function pointer leaks a memory address that's useless for grouping or filtering, so the callable case is intentionally skipped.
- The `django_q2.worker` / `messaging.client.id` attribute is captured from the first `post_spawn` signal in each worker process. django-q2 fires that signal at the top of the worker loop (both for forked workers and `sync=True`), so the attribute is present on every consumer span in normal use. It is absent only if `pre_execute` is fired manually (e.g. by tests) before any `post_spawn` ran.
- **`async_chain` continuity:** django-q2 progresses a chain by having its `monitor` process call `async_chain(task["chain"], ...)` after each link completes. The `monitor` process has no ambient OTel context, so only the *first* chain link sits under the trace that started it; subsequent links land in fresh traces. `django_q2.chain_length` and `django_q2.group` are still stamped on every span, so dashboards can pivot the rest of the pipeline by group. Adding full cross-link propagation would require django-q2 to expose a chain-progression hook upstream — tracked as a follow-up.

## Status

Working v0. See `CHANGELOG.md` for what landed.
