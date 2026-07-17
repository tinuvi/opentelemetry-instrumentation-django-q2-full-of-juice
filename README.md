# OpenTelemetry instrumentation for django-q2

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice&metric=coverage)](https://sonarcloud.io/summary/new_code?id=tinuvi_opentelemetry-instrumentation-django-q2-full-of-juice)

Transparent OpenTelemetry instrumentation for [`django-q2`](https://github.com/django-q2/django-q2). Propagates trace context through the producer → broker → worker chain so cascading task graphs (HTTP request → task A → task B → task C) appear as one continuous distributed trace.

## Installation

This package does **not** pull in a `django_q` provider of its own. Install the
instrumentation plus exactly **one** provider — either upstream `django-q2` or the
`django-q2-full-of-juice` fork:

```bash
# Upstream django-q2
pip install opentelemetry-instrumentation-django-q2-full-of-juice django-q2

# …or the juice fork (adds chain-continuity / attempt / exc_info signals)
pip install opentelemetry-instrumentation-django-q2-full-of-juice django-q2-full-of-juice
```

Or, with Poetry:

```bash
poetry add opentelemetry-instrumentation-django-q2-full-of-juice django-q2
```

Both providers ship the **same** `django_q` import package under different PyPI
distribution names, so only one may be installed at a time. Installing both silently
clobbers files in `site-packages` (pip does not detect the conflict; last install wins)
— that's why the instrumentation declares neither as a runtime dependency.

> **⚠️ Do not run `pip install opentelemetry-instrumentation-django-q2-full-of-juice[instruments-any]`.**
> The package exposes an `instruments-any` extra listing *both* providers, but the extra
> exists as **machine-readable metadata** for OpenTelemetry tooling
> (`opentelemetry.instrumentation.dependencies` reads the `extra == "instruments-any"`
> markers to know which distributions satisfy this instrumentation) — installing it would
> pull in *both* providers and recreate the clobbering. Pick one provider by name instead.

Requires Python ≥ 3.12, Django ≥ 5.2.11, and django-q2 ≥ 1.10.0 (or django-q2-full-of-juice ≥ 0.1.0).

## Quick start

```python
from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor

DjangoQ2Instrumentor().instrument()
```

Call this once before workers fork (e.g. in your project's `AppConfig.ready()`, or via the `opentelemetry-instrument` CLI bootstrap).

## Capabilities

A scan-map of what the instrumentor emits and how you turn each piece on. Legend: ✅ supported · ⚙️ automatic, zero config · ⚠️ supported, with a caveat · `—` no dedicated end-to-end spec yet (still covered by the Python suite).

| Capability | Status | How you get it | E2E proof |
|---|:---:|---|---|
| Producer (`PUBLISH`) span bracketing `async_task` — real broker-publish latency | ✅ | call `instrument()` before workers fork | `producer-consumer`, `durations` |
| Consumer (`PROCESS`) span around task execution | ⚙️ | automatic | `producer-consumer` |
| Trace-context propagation producer → worker (carrier rides the signed payload) | ⚙️ | automatic | `producer-consumer` |
| **Baggage** propagation across the queue boundary | ⚙️ | automatic (composite propagator) | `baggage` |
| **Cascading** tasks — a nested `async_task` parents under the consumer span | ✅ | automatic | `cascading` |
| **Chain** (`async_chain`) continuity — every link lands on one trace&nbsp;† | ✅ | automatic | `chain-continuity` |
| **Scheduled / cron** tasks (`schedule`) | ⚠️ instrumented, but each run roots a fresh trace (the scheduler has no inbound parent) | automatic | `—` |
| **Sync mode** (`sync=True`) | ✅ | automatic | `—` |
| Error status + `exception` events — including cause-chains (`raise B from A` → two events) and `add_note()`&nbsp;† | ✅ | automatic | `error-handling`, `exception-passthrough` |
| **Retry / attempt** tracking (`django_q2.attempt`)&nbsp;† | ✅ | automatic | `retry` |
| Producer **and** consumer duration histograms, bounded cardinality | ✅ | `meter_provider=` or the global meter | `metrics`, `durations` |
| Messaging semconv + `broker.type` + `timeout` + worker / `client.id` attributes | ✅ | automatic | `attributes`, `iter` |
| Bring-your-own providers (`tracer_provider=` / `meter_provider=`) | ✅ | pass them to `instrument()` | `—` |
| Zero-code activation via the `opentelemetry-instrument` CLI | ⚙️ | run your process under the CLI (the entry point is registered) | `—` |
| `uninstrument()` — full, idempotent teardown | ✅ | `DjangoQ2Instrumentor().uninstrument()` | `—` |

&nbsp;† Available with the [`django-q2-full-of-juice`](https://github.com/tinuvi/django-q2-full-of-juice) runtime — the pairing this package is built for. On a stock `django-q2` install these degrade gracefully rather than break; see [Caveats](#caveats).

End-to-end specs live under `playwright/tests/` and `playwright/tests-juice/`; the names above are the spec files (minus the `.spec.ts` suffix).

## Setup options

Pick whichever fits your bootstrap — the instrumentor behaves identically either way.

**Programmatic (default).** Call `instrument()` once before any worker forks; `AppConfig.ready()` is the canonical spot:

```python
from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor

DjangoQ2Instrumentor().instrument()
```

**Bring your own providers.** Skip the global lookup and hand the instrumentor explicit providers — handy when you build the SDK yourself (e.g. per worker after `os.fork`):

```python
DjangoQ2Instrumentor().instrument(
    tracer_provider=my_tracer_provider,
    meter_provider=my_meter_provider,
)
```

**Zero code via the CLI.** The package registers an `opentelemetry_instrumentor` entry point, so the bootstrap CLI activates it with no call site of your own:

```bash
opentelemetry-instrument python manage.py qcluster
```

This is also the simplest fork-safe story: each worker initializes its own SDK on import, so the `BatchSpanProcessor` background threads live in the process that actually exports (see [Caveats](#caveats)).

**Teardown.** `DjangoQ2Instrumentor().uninstrument()` unwinds everything — the `async_task` wrap, every signal connection, and any per-task context — and is idempotent, so it's safe to call from a test `tearDown`.

## How it works

The instrumentor connects to django-q2's signal lifecycle. The PRODUCER span is opened by a `wrapt` wrapper around `django_q.tasks.async_task` (so it brackets the broker call); signals enrich it and bridge to the consumer side.

| Signal | Process | Role |
|---|---|---|
| `pre_enqueue(task)` | Producer | Enrich the active PRODUCER span (opened by the `async_task` wrap) with task-dict attributes, then inject trace context into `task["otel_carrier"]`. Falls back to opening a near-zero-duration span if the wrap was bypassed — see Caveats. |
| `post_spawn(proc_name)` | Worker | Capture the worker `proc_name` so later consumer spans can stamp `django_q2.worker` / `messaging.client.id`. |
| `pre_execute(func, task)` | Worker | Extract carrier, start CONSUMER span as child of the extracted context, attach as the current OTel context. |
| `post_execute_in_worker(func, task)` | Worker | Set span status from `task["success"]`, re-inject the carrier with the CONSUMER span's traceparent (so the next chain link can parent under it on the juice fork), end CONSUMER span, detach context, record the `django_q2.task.duration` histogram. |
| `pre_chain_progress(task)` | Monitor (juice fork only) | Extract the just-finished task's re-injected carrier and attach it as the current OTel context so the next `async_chain` link's PRODUCER span parents under the previous CONSUMER span. Silently absent on upstream `django-q2`. |
| `post_chain_progress(task)` | Monitor (juice fork only) | Detach the context attached by `pre_chain_progress`. |

Because the consumer span is the current OTel context **during** task execution, any nested `async_task(...)` call inside a task automatically parents under it — that's how the cascading chain composes.

The chain-progress hooks above are only fired by the [`tinuvi/django-q2-full-of-juice`](https://github.com/tinuvi/django-q2-full-of-juice) fork, which adds two `Signal()` instances on top of upstream and wraps `async_chain(...)` with them inside `django_q.monitor`. The instrumentor connects opportunistically: when the fork is installed it lights up; on upstream `django-q2` the import fails silently and chain links 2..N keep starting fresh traces (the existing caveat).

The carrier travels inside the pickled, signed payload (not in broker headers), so it's confidentiality-bound to producers/workers that share `Q_CLUSTER`'s `SECRET_KEY`. Fine for django-q2↔django-q2 propagation; not suitable for non-django-q2 observers reading the broker directly.

## Baggage propagation

The carrier the instrumentor injects is the full W3C context, so OpenTelemetry **Baggage** rides across the queue alongside the trace — there's no separate switch to flip. Whatever you set at the HTTP edge (the usual `user.id` / `tenant.id` / `request.id` correlation keys) is in scope on the producer span, the consumer span, and every cascaded producer/consumer below it, without any layer restamping it by hand.

It works because the instrumentor uses OpenTelemetry's default composite propagator (`tracecontext,baggage`) to write the carrier and re-attach it before the task runs. Swap in a TraceContext-only propagator and baggage stops flowing — the `baggage` E2E spec pins exactly that contract so a future carrier refactor can't silently drop it.

Two cautions:

- **No PII in Baggage.** The values travel inside the signed task payload, and — if you also run the HTTP/`requests` instrumentations — Baggage propagates on outbound calls to third parties too. Keep it to non-sensitive, non-joinable correlation keys.
- Baggage is bound to the same `SECRET_KEY` confidentiality boundary as the rest of the carrier (described just above).

## Span attributes

Every emitted span carries OpenTelemetry messaging semantic-convention attributes:

| Attribute | Value | Notes |
|---|---|---|
| `messaging.system` | `"django_q2"` | |
| `messaging.operation.type` | `"publish"` (producer) / `"process"` (consumer) | |
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
| `django_q2.attempt` | positive int | only when `task["attempt"]` is set — the [`tinuvi/django-q2-full-of-juice`](https://github.com/tinuvi/django-q2-full-of-juice) fork's pusher stamps this on every dequeue (`1` on first delivery, `N >= 2` on re-deliveries). Absent on upstream `django-q2` and in sync mode (the pusher is bypassed) — that absence is itself the cleanest "no retry instrumentation available" signal. Stamped on attempt 1 too so dashboards can express `attempt > 1` without disambiguating "no retries" from "no instrumentation". Not added to histogram labels (same cardinality argument as `django_q2.broker.type`). |

Consumer spans inherit `Status(ERROR)` with the underlying error message when `task["success"]` is `False`, and gain one or more standard `exception` events. The shape depends on which django-q2 build is installed: on upstream `django-q2 1.10.x` the live exception object is discarded before `post_execute_in_worker` fires, so we parse `task["result"]`'s `"{e} : {traceback}"` string into `exception.type` / `exception.message` / `exception.stacktrace` and emit a single event. On the [`tinuvi/django-q2-full-of-juice`](https://github.com/tinuvi/django-q2-full-of-juice) fork the worker forwards `sys.exc_info()` to the signal, so we call `span.record_exception(exc)` per link in the `__cause__` / `__context__` chain — `raise B from A` lands two events (one each for `B` and `A`), each addressable by `exception.type` in dashboards. Python 3.11+ `add_note()` annotations are surfaced in `exception.stacktrace`. `otel.status_description` carries `str(exc)` on the outermost exception (juice path) or the formatted prefix from `task["result"]` (upstream path). Backends like Jaeger, Tempo, and Grafana render these events as the span's error details.

## Metrics

| Metric | Type | Unit | Labels | Recorded by |
|---|---|---|---|---|
| `django_q2.task.duration` | histogram | `s` (seconds) | `messaging.destination.name`, `django_q2.func`, `status` (`"success"` / `"error"`) | Consumer — wall-clock time inside the worker (the user's function). |
| `django_q2.publish.duration` | histogram | `s` (seconds) | same as above | Producer — wall-clock time inside the `async_task` call (`broker.enqueue` + signing in async mode; full inline run in sync mode). |

Plumb a meter provider with `DjangoQ2Instrumentor().instrument(meter_provider=...)`, or rely on the global one set by `opentelemetry.metrics.set_meter_provider(...)`. Cardinality is bounded intentionally: task name and task id are deliberately **not** labels — they would explode any non-trivial workload. Operators can split a slow broker (`publish.duration` rising, `task.duration` flat) from slow workers (the inverse) without leaving the same dashboard.

`django_q2.broker.type` is also deliberately **not** a metric label. django-q2 has a single broker per cluster, so most fleets would carry a constant value on every histogram series — pure noise with no analytical payoff. Adding a label later is a backward-compatible change; removing one is breaking. The attribute is still emitted on every PRODUCER and CONSUMER span, so operators running multiple cluster types can split traces by backend via span queries.

## Scheduled & cron tasks

`schedule(...)` (one-off `ONCE`, minutely/hourly/daily, `CRON`, …) is dispatched by django-q2's scheduler, which calls `async_task` for each due run (`django_q/scheduler.py`). So scheduled and cron tasks are instrumented exactly like a direct call — producer span, consumer span, the full attribute pack, and both duration histograms all apply.

The one difference is trace shape. The scheduler runs in the cluster process, which has no inbound request context, so **each scheduled run roots its own fresh trace** rather than hanging off an HTTP request. `django_q2.group` and `django_q2.chain_length` are still stamped, so a scheduled task that fans out into a chain stays pivotable by group.

## Caveats

- The PRODUCER span is opened by a `wrapt` wrapper around `django_q.tasks.async_task` so it brackets `broker.enqueue` and reports real publish latency. If user code does `from django_q.tasks import async_task` at module-import time **before** `DjangoQ2Instrumentor().instrument()` runs, that reference bypasses the wrapper; in that case the `pre_enqueue` handler falls back to emitting a near-zero-duration PRODUCER span so the trace shape stays correct. Calling `instrument()` from `AppConfig.ready()` (or bootstrapping with `opentelemetry-instrument`) avoids this — Django's URL/views imports happen after `ready()`.
- django-q2 forks workers; OpenTelemetry SDK background threads (e.g. `BatchSpanProcessor`) do not survive `os.fork`. Either bootstrap with the `opentelemetry-instrument` CLI (each worker initializes its own SDK on import) or configure your tracer provider from a `post_spawn` handler.
- `task["hook"]` is only stamped as `django_q2.hook` when it's a dotted-path string. django-q2 also accepts a callable hook, but `repr`-ing a function pointer leaks a memory address that's useless for grouping or filtering, so the callable case is intentionally skipped.
- The `django_q2.worker` / `messaging.client.id` attribute is captured from the first `post_spawn` signal in each worker process. django-q2 fires that signal at the top of the worker loop (both for forked workers and `sync=True`), so the attribute is present on every consumer span in normal use. It is absent only if `pre_execute` is fired manually (e.g. by tests) before any `post_spawn` ran.
- **`async_chain` continuity:** django-q2 progresses a chain by having its `monitor` process call `async_chain(task["chain"], ...)` after each link completes. On upstream `django-q2` the `monitor` has no ambient OTel context, so only the *first* chain link sits under the trace that started it; subsequent links land in fresh traces. `django_q2.chain_length` and `django_q2.group` are still stamped on every span so dashboards can pivot the rest of the pipeline by group. On the [`tinuvi/django-q2-full-of-juice`](https://github.com/tinuvi/django-q2-full-of-juice) fork the limitation is lifted: the fork wraps `async_chain` with `pre_chain_progress` / `post_chain_progress` signals, the instrumentor restores the just-finished task's consumer-side trace context in the monitor process, and every chain link lands on the same trace as the chain head (`PRODUCER_A → CONSUMER_A → PRODUCER_B → CONSUMER_B → ...`). No configuration toggle is required — the instrumentor opportunistically connects to the fork-only signals when they're importable and falls back to the upstream behavior otherwise.
