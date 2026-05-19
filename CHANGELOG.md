# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - Unreleased

### Added
- Initial project scaffolding for `opentelemetry-instrumentation-django-q2-full-of-juice`.
- `DjangoQ2Instrumentor` wired against django-q2's `pre_enqueue`, `pre_execute`, `post_execute_in_worker`, and `post_spawn` signals.
- Producer span (`async_task/<func>`, `SpanKind.PRODUCER`) emitted from `pre_enqueue`; the trace context is injected into `task["otel_carrier"]` so it survives pickling and ships through the broker.
- Consumer span (`run/<func>`, `SpanKind.CONSUMER`) started from `pre_execute` as a child of the extracted carrier context and activated as the current OTel context for the duration of the task — nested `async_task(...)` calls inside a task automatically parent under it, enabling end-to-end cascading.
- Consumer span status: `ERROR` with the underlying message when `task["success"]` is `False`; defensive about the sync-error branch in `django_q/worker.py` that emits `post_execute_in_worker` before `task["success"]`/`result` are set.
- Messaging semantic-convention attributes on every span: `messaging.system="django_q2"`, `messaging.operation`, `messaging.destination.name` (cluster name or `"default"`), `messaging.message.id`, plus `django_q2.task.name`, `django_q2.func`, and `django_q2.group`.
- `_uninstrument()` disconnects every signal handler and clears the per-task context store.
- Per-task context storage helpers in `opentelemetry_instrumentation_django_q2.utils`.
- Test scaffolding (`tests/testapp/`) with a minimal Django settings module wired to `django-q2` in `sync=True` mode.
- `messaging.operation.type` attribute (semconv-current name) on every span alongside the deprecated `messaging.operation` key, with values from `MessagingOperationTypeValues` (`publish` / `process`).
- Standard OTel `exception` event on the consumer span for failed tasks: parses the `"{e} : {traceback}"` string into `exception.type`, `exception.message`, and `exception.stacktrace` attributes so Jaeger/Tempo/Grafana surface the traceback natively.
- Tracer is now created with `schema_url="https://opentelemetry.io/schemas/1.28.0"`.
- `parse_worker_result()` helper in `opentelemetry_instrumentation_django_q2.utils` for extracting `(message, exception_type, stacktrace)` from a worker failure string.
- `messaging.message.conversation_id` attribute (semconv) on producer and consumer spans, mirroring `django_q2.group` so generic messaging dashboards group related messages without knowing the django-q2-specific key.
- `django_q2.task.duration` histogram metric (unit: seconds, labels: `messaging.destination.name`, `django_q2.func`, `status="success"|"error"`) recorded once per consumer task. `_instrument(meter_provider=...)` now accepts a meter provider.
- `django_q2.*` attribute pack on producer and consumer spans, derived from the task dict: `django_q2.cached`, `django_q2.sync`, `django_q2.ack_failure`, `django_q2.hook` (only when it's a string — callables are skipped), `django_q2.iter_count` (positive int), `django_q2.chain_length` (from a list-shaped `chain`).
- `django_q2.worker` and `messaging.client.id` attributes on consumer spans, captured once per worker process from `post_spawn`'s `proc_name`. Not stamped on producer spans (the producer doesn't know which worker will pick the task up).
- Regression test for `instrument()` idempotency — a second `instrument()` call must not double-fire signal handlers.

### Changed
- The PRODUCER span now brackets the whole `django_q.tasks.async_task` call (via a `wrapt` wrapper) instead of starting and ending inside `pre_enqueue`. Result: the span carries a real broker-publish duration. Pre-instrument imports (`from django_q.tasks import async_task` before `instrument()` runs) fall back to the legacy zero-duration path so trace shape stays correct.
- `_uninstrument()` also unwraps `async_task` so the original function is restored.
- Producer and consumer attribute setting consolidated into a shared `_apply_task_attributes` helper so both span sides stay in lockstep.
