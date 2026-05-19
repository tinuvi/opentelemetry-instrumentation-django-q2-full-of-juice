# sample-project

A minimal Django application that uses `django-q2` and the
`opentelemetry-instrumentation-django-q2-full-of-juice` library. It exists to be
driven by the Playwright E2E suite under `../playwright/` — never published, never
shipped.

## Layout

```
sample-project/
├── Dockerfile          # Builds an image with the library installed editable from ../
├── docker-compose.yml  # jaeger + web (runserver) + worker (qcluster), shared sqlite via volume
├── manage.py
├── sample/             # Django project (settings, urls, wsgi, asgi)
├── tasks_app/          # Django app: tasks, views, OTel bootstrap
└── scripts/            # start-web.sh, start-worker.sh
```

The build context for the Dockerfile is the **repo root** (one level up) so the sample
can install the library from source. See `docker-compose.yml`.

## Stack

- **Jaeger all-in-one** (`jaegertracing/all-in-one:1.63.0`) — runs the OTLP receivers
  (gRPC `:4317`, HTTP `:4318`) and the Query API + UI on `:16686`. No OTel Collector
  in between; the sample's web and worker push directly into Jaeger via OTLP gRPC.
- **web** — Django `runserver` on `:8000`, exposing `POST /api/enqueue/` and `GET
  /health/`.
- **worker** — `python manage.py qcluster` with 2 worker processes.
- Shared sqlite at `/data/db.sqlite3` (WAL) for the django-q2 ORM broker.

## Run

From the `sample-project/` directory:

```bash
docker compose up --build
```

Then in another terminal:

```bash
# Enqueue a cascading chain (HTTP → producer → consumer → producer → consumer)
curl -X POST http://localhost:8000/api/enqueue/ \
  -H 'Content-Type: application/json' \
  -d '{"task": "cascade_two", "args": ["hello"], "trigger_span": "demo-cascade-two"}'

# Open Jaeger UI:
open http://localhost:16686
# Service = sample-web, Operation = demo-cascade-two

# Or query Jaeger's HTTP API directly:
curl "http://localhost:16686/api/traces?service=sample-web&operation=demo-cascade-two&limit=1" | jq
```

Tear down:

```bash
docker compose down --volumes
```

## HTTP surface

| Method | Path             | Body                                                                              | Purpose                                  |
|--------|------------------|-----------------------------------------------------------------------------------|------------------------------------------|
| GET    | `/health/`       | —                                                                                 | Compose healthcheck for the web service. |
| POST   | `/api/enqueue/`  | `{"task": "<name>", "args": [...], "kwargs": {...}, "trigger_span": "<name>"}`    | Enqueue a registered task. Returns `{"task_id", "task", "trace_id"}`. |

`trigger_span` becomes the operation name of the HTTP root span — Playwright passes a
unique value per scenario so it can query Jaeger for exactly that trace.

Registered task names: `noop`, `add`, `boom`, `cascade_two`, `cascade_three`. They mirror
`tests/fixtures.py` so the same scenarios exercised in the unit suite are reproducible
end-to-end against a real `django-q2` cluster.

## How spans get to Jaeger

`tasks_app.apps.TasksAppConfig.ready()` runs in every process (web and each forked
worker), configures a `TracerProvider` with a `SimpleSpanProcessor` + the OTLP gRPC
exporter, and calls `DjangoQ2Instrumentor().instrument()`. The exporter reads its
endpoint from `OTEL_EXPORTER_OTLP_ENDPOINT` (set in `docker-compose.yml`).

`SimpleSpanProcessor` is used deliberately — it flushes synchronously, so it has no
background thread to die during django-q2's `fork()`. Production code should use
`BatchSpanProcessor` and re-initialize the SDK inside the `post_spawn` signal handler
(see `HANDOFF.md §7`).

## Caveats

- sqlite + WAL is used so two processes can share `/data/db.sqlite3`. Fine for the
  harness — not a recommendation. Use PostgreSQL or Redis for anything real.
- `runserver`'s auto-reloader can fork twice; the AppConfig guard against re-init keeps
  that safe.
- Jaeger all-in-one's in-memory store loses traces when the container restarts. The
  Playwright suite uses per-test `trigger_span` names so test order is irrelevant.
