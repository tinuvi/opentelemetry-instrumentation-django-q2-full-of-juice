# Playwright tests

End-to-end verification for `opentelemetry-instrumentation-django-q2-full-of-juice`,
driven through `../sample-project/` against a real Jaeger instance.

## What this proves

The unit suite at `../tests/` mocks signals into a Python in-memory exporter. These
tests close the loop end-to-end: they enqueue real tasks through HTTP, let
`django-q2`'s forked worker process them, and then query Jaeger's HTTP API to assert
the parent-child shape and attributes of the emitted spans.

## Prerequisites

- Node.js ≥ 20
- Docker + Docker Compose (the sample-project stack — `jaeger`, `web`, `worker` — runs
  in containers)

## Run locally

```bash
# 1. Bring up the stack (Jaeger + Django web + django-q2 worker)
cd ../sample-project
docker compose up --build -d

# 2. Install Playwright (once)
cd ../playwright
npm install
npm run install-browsers

# 3. Run the tests
npm test
```

Tear down:

```bash
cd ../sample-project
docker compose down --volumes
```

## Layout

```
playwright/
├── helpers/
│   ├── data.ts        # unique(prefix) — per-test correlation id used as trigger_span
│   ├── jaeger.ts      # Jaeger query API client + tree helpers + enqueue/enqueueChain/enqueueIter
│   └── prometheus.ts  # OTel collector scrape helpers (parser + polling fetch)
├── tests/
│   ├── producer-consumer.spec.ts   # single task: HTTP → PRODUCER → CONSUMER, group, worker identifier
│   ├── cascading.spec.ts           # linear cascade, fan-out, mid-cascade failure isolation, non-HTTP root
│   ├── durations.spec.ts           # real broker-publish + consumer-side wall time
│   ├── error-handling.spec.ts      # failing task → consumer span ERROR status + exception event
│   ├── attributes.spec.ts          # django_q2.* attribute pack (hook, ack_failure, cached, task.name, ...)
│   ├── state.spec.ts               # django_q2.state on consumer (success / error / cascade)
│   ├── chain.spec.ts               # async_chain → decreasing django_q2.chain_length per layer
│   ├── iter.spec.ts                # async_iter → django_q2.iter_count on the umbrella task
│   ├── baggage.spec.ts             # OTel baggage at HTTP edge survives the carrier round-trip
│   └── metrics.spec.ts             # django_q2.task.duration + django_q2.publish.duration histograms
├── playwright.config.ts            # baseURL = $SAMPLE_PROJECT_URL or localhost:8000
├── tsconfig.json
└── package.json
```

## Sample-project HTTP surface used by the suite

| Endpoint | Purpose |
|---|---|
| `POST /api/enqueue/` | `async_task(...)` — most tests use this. Body: `{ task, trigger_span, args?, kwargs? }`. |
| `POST /api/enqueue-chain/` | `async_chain([(func, args, kwargs), ...])` — exercises `django_q2.chain_length`. |
| `POST /api/enqueue-iter/` | `async_iter(func, [(args,), ...])` — exercises `django_q2.iter_count`. |
| `POST /api/enqueue-detached/` | `async_task(...)` with the OTel context detached — producer becomes the trace root. Models schedulers / management commands. Body: `{ task, args?, kwargs? }` (no `trigger_span`). |
| `POST /api/enqueue-with-baggage/` | Sets OTel baggage, then enqueues `read_baggage` — exercises carrier baggage propagation. |
| `GET /health/` | Compose readiness probe. |

`fullyParallel: false` + `workers: 1` keeps the harness honest — the sample has a
single django-q2 cluster and per-test isolation already comes from the unique
`trigger_span` correlation IDs.

## How tests look up "their" trace

1. Test computes `trigger = unique('e2e-...')`.
2. POST `/api/enqueue/` with that `trigger_span`. The web view starts an OTel span
   named `trigger` around `async_task(...)`, so the entire downstream chain inherits
   its trace.
3. Response includes `trace_id` (hex). Tests poll `GET
   http://localhost:16686/api/traces/<trace_id>` until the trace has the expected
   number of spans, then assert tree shape via `references[0].refType=CHILD_OF`.

The detached-root cascading test (`/api/enqueue-detached/`) deliberately runs
without a trigger span — the PRODUCER itself is the trace root, so there is no
`trace_id` to return. That test resolves its trace with
`findTraceByTag('sample-web', 'messaging.message.id', task_id)`, which queries
Jaeger's tag-search API for the unique task id the endpoint just enqueued.

Jaeger's in-memory store is per-container — a `docker compose down --volumes` resets
it. Within a run, test order does not matter: tests find their trace by `trace_id`
(or by `messaging.message.id` in the detached-root case) alone.

## Environment

| Env var                    | Default                  | Meaning                                  |
|----------------------------|--------------------------|------------------------------------------|
| `SAMPLE_PROJECT_URL`       | `http://localhost:8000`  | Where the sample's web service is reachable. |
| `JAEGER_URL`               | `http://localhost:16686` | Where Jaeger's UI + Query API is reachable. |
| `COLLECTOR_PROMETHEUS_URL` | `http://localhost:8889`  | Where the OTel collector exposes Prometheus-format metrics (used by `metrics.spec.ts`). |
| `CI`                       | unset                    | Switches reporters to `html` + `github`, enables retries, forbids `test.only`. |

## CI

`.github/workflows/playwright.yml` brings the compose stack up, waits for `/health/`,
installs Playwright, and runs the suite. The HTML report is uploaded on every run; the
compose logs only on failure.
