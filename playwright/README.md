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
│   ├── jaeger.ts      # Jaeger query API client + tree helpers
│   └── prometheus.ts  # OTel collector scrape helpers (parser + polling fetch)
├── tests/
│   ├── producer-consumer.spec.ts   # single task: HTTP → PRODUCER → CONSUMER, group, worker identifier
│   ├── cascading.spec.ts           # multi-layer cascade keeps one trace
│   ├── durations.spec.ts           # real broker-publish + consumer-side wall time
│   ├── error-handling.spec.ts      # failing task → consumer span ERROR status + exception event
│   ├── attributes.spec.ts          # django_q2.* attribute pack (hook, ack_failure, ...)
│   └── metrics.spec.ts             # django_q2.task.duration histogram from the OTel collector
├── playwright.config.ts            # baseURL = $SAMPLE_PROJECT_URL or localhost:8000
├── tsconfig.json
└── package.json
```

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

Jaeger's in-memory store is per-container — a `docker compose down --volumes` resets
it. Within a run, test order does not matter: tests find their trace by `trace_id`
alone.

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
