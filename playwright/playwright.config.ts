import { defineConfig, devices } from '@playwright/test';

// Two playwright "projects" share this config:
//   - `chromium` drives the upstream `django-q2` sample at $SAMPLE_PROJECT_URL.
//   - `juice` drives the `django-q2-full-of-juice` sample at $JUICE_SAMPLE_PROJECT_URL.
// Both stacks write to the same Jaeger ($JAEGER_URL) — traces are isolated by
// `service.name` (`sample-web` / `sample-worker` vs `juice-web` / `juice-worker`).
const UPSTREAM_URL = process.env.SAMPLE_PROJECT_URL ?? 'http://localhost:8000';
const JUICE_URL = process.env.JUICE_SAMPLE_PROJECT_URL ?? 'http://localhost:8001';

export default defineConfig({
  // Each scenario uses a unique correlation ID, so tests are independent — but
  // we run sequentially to keep Jaeger search logs readable and avoid worker
  // contention for the single django-q2 cluster in the sample.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  timeout: 90_000,
  expect: { timeout: 20_000 },
  reporter: process.env.CI
    ? [['html', { open: 'never' }], ['github']]
    : [['html', { open: 'on-failure' }], ['list']],
  use: {
    extraHTTPHeaders: { Accept: 'application/json' },
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      testDir: './tests',
      use: { ...devices['Desktop Chrome'], baseURL: UPSTREAM_URL },
    },
    {
      // `juice` runs the fork-only specs under ./tests-juice against the
      // `juice-web` service. Excluded from the default `npm test` so a stack
      // that didn't `docker compose --profile juice up` doesn't flap.
      // Trigger explicitly via `npm run test:juice` or `--project=juice`.
      name: 'juice',
      testDir: './tests-juice',
      use: { ...devices['Desktop Chrome'], baseURL: JUICE_URL },
    },
  ],
});
