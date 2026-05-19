import { defineConfig, devices } from '@playwright/test';

const SAMPLE_PROJECT_URL = process.env.SAMPLE_PROJECT_URL ?? 'http://localhost:8000';

export default defineConfig({
  testDir: './tests',

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
    baseURL: SAMPLE_PROJECT_URL,
    extraHTTPHeaders: { Accept: 'application/json' },
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
});
