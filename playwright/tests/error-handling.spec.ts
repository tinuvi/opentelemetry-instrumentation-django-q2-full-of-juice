import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  enqueueTask,
  fetchTraceWhenReady,
  spanByOperation,
  tag,
} from '../helpers/jaeger';

test.describe('error handling', () => {
  test('failing task marks the CONSUMER span with ERROR status', async ({ request }) => {
    const trigger = unique('e2e-boom');

    const enqueue = await enqueueTask(request, {
      task: 'boom',
      trigger_span: trigger,
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    // Jaeger conventionally exposes failures via the `error=true` tag and an
    // `otel.status_code` tag with value "ERROR" (set by the OTLP receiver from
    // the span Status).
    expect(tag(consumer, 'otel.status_code')).toBe('ERROR');
    expect(tag(consumer, 'error')).toBe(true);

    const description = tag(consumer, 'otel.status_description');
    expect(description).toBeDefined();
    expect(String(description)).toContain('boom');
  });
});
