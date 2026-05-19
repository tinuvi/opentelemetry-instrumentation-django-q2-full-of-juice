import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  JaegerSpan,
  enqueueTask,
  fetchTraceWhenReady,
  spanByOperation,
  tag,
} from '../helpers/jaeger';

// Jaeger surfaces OTel events as `logs[].fields` with `event`=<name> plus the
// event's attributes mapped to additional fields. Pulling the named field out
// of a single log entry keeps the rest of the test readable.
function logField(span: JaegerSpan, eventName: string, field: string): string | undefined {
  const logs = (span as unknown as { logs?: Array<{ fields: Array<{ key: string; value: string }> }> }).logs ?? [];
  for (const log of logs) {
    const isEvent = log.fields.some(f => f.key === 'event' && f.value === eventName);
    if (!isEvent) continue;
    return log.fields.find(f => f.key === field)?.value;
  }
  return undefined;
}

test.describe('error handling', () => {
  test('failing task marks the CONSUMER span with ERROR status', async ({ request }) => {
    const trigger = unique('e2e-boom-status');

    const enqueue = await enqueueTask(request, {
      task: 'boom',
      trigger_span: trigger,
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    // Jaeger surfaces span Status via these conventional tags.
    expect(tag(consumer, 'otel.status_code')).toBe('ERROR');
    expect(tag(consumer, 'error')).toBe(true);

    const description = tag(consumer, 'otel.status_description');
    expect(description).toBeDefined();
    expect(String(description)).toContain('boom');
  });

  test('failing task records an exception event with type, message and stacktrace', async ({ request }) => {
    // Proves gap #1 fix: the consumer span carries a standard OTel `exception`
    // event so downstream UIs (Jaeger, Tempo, Grafana) can render the traceback.
    const trigger = unique('e2e-boom-event');

    const enqueue = await enqueueTask(request, {
      task: 'boom',
      trigger_span: trigger,
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    expect(logField(consumer, 'exception', 'exception.type')).toBe('RuntimeError');
    expect(logField(consumer, 'exception', 'exception.message')).toContain('boom');
    const stack = logField(consumer, 'exception', 'exception.stacktrace');
    expect(stack).toBeDefined();
    expect(String(stack)).toContain('RuntimeError');
    expect(String(stack)).toContain('boom');
  });
});
