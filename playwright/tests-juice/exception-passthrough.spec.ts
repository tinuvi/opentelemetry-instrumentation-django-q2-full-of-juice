import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  JaegerSpan,
  enqueueTask,
  fetchTraceWhenReady,
  spanByOperation,
  tag,
} from '../helpers/jaeger';

// What this proves end-to-end on `django-q2-full-of-juice`
// =========================================================
// The juice fork forwards `sys.exc_info()` to subscribers of
// `post_execute_in_worker`, so the instrumentor can call `record_exception`
// per cause-link instead of regex-parsing the formatted-string fallback
// upstream produces. Net effect on a chained `raise B from A`: two
// `exception` events on the consumer span (one per link), each addressable
// by `exception.type=<cls>` in Jaeger / Tempo / Grafana queries.
//
// Upstream `django-q2 1.10.x` collapses the chain into a single string in
// `task["result"]`, so the existing `playwright/tests/error-handling.spec.ts`
// only ever sees the outer exception. That asymmetry is the whole reason
// this spec lives under `tests-juice/`.

function allExceptionEvents(span: JaegerSpan): Array<Record<string, string>> {
  const logs = (span as unknown as { logs?: Array<{ fields: Array<{ key: string; value: string }> }> }).logs ?? [];
  const events: Array<Record<string, string>> = [];
  for (const log of logs) {
    const flat: Record<string, string> = {};
    for (const f of log.fields) {
      flat[f.key] = f.value;
    }
    if (flat.event === 'exception') {
      events.push(flat);
    }
  }
  return events;
}

test.describe('juice fork — exc_info pass-through on post_execute_in_worker', () => {
  test('chained exception records one event per cause link on the consumer span', async ({ request }) => {
    const trigger = unique('e2e-juice-chained-exception');

    const enqueue = await enqueueTask(request, {
      task: 'chained_failure',
      trigger_span: trigger,
      // The juice service runs with a low `Q_CLUSTER["retry"]` (see
      // docker-compose.yml) so failed tasks are re-popped by default after
      // ~5s. We don't want a silent retry to add a phantom consumer span to
      // this trace mid-assertion — `ack_failure=True` tells the monitor to
      // acknowledge the broker even on failure, suppressing the retry.
      q_options: { ack_failure: true },
    });

    // HTTP root + producer + consumer = 3 spans, same shape as upstream
    // error-handling. The interesting part is on the consumer span.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.chained_failure');

    expect(tag(consumer, 'otel.status_code')).toBe('ERROR');
    expect(tag(consumer, 'django_q2.state')).toBe('error');
    // Description = `str(exc)` on the outermost exception (the `RuntimeError`
    // raised by the task), NOT the formatted-string prefix the upstream path
    // produces. Lets dashboards keyed on `otel.status_description` show the
    // real top-level message even when the underlying cause has a different
    // representation.
    expect(String(tag(consumer, 'otel.status_description'))).toContain('outer failure');

    const events = allExceptionEvents(consumer);
    expect(events).toHaveLength(2);

    // Outermost first (the exception the task itself raised), then its
    // `__cause__`. Mirrors how Python itself prints chains and how the
    // instrumentor walks them.
    const [outer, inner] = events;
    expect(outer['exception.type']).toBe('RuntimeError');
    expect(outer['exception.message']).toContain('outer failure');
    expect(String(outer['exception.stacktrace'])).toContain('RuntimeError');

    expect(inner['exception.type']).toBe('ValueError');
    expect(inner['exception.message']).toContain('inner cause');
    expect(String(inner['exception.stacktrace'])).toContain('ValueError');
  });
});
