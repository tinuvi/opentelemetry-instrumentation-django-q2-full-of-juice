import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  enqueueTask,
  fetchTraceWhenReady,
  parentOf,
  serviceOf,
  spanByOperation,
  spansByKind,
} from '../helpers/jaeger';

test.describe('cascading context propagation', () => {
  test('scenario 2 — HTTP → task A → task B keeps one continuous trace', async ({ request }) => {
    const trigger = unique('e2e-cascade-two');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_two',
      trigger_span: trigger,
      args: ['hello'],
    });

    // HTTP root + (producer + consumer) × 2 = 5 spans total.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 5);

    const http = spanByOperation(trace, trigger);
    const producerA = spanByOperation(trace, 'async_task/tasks_app.tasks.cascade_two');
    const consumerA = spanByOperation(trace, 'run/tasks_app.tasks.cascade_two');
    const producerB = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumerB = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    // Chain: HTTP → PRODUCER_A → CONSUMER_A → PRODUCER_B → CONSUMER_B
    expect(parentOf(producerA)).toBe(http.spanID);
    expect(parentOf(consumerA)).toBe(producerA.spanID);
    expect(parentOf(producerB)).toBe(consumerA.spanID);
    expect(parentOf(consumerB)).toBe(producerB.spanID);

    // The cascade producer is recorded by the worker (it was enqueued from inside a task).
    expect(serviceOf(trace, http)).toBe('sample-web');
    expect(serviceOf(trace, producerA)).toBe('sample-web');
    expect(serviceOf(trace, consumerA)).toBe('sample-worker');
    expect(serviceOf(trace, producerB)).toBe('sample-worker');
    expect(serviceOf(trace, consumerB)).toBe('sample-worker');

    expect(spansByKind(trace, 'producer')).toHaveLength(2);
    expect(spansByKind(trace, 'consumer')).toHaveLength(2);
  });

  test('scenario 3 — HTTP → A → B → C stays on one trace (7 spans)', async ({ request }) => {
    const trigger = unique('e2e-cascade-three');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_three',
      trigger_span: trigger,
      args: ['deep'],
    });

    // HTTP root + (producer + consumer) × 3 = 7 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);

    const http = spanByOperation(trace, trigger);
    const pc3 = spanByOperation(trace, 'async_task/tasks_app.tasks.cascade_three');
    const cc3 = spanByOperation(trace, 'run/tasks_app.tasks.cascade_three');
    const pc2 = spanByOperation(trace, 'async_task/tasks_app.tasks.cascade_two');
    const cc2 = spanByOperation(trace, 'run/tasks_app.tasks.cascade_two');
    const pleaf = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const cleaf = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(parentOf(pc3)).toBe(http.spanID);
    expect(parentOf(cc3)).toBe(pc3.spanID);
    expect(parentOf(pc2)).toBe(cc3.spanID);
    expect(parentOf(cc2)).toBe(pc2.spanID);
    expect(parentOf(pleaf)).toBe(cc2.spanID);
    expect(parentOf(cleaf)).toBe(pleaf.spanID);

    // Same trace_id all the way through.
    for (const span of trace.spans) {
      expect(span.traceID).toBe(enqueue.trace_id);
    }

    expect(spansByKind(trace, 'producer')).toHaveLength(3);
    expect(spansByKind(trace, 'consumer')).toHaveLength(3);
  });
});
