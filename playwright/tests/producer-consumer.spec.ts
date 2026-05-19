import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  enqueueTask,
  fetchTraceWhenReady,
  parentOf,
  serviceOf,
  spanByOperation,
  spansByKind,
  tag,
} from '../helpers/jaeger';

test.describe('producer/consumer single task', () => {
  test('enqueue noop emits PRODUCER (web) + CONSUMER (worker) under the HTTP root', async ({ request }) => {
    const trigger = unique('e2e-single-noop');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello', 'world'],
    });

    // HTTP root + producer + consumer = 3 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    const http = spanByOperation(trace, trigger);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'span.kind')).toBe('producer');
    expect(tag(consumer, 'span.kind')).toBe('consumer');

    // Tree shape: HTTP → PRODUCER → CONSUMER (across processes).
    expect(parentOf(producer)).toBe(http.spanID);
    expect(parentOf(consumer)).toBe(producer.spanID);

    // Process attribution: producer recorded by sample-web, consumer by sample-worker.
    expect(serviceOf(trace, http)).toBe('sample-web');
    expect(serviceOf(trace, producer)).toBe('sample-web');
    expect(serviceOf(trace, consumer)).toBe('sample-worker');

    // Messaging semantic-convention attributes survive end-to-end.
    expect(tag(producer, 'messaging.system')).toBe('django_q2');
    expect(tag(producer, 'messaging.operation')).toBe('publish');
    expect(tag(producer, 'messaging.message.id')).toBe(enqueue.task_id);
    expect(tag(consumer, 'messaging.system')).toBe('django_q2');
    expect(tag(consumer, 'messaging.operation')).toBe('process');
    expect(tag(consumer, 'messaging.message.id')).toBe(enqueue.task_id);

    // Exactly one producer and one consumer in this trace.
    expect(spansByKind(trace, 'producer')).toHaveLength(1);
    expect(spansByKind(trace, 'consumer')).toHaveLength(1);
  });
});
