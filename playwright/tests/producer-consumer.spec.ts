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

    // Messaging semantic-convention attributes survive end-to-end. We assert both
    // the new operation.type key and the legacy operation key to guard the
    // backward-compat shim — collectors on either schema version see the same value.
    expect(tag(producer, 'messaging.system')).toBe('django_q2');
    expect(tag(producer, 'messaging.operation.type')).toBe('publish');
    expect(tag(producer, 'messaging.operation')).toBe('publish');
    expect(tag(producer, 'messaging.message.id')).toBe(enqueue.task_id);
    expect(tag(consumer, 'messaging.system')).toBe('django_q2');
    expect(tag(consumer, 'messaging.operation.type')).toBe('process');
    expect(tag(consumer, 'messaging.operation')).toBe('process');
    expect(tag(consumer, 'messaging.message.id')).toBe(enqueue.task_id);

    // Exactly one producer and one consumer in this trace.
    expect(spansByKind(trace, 'producer')).toHaveLength(1);
    expect(spansByKind(trace, 'consumer')).toHaveLength(1);
  });

  test('task group flows to messaging.message.conversation_id on both spans', async ({ request }) => {
    // Group is django-q2's analogue of Celery's correlation_id. We expose it as
    // both `django_q2.group` (familiar key) and `messaging.message.conversation_id`
    // (current semconv) so generic messaging dashboards group related messages.
    const trigger = unique('e2e-group-conversation-id');
    const groupName = `group-${Date.now()}`;

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { group: groupName },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.group')).toBe(groupName);
    expect(tag(producer, 'messaging.message.conversation_id')).toBe(groupName);
    expect(tag(consumer, 'django_q2.group')).toBe(groupName);
    expect(tag(consumer, 'messaging.message.conversation_id')).toBe(groupName);
  });

  test('no group ⇒ no conversation_id / django_q2.group on either span', async ({ request }) => {
    const trigger = unique('e2e-no-group');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'messaging.message.conversation_id')).toBeUndefined();
    expect(tag(producer, 'django_q2.group')).toBeUndefined();
    expect(tag(consumer, 'messaging.message.conversation_id')).toBeUndefined();
    expect(tag(consumer, 'django_q2.group')).toBeUndefined();
  });

  test('CONSUMER carries worker identifier captured from post_spawn', async ({ request }) => {
    // post_spawn fires once per forked worker, before any task runs. The
    // instrumentor stashes proc_name and stamps it on every later CONSUMER span
    // so dashboards can pin a failing task to the exact worker process.
    const trigger = unique('e2e-worker-identifier');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');

    const worker = tag(consumer, 'django_q2.worker');
    expect(typeof worker).toBe('string');
    expect(String(worker)).not.toBe('');
    // semconv mirror: same value under messaging.client.id so generic dashboards
    // filter by worker without knowing django-q2's bespoke key.
    expect(tag(consumer, 'messaging.client.id')).toBe(worker);

    // The producer doesn't know which worker will pick the task up — stamping
    // it there would be a lie. Guard against that regressing.
    expect(tag(producer, 'django_q2.worker')).toBeUndefined();
    expect(tag(producer, 'messaging.client.id')).toBeUndefined();
  });

  test('PRODUCER span covers broker publish — not the old near-zero duration', async ({ request }) => {
    // Proves gap #2 fix: the wrap brackets broker.enqueue, so the PRODUCER span
    // has a real (>0) duration. Old behaviour would have surfaced ~0 µs here.
    const trigger = unique('e2e-producer-duration');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');

    // Jaeger reports duration in microseconds. A single ORM-broker enqueue hits
    // SQLite + a signing roundtrip — comfortably above 100µs in any real setup.
    expect(producer.duration).toBeGreaterThan(100);
  });
});
