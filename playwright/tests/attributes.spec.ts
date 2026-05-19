import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import { enqueueTask, fetchTraceWhenReady, spanByOperation, tag } from '../helpers/jaeger';

test.describe('django_q2.* attribute pack', () => {
  test('string hook flows to django_q2.hook on both PRODUCER and CONSUMER', async ({ request }) => {
    // django-q2's `hook` kwarg accepts either a dotted path string or a callable.
    // Only the string form is recorded — repr of a function pointer leaks an
    // address. Pass a fake path here: we're not invoking the hook, just asserting
    // the attribute travels through the carrier and onto both span sides.
    const trigger = unique('e2e-attr-hook-string');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { hook: 'tasks_app.tasks.noop' },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.hook')).toBe('tasks_app.tasks.noop');
    expect(tag(consumer, 'django_q2.hook')).toBe('tasks_app.tasks.noop');
  });

  test('ack_failure flag flows to django_q2.ack_failure on both spans', async ({ request }) => {
    const trigger = unique('e2e-attr-ack-failure');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { ack_failure: true },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.ack_failure')).toBe(true);
    expect(tag(consumer, 'django_q2.ack_failure')).toBe(true);
  });

  test('plain task has none of the optional django_q2.* attributes', async ({ request }) => {
    const trigger = unique('e2e-attr-none');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    // The optional pack must not surface on a vanilla enqueue — otherwise we'd
    // be lying about why the task was configured the way it was.
    for (const span of [producer, consumer]) {
      expect(tag(span, 'django_q2.hook')).toBeUndefined();
      expect(tag(span, 'django_q2.ack_failure')).toBeUndefined();
      expect(tag(span, 'django_q2.cached')).toBeUndefined();
      expect(tag(span, 'django_q2.sync')).toBeUndefined();
      expect(tag(span, 'django_q2.iter_count')).toBeUndefined();
      expect(tag(span, 'django_q2.chain_length')).toBeUndefined();
    }
  });

  test('cached flag flows to django_q2.cached on both spans', async ({ request }) => {
    // `cached=60` enables django-q2's result cache for 60 s. The instrumentor
    // records the attribute as a boolean — we deliberately don't surface the
    // numeric TTL because dashboards care about "was caching on?" not the value.
    const trigger = unique('e2e-attr-cached');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { cached: 60 },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.cached')).toBe(true);
    expect(tag(consumer, 'django_q2.cached')).toBe(true);
  });

  test('django_q2.task.name is present (django-q2 auto-generates one)', async ({ request }) => {
    // django-q2 stamps `task["name"]` either from the caller's `task_name` kwarg
    // or by auto-generating a UUID-derived label. Either way the instrumentor
    // surfaces it as `django_q2.task.name` so dashboards have a stable per-task
    // identifier without needing the unbounded `messaging.message.id`.
    const trigger = unique('e2e-attr-taskname');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    const producerName = tag(producer, 'django_q2.task.name');
    expect(typeof producerName).toBe('string');
    expect(String(producerName)).not.toBe('');
    // Producer and consumer must agree on the name — they're observing the
    // same task dict through the carrier.
    expect(tag(consumer, 'django_q2.task.name')).toBe(producerName);
  });

  test('caller-supplied task_name kwarg surfaces verbatim on both spans', async ({ request }) => {
    // Confirms the dot-namespaced attribute reflects what the caller asked for,
    // not just the auto-generated UUID label.
    const trigger = unique('e2e-attr-taskname-explicit');
    const explicitName = `named-${Date.now()}`;

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { task_name: explicitName },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.task.name')).toBe(explicitName);
    expect(tag(consumer, 'django_q2.task.name')).toBe(explicitName);
  });
});
