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
});
