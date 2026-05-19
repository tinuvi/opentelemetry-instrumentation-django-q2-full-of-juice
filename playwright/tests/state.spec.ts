import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import { enqueueTask, fetchTraceWhenReady, spanByOperation, tag } from '../helpers/jaeger';

// `django_q2.state` mirrors Celery's `celery.state` — it lets dashboards split
// terminal outcomes without parsing the OTel status code. Only set on the
// consumer span at end-of-task, never on the producer.
test.describe('django_q2.state — terminal state on the CONSUMER span', () => {
  test('successful task → django_q2.state="success"', async ({ request }) => {
    const trigger = unique('e2e-state-success');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');

    expect(tag(consumer, 'django_q2.state')).toBe('success');
    // Not stamped on the producer — the producer publishes; it doesn't know
    // the terminal outcome.
    expect(tag(producer, 'django_q2.state')).toBeUndefined();
  });

  test('failing task → django_q2.state="error" (alongside ERROR status + exception event)', async ({ request }) => {
    const trigger = unique('e2e-state-error');

    const enqueue = await enqueueTask(request, {
      task: 'boom',
      trigger_span: trigger,
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    expect(tag(consumer, 'django_q2.state')).toBe('error');
    // The attribute is consistent with the rest of the error surface — defensive
    // belt-and-braces so dashboards keying off any of the three keys agree.
    expect(tag(consumer, 'otel.status_code')).toBe('ERROR');
    expect(tag(consumer, 'error')).toBe(true);
  });

  test('every consumer in a cascade carries a state attribute', async ({ request }) => {
    // Three consumers across three layers — each must be tagged independently.
    const trigger = unique('e2e-state-cascade');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_three',
      trigger_span: trigger,
      args: ['hello'],
    });

    // 1 HTTP + 3 producer + 3 consumer = 7 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);

    const consumers = [
      spanByOperation(trace, 'run/tasks_app.tasks.cascade_three'),
      spanByOperation(trace, 'run/tasks_app.tasks.cascade_two'),
      spanByOperation(trace, 'run/tasks_app.tasks.noop'),
    ];

    for (const consumer of consumers) {
      expect(tag(consumer, 'django_q2.state'), `missing state on ${consumer.operationName}`).toBe('success');
    }
  });
});
