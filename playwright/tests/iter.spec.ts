import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import { enqueueIter, fetchTraceWhenReady, spansByKind, tag } from '../helpers/jaeger';

// `async_iter(func, [(arg_tuple), ...])` enqueues N independent invocations of
// the same function and django-q2 stamps `task["iter_count"]` on each one.
// (django-q2's async_iter is a thin loop around async_task, not an umbrella
// task — each iteration produces its own PRODUCER+CONSUMER pair tied together
// by a shared group id.) The instrumentor surfaces iter_count on every span
// in the fan-out so dashboards can pivot on the batch size.
test.describe('django_q2.iter_count — async_iter batches', () => {
  test('every fan-out span carries iter_count and a shared group', async ({ request }) => {
    const trigger = unique('e2e-iter-count');
    const batch = [['iter-a'], ['iter-b'], ['iter-c'], ['iter-d']];

    const enqueue = await enqueueIter(request, {
      task: 'noop',
      trigger_span: trigger,
      args_iter: batch,
    });
    expect(enqueue.iter_count).toBe(4);

    // HTTP root + 4 producers + 4 consumers = 9 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 9);

    const producers = spansByKind(trace, 'producer');
    const consumers = spansByKind(trace, 'consumer');
    expect(producers).toHaveLength(4);
    expect(consumers).toHaveLength(4);

    // Every span — producer and consumer alike — must carry iter_count=4. That
    // tells dashboards "this came from a batch of 4" regardless of which side
    // they're looking at.
    for (const span of [...producers, ...consumers]) {
      expect(tag(span, 'django_q2.iter_count'), `missing iter_count on ${span.operationName}`).toBe(4);
    }

    // async_iter ties the fan-out together via a shared `group` id (django-q2's
    // analogue of Celery's correlation_id). All spans in this trace must agree
    // on that group, otherwise downstream dashboards can't roll the batch up.
    const groups = new Set([...producers, ...consumers].map(s => tag(s, 'django_q2.group')));
    expect(groups.size).toBe(1);
    expect([...groups][0]).toBeTruthy();
  });
});
