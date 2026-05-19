import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  ChainEntry,
  enqueueChain,
  fetchTraceWhenReady,
  spanByOperation,
  spansByKind,
  tag,
} from '../helpers/jaeger';

// `async_chain([(func, args, kwargs), ...])` enqueues a sequential pipeline.
// django-q2 stamps `task["chain"]` on each task with the list of *remaining*
// tasks, so the first task sees `len(remaining) == total - 1`. The instrumentor
// surfaces that as `django_q2.chain_length`.
//
// **Limitation surfaced by this test:** chain progression happens inside
// django-q2's monitor process, which has no ambient OTel context — so each
// subsequent chain step lands in its own trace. The first step's spans (and
// only those) sit under the HTTP root that triggered the chain. Dashboards
// can still pivot the rest of the chain by `django_q2.group`.
test.describe('django_q2.chain_length — async_chain pipelines', () => {
  test('first chain step is parented under the HTTP root with chain_length = N-1', async ({ request }) => {
    const trigger = unique('e2e-chain-three');
    const chain: ChainEntry[] = [
      { task: 'noop', args: ['link-a'] },
      { task: 'noop', args: ['link-b'] },
      { task: 'noop', args: ['link-c'] },
    ];

    const enqueue = await enqueueChain(request, { trigger_span: trigger, chain });
    expect(enqueue.chain_length).toBe(3);

    // HTTP root + producer + consumer for the FIRST task only = 3 spans. The
    // remaining steps are enqueued by django-q2's monitor process (which has no
    // OTel context attached) and land in fresh traces. See the spec preamble.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    expect(spansByKind(trace, 'producer')).toHaveLength(1);
    expect(spansByKind(trace, 'consumer')).toHaveLength(1);

    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    // First task: 2 entries remain in `chain`, so chain_length must be 2.
    // This is the load-bearing assertion — it pins that we surface the
    // attribute correctly when django-q2 hands us a non-empty chain list.
    expect(tag(producer, 'django_q2.chain_length')).toBe(2);
    expect(tag(consumer, 'django_q2.chain_length')).toBe(2);

    // The group id is also stamped on both spans — that's how operators stitch
    // the rest of the chain's traces back together in dashboards.
    const producerGroup = tag(producer, 'django_q2.group');
    expect(typeof producerGroup).toBe('string');
    expect(tag(consumer, 'django_q2.group')).toBe(producerGroup);
  });

  test('a chain of one task still surfaces chain_length=0 (empty-list edge case)', async ({ request }) => {
    // Empty chain (`task["chain"] = []`) is still a list, so we tag it as 0.
    // Confirms the attribute doesn't silently disappear on the final link of a
    // chain — dashboards filtering "tasks that ran with chain_length=0" need
    // them visible.
    const trigger = unique('e2e-chain-one');
    const chain: ChainEntry[] = [{ task: 'noop', args: ['only-link'] }];

    const enqueue = await enqueueChain(request, { trigger_span: trigger, chain });
    expect(enqueue.chain_length).toBe(1);

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.chain_length')).toBe(0);
    expect(tag(consumer, 'django_q2.chain_length')).toBe(0);
  });
});
