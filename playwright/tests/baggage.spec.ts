import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import { enqueueWithBaggage, fetchTraceWhenReady, spanByOperation, tag } from '../helpers/jaeger';

// OTel baggage is the cross-cutting key/value channel that rides alongside the
// trace context. Setting it once at the HTTP edge (typical use: `user.id`,
// `tenant.id`, `request.id`) makes those values available to every nested span
// — HTTP → producer → consumer → cascaded producers/consumers — without code
// at every layer manually stamping the attribute.
//
// This is regression-prevention: baggage already works today because the
// instrumentor uses OTel's default composite propagator (`tracecontext,baggage`)
// to inject the carrier and re-attach it before pre_execute. Pinning the
// contract here guards against a future carrier-handling refactor that
// silently swaps in a TraceContext-only propagator.
//
// The sample's `read_baggage` task reads `baggage.get_all()` while the consumer
// span is current and stamps each entry as `baggage.<key>`, so we can assert
// from Jaeger without needing to inspect the worker process directly.
test.describe('OTel baggage propagation through django-q2 carrier', () => {
  test('baggage set at HTTP edge surfaces on the consumer span', async ({ request }) => {
    const trigger = unique('e2e-baggage-single-key');

    const enqueue = await enqueueWithBaggage(request, {
      trigger_span: trigger,
      baggage: { 'user.id': 'u-42' },
    });

    // HTTP root + producer + consumer = 3 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.read_baggage');

    // `read_baggage` stamps each baggage entry as `baggage.<key>`. If
    // propagation broke, the attribute would be absent.
    expect(tag(consumer, 'baggage.user.id')).toBe('u-42');
  });

  test('multiple baggage keys all flow through to the consumer span', async ({ request }) => {
    // Typical real-world payload: user + tenant + request correlation IDs. Verifies
    // the carrier-side serialization handles more than one key at a time.
    const trigger = unique('e2e-baggage-multi-key');

    const enqueue = await enqueueWithBaggage(request, {
      trigger_span: trigger,
      baggage: {
        'user.id': 'u-100',
        'tenant.id': 't-7',
        'request.id': 'req-abc-123',
      },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.read_baggage');

    expect(tag(consumer, 'baggage.user.id')).toBe('u-100');
    expect(tag(consumer, 'baggage.tenant.id')).toBe('t-7');
    expect(tag(consumer, 'baggage.request.id')).toBe('req-abc-123');
  });

  test('baggage is NOT present when none was set at the edge', async ({ request }) => {
    // Negative control: a `read_baggage` task whose producer didn't set any
    // baggage must surface no `baggage.*` attributes. Otherwise the positive
    // tests above could be silently picking up entries from a noisy default
    // propagator or a leaked context from a previous request.
    const trigger = unique('e2e-baggage-empty');

    // The dedicated endpoint requires a non-empty baggage dict by design (it
    // exists *to* test baggage). For the empty case we use the plain enqueue
    // endpoint with the read_baggage task — no baggage was set, so the task
    // sees an empty dict.
    const res = await request.post('/api/enqueue/', {
      data: { task: 'read_baggage', trigger_span: trigger, args: [] },
    });
    expect(res.ok()).toBeTruthy();
    const enqueue = (await res.json()) as { task_id: string; trace_id: string };

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.read_baggage');

    // Sample two keys that the positive tests use — neither should appear.
    expect(tag(consumer, 'baggage.user.id')).toBeUndefined();
    expect(tag(consumer, 'baggage.tenant.id')).toBeUndefined();
  });
});
