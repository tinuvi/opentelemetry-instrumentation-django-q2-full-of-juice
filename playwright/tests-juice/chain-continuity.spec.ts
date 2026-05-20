import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  ChainEntry,
  enqueueChain,
  fetchTraceWhenReady,
  parentOf,
  serviceOf,
  spanByOperation,
  spansByKind,
  spansByOperation,
  tag,
} from '../helpers/jaeger';

// What this proves end-to-end on `django-q2-full-of-juice`
// =========================================================
// Upstream `django-q2` progresses an `async_chain([...])` pipeline inside its
// monitor process, which has no ambient OTel context. Without the fork's
// chain-progress signals only the *first* link sits under the trace that
// triggered the chain; links 2..N start fresh traces (see
// playwright/tests/chain.spec.ts — the spec running against the upstream
// stack still pins that limitation).
//
// On the juice fork, the instrumentor connects to `pre_chain_progress` /
// `post_chain_progress`, restores the just-finished CONSUMER's context inside
// the monitor process, and the next link's PRODUCER opens as a child of that
// CONSUMER. Net effect: every link of `async_chain([A, B, C])` lands on the
// HTTP-trigger's trace with the shape
//   HTTP → PRODUCER_A → CONSUMER_A → PRODUCER_B → CONSUMER_B → PRODUCER_C → CONSUMER_C.
//
// These specs run only against the `juice-web` / `juice-worker` services
// (Playwright `juice` project — `npm run test:juice`). The upstream cascade
// suite already proves the in-task `async_task(...)` cascading shape; we test
// only the chain-specific behaviour the fork unlocks.
test.describe('juice fork — async_chain cross-link continuity', () => {
  test('three-link chain lands on a single trace with the cascaded shape', async ({ request }) => {
    const trigger = unique('e2e-juice-chain-three');
    const chain: ChainEntry[] = [
      { task: 'noop', args: ['link-a'] },
      { task: 'noop', args: ['link-b'] },
      { task: 'noop', args: ['link-c'] },
    ];

    const enqueue = await enqueueChain(request, { trigger_span: trigger, chain });
    expect(enqueue.chain_length).toBe(3);

    // HTTP root + (producer + consumer) × 3 = 7 spans on ONE trace. That's the
    // load-bearing assertion: pre-fix the count was 3 (HTTP + first link only).
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);

    expect(spansByKind(trace, 'producer')).toHaveLength(3);
    expect(spansByKind(trace, 'consumer')).toHaveLength(3);

    const http = spanByOperation(trace, trigger);
    const producers = spansByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumers = spansByOperation(trace, 'run/tasks_app.tasks.noop');
    expect(producers).toHaveLength(3);
    expect(consumers).toHaveLength(3);

    // Pair each consumer to its producer via the CHILD_OF reference, then
    // sort chronologically so we can call the first one "link A". Sorting by
    // startTime — not by spanID — survives Jaeger's non-deterministic ingest
    // order across processes.
    const pairs = producers
      .map((producer) => {
        const consumer = consumers.find((c) => parentOf(c) === producer.spanID);
        if (!consumer) {
          throw new Error(`producer ${producer.spanID} has no matching consumer`);
        }
        return { producer, consumer };
      })
      .sort((a, b) => a.producer.startTime - b.producer.startTime);

    expect(pairs).toHaveLength(3);
    const [linkA, linkB, linkC] = pairs;

    // Tree shape: HTTP → PA → CA → PB → CB → PC → CC, all on the same trace.
    expect(parentOf(linkA.producer)).toBe(http.spanID);
    expect(parentOf(linkB.producer)).toBe(linkA.consumer.spanID);
    expect(parentOf(linkC.producer)).toBe(linkB.consumer.spanID);

    for (const span of trace.spans) {
      expect(span.traceID).toBe(trace.traceID);
    }

    // `django_q2.chain_length` is the count of *remaining* links recorded on
    // each task. With three links queued: A sees 2 remaining, B sees 1, C sees 0.
    // This is what makes the chain shape recoverable in dashboards even without
    // walking the tree.
    expect(tag(linkA.producer, 'django_q2.chain_length')).toBe(2);
    expect(tag(linkB.producer, 'django_q2.chain_length')).toBe(1);
    expect(tag(linkC.producer, 'django_q2.chain_length')).toBe(0);

    // First link's PRODUCER is enqueued by the web process; chain progression
    // happens inside the monitor (which runs in the worker container).
    expect(serviceOf(trace, http)).toBe('juice-web');
    expect(serviceOf(trace, linkA.producer)).toBe('juice-web');
    expect(serviceOf(trace, linkA.consumer)).toBe('juice-worker');
    expect(serviceOf(trace, linkB.producer)).toBe('juice-worker');
    expect(serviceOf(trace, linkB.consumer)).toBe('juice-worker');
    expect(serviceOf(trace, linkC.producer)).toBe('juice-worker');
    expect(serviceOf(trace, linkC.consumer)).toBe('juice-worker');

    // Every span carries the same `django_q2.group` (the chain's group id) —
    // that's how dashboards correlate links even without the parent-child
    // restoration we're proving here.
    const groupId = tag(linkA.producer, 'django_q2.group');
    expect(typeof groupId).toBe('string');
    for (const { producer, consumer } of pairs) {
      expect(tag(producer, 'django_q2.group')).toBe(groupId);
      expect(tag(consumer, 'django_q2.group')).toBe(groupId);
    }
  });

  test('single-link chain still produces one PRODUCER → CONSUMER pair (no spurious spans)', async ({ request }) => {
    // Edge case: a chain of one is enqueued through `async_chain` but never
    // triggers a `pre_chain_progress` cycle in the monitor (the chain list is
    // empty after the first pop). The instrumentor must not emit phantom
    // spans for the no-op progression — same shape as a single async_task.
    const trigger = unique('e2e-juice-chain-one');
    const chain: ChainEntry[] = [{ task: 'noop', args: ['only-link'] }];

    const enqueue = await enqueueChain(request, { trigger_span: trigger, chain });
    expect(enqueue.chain_length).toBe(1);

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    expect(spansByKind(trace, 'producer')).toHaveLength(1);
    expect(spansByKind(trace, 'consumer')).toHaveLength(1);

    const http = spanByOperation(trace, trigger);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(parentOf(producer)).toBe(http.spanID);
    expect(parentOf(consumer)).toBe(producer.spanID);
    expect(tag(producer, 'django_q2.chain_length')).toBe(0);
    expect(tag(consumer, 'django_q2.chain_length')).toBe(0);
  });
});
