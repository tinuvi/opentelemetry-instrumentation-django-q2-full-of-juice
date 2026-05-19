import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  JaegerSpan,
  enqueueDetached,
  enqueueTask,
  fetchTraceWhenReady,
  findTraceByTag,
  parentOf,
  serviceOf,
  spanByOperation,
  spansByKind,
  spansByOperation,
  tag,
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

  test('scenario 4 — fan-out: one consumer enqueues N siblings, all parented under it', async ({ request }) => {
    // Models the "dispatcher" pattern (process a batch of N items by enqueuing
    // one subtask per item). Pins the contract that the CONSUMER span stays
    // current across *multiple* inner async_task calls — a regression that
    // resets the OTel context between calls would surface as siblings parenting
    // under the previous sibling instead of the dispatcher.
    const trigger = unique('e2e-cascade-fanout');
    const FANOUT = 3;

    const enqueue = await enqueueTask(request, {
      task: 'fan_out',
      trigger_span: trigger,
      args: ['batch', FANOUT],
    });

    // HTTP + dispatcher (producer + consumer) + N × (producer + consumer)
    const expectedSpans = 1 + 2 + FANOUT * 2;
    const trace = await fetchTraceWhenReady(enqueue.trace_id, expectedSpans);

    const http = spanByOperation(trace, trigger);
    const dispatcherProducer = spanByOperation(trace, 'async_task/tasks_app.tasks.fan_out');
    const dispatcherConsumer = spanByOperation(trace, 'run/tasks_app.tasks.fan_out');

    // Dispatcher branch stays intact: HTTP → PRODUCER_dispatch → CONSUMER_dispatch.
    expect(parentOf(dispatcherProducer)).toBe(http.spanID);
    expect(parentOf(dispatcherConsumer)).toBe(dispatcherProducer.spanID);

    const siblingProducers = spansByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const siblingConsumers = spansByOperation(trace, 'run/tasks_app.tasks.noop');
    expect(siblingProducers).toHaveLength(FANOUT);
    expect(siblingConsumers).toHaveLength(FANOUT);

    // Every sibling producer parents under the dispatcher's CONSUMER — that's
    // the per-task context invariant. None of them should accidentally chain
    // under a previous sibling (which would look fine in a flame graph but is
    // structurally wrong).
    for (const producer of siblingProducers) {
      expect(parentOf(producer)).toBe(dispatcherConsumer.spanID);
    }
    // Each consumer parents under its own producer (same message id).
    const producerById = new Map<string, JaegerSpan>(
      siblingProducers.map(p => [String(tag(p, 'messaging.message.id')), p]),
    );
    for (const consumer of siblingConsumers) {
      const msgId = String(tag(consumer, 'messaging.message.id'));
      const matchingProducer = producerById.get(msgId);
      expect(matchingProducer, `no producer for sibling task ${msgId}`).toBeDefined();
      expect(parentOf(consumer)).toBe(matchingProducer!.spanID);
    }

    // Process attribution: dispatcher producer is on web (called from HTTP),
    // every sibling producer is on the worker (the dispatcher ran there).
    expect(serviceOf(trace, dispatcherProducer)).toBe('sample-web');
    expect(serviceOf(trace, dispatcherConsumer)).toBe('sample-worker');
    for (const producer of siblingProducers) {
      expect(serviceOf(trace, producer)).toBe('sample-worker');
    }
    for (const consumer of siblingConsumers) {
      expect(serviceOf(trace, consumer)).toBe('sample-worker');
    }

    // All siblings on one trace.
    for (const span of [...siblingProducers, ...siblingConsumers]) {
      expect(span.traceID).toBe(enqueue.trace_id);
    }
  });

  test('scenario 5 — mid-cascade failure: parent stays UNSET, failing child carries the error, surviving sibling is clean', async ({ request }) => {
    // The contract being pinned: a child's failure must not taint its parent's
    // status, and the surviving sibling must land on the same trace with a
    // clean status surface. A regression that propagated child errors up to
    // the parent would falsify dashboards built on `django_q2.state="error"`.
    const trigger = unique('e2e-cascade-failure');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_with_failure',
      trigger_span: trigger,
      args: ['payload'],
    });

    // HTTP + parent (producer + consumer) + noop (producer + consumer) + boom (producer + consumer) = 7
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);

    const http = spanByOperation(trace, trigger);
    const parentProducer = spanByOperation(trace, 'async_task/tasks_app.tasks.cascade_with_failure');
    const parentConsumer = spanByOperation(trace, 'run/tasks_app.tasks.cascade_with_failure');
    const siblingOkProducer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const siblingOkConsumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');
    const siblingErrProducer = spanByOperation(trace, 'async_task/tasks_app.tasks.boom');
    const siblingErrConsumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    // Tree shape: HTTP → parent → (noop + boom) under parent's CONSUMER.
    expect(parentOf(parentProducer)).toBe(http.spanID);
    expect(parentOf(parentConsumer)).toBe(parentProducer.spanID);
    expect(parentOf(siblingOkProducer)).toBe(parentConsumer.spanID);
    expect(parentOf(siblingOkConsumer)).toBe(siblingOkProducer.spanID);
    expect(parentOf(siblingErrProducer)).toBe(parentConsumer.spanID);
    expect(parentOf(siblingErrConsumer)).toBe(siblingErrProducer.spanID);

    // Parent's consumer stays CLEAN — the function returned successfully.
    // (django-q2 marks a task failed only when *its own* body raises; an
    // enqueued child failing doesn't touch the parent's task.)
    expect(tag(parentConsumer, 'otel.status_code')).toBeUndefined();
    expect(tag(parentConsumer, 'error')).toBeUndefined();
    expect(tag(parentConsumer, 'django_q2.state')).toBe('success');

    // Surviving sibling: same — UNSET status, state="success".
    expect(tag(siblingOkConsumer, 'otel.status_code')).toBeUndefined();
    expect(tag(siblingOkConsumer, 'error')).toBeUndefined();
    expect(tag(siblingOkConsumer, 'django_q2.state')).toBe('success');

    // Failing sibling: ERROR + state="error". The error is *isolated* to this
    // span — the other two consumers above prove it doesn't leak.
    expect(tag(siblingErrConsumer, 'otel.status_code')).toBe('ERROR');
    expect(tag(siblingErrConsumer, 'error')).toBe(true);
    expect(tag(siblingErrConsumer, 'django_q2.state')).toBe('error');

    // Producer spans never carry terminal state — they publish, they don't run.
    expect(tag(parentProducer, 'django_q2.state')).toBeUndefined();
    expect(tag(siblingOkProducer, 'django_q2.state')).toBeUndefined();
    expect(tag(siblingErrProducer, 'django_q2.state')).toBeUndefined();

    // Same trace across the whole shape.
    for (const span of trace.spans) {
      expect(span.traceID).toBe(enqueue.trace_id);
    }
  });

  test('scenario 6 — non-HTTP root: PRODUCER itself is the trace root, CONSUMER still parents under it', async ({ request }) => {
    // Models scheduled tasks, management commands, or any path that enqueues
    // outside an ambient span. The producer becomes a valid trace root and the
    // carrier still propagates to the consumer — same end-to-end story, just
    // with no HTTP edge to anchor the trace.
    const enqueue = await enqueueDetached(request, {
      task: 'noop',
      args: ['detached-root'],
    });

    // No trigger span ⇒ no HTTP root. Just PRODUCER + CONSUMER.
    // Look the trace up by `messaging.message.id = task_id` since we can't
    // search by operation name (concurrent tests could enqueue the same task).
    const trace = await findTraceByTag('sample-web', 'messaging.message.id', enqueue.task_id, {
      expectedSpans: 2,
    });

    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    // The whole point: producer has no parent — it IS the trace root.
    expect(parentOf(producer)).toBeNull();
    expect(parentOf(consumer)).toBe(producer.spanID);

    // Carrier still propagates across processes.
    expect(serviceOf(trace, producer)).toBe('sample-web');
    expect(serviceOf(trace, consumer)).toBe('sample-worker');
    expect(producer.traceID).toBe(consumer.traceID);

    // Span kinds + messaging attributes survive the detached path — guards against
    // a regression where "no parent" silently broke attribute population.
    expect(tag(producer, 'span.kind')).toBe('producer');
    expect(tag(consumer, 'span.kind')).toBe('consumer');
    expect(tag(producer, 'messaging.message.id')).toBe(enqueue.task_id);
    expect(tag(consumer, 'messaging.message.id')).toBe(enqueue.task_id);

    // Exactly the two spans we expected — no stray span from a leaked context.
    expect(trace.spans).toHaveLength(2);
  });
});
