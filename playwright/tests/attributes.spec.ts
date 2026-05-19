import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  ChainEntry,
  enqueueChain,
  enqueueIter,
  enqueueTask,
  fetchTraceWhenReady,
  spanByOperation,
  spansByKind,
  tag,
} from '../helpers/jaeger';

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

// `django_q2.broker.type` is resolved once at `_instrument()` time from
// django-q2's broker-selection precedence (Conf.BROKER_CLASS → IRON_MQ → SQS →
// ORM → MONGO → redis default) and stamped on every span. The sample-project
// configures `"orm": "default"`, so the expected value is "orm" across the
// suite. Deliberately NOT a metric label — see the instrumentor comments for
// the rationale (single-broker fleets would carry a constant column).
test.describe('django_q2.broker.type — broker backend identification', () => {
  test('orm backend appears on both PRODUCER and CONSUMER for a single task', async ({ request }) => {
    const trigger = unique('e2e-broker-type-single');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.broker.type')).toBe('orm');
    expect(tag(consumer, 'django_q2.broker.type')).toBe('orm');
  });

  test('broker.type appears on every span across a 3-level cascade', async ({ request }) => {
    // Confirms the attribute is stamped on cascade-spawned producers (which run
    // in the worker process), not only on the web-side producer. A regression
    // here would mean dashboards splitting `task.duration` by broker would
    // silently miss cascaded tasks.
    const trigger = unique('e2e-broker-type-cascade');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_three',
      trigger_span: trigger,
      args: ['hello'],
    });

    // HTTP root + (producer + consumer) × 3 = 7 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);
    const messagingSpans = [
      ...spansByKind(trace, 'producer'),
      ...spansByKind(trace, 'consumer'),
    ];
    expect(messagingSpans).toHaveLength(6);

    for (const span of messagingSpans) {
      expect(tag(span, 'django_q2.broker.type'), `missing broker.type on ${span.operationName}`).toBe('orm');
    }
  });

  test('broker.type appears on every span of an async_iter fan-out', async ({ request }) => {
    const trigger = unique('e2e-broker-type-iter');
    const batch = [['iter-a'], ['iter-b'], ['iter-c']];

    const enqueue = await enqueueIter(request, {
      task: 'noop',
      trigger_span: trigger,
      args_iter: batch,
    });

    // HTTP root + 3 producers + 3 consumers = 7 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);
    const messagingSpans = [
      ...spansByKind(trace, 'producer'),
      ...spansByKind(trace, 'consumer'),
    ];
    expect(messagingSpans).toHaveLength(6);

    for (const span of messagingSpans) {
      expect(tag(span, 'django_q2.broker.type'), `missing broker.type on ${span.operationName}`).toBe('orm');
    }
  });

  test('broker.type appears on the first chain step', async ({ request }) => {
    // chain.spec.ts already documents that only the first link sits under the
    // HTTP root; subsequent links land in fresh traces. We only assert the
    // first link here — by parity with how chain_length is tested.
    const trigger = unique('e2e-broker-type-chain');
    const chain: ChainEntry[] = [
      { task: 'noop', args: ['link-a'] },
      { task: 'noop', args: ['link-b'] },
    ];

    const enqueue = await enqueueChain(request, { trigger_span: trigger, chain });
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.broker.type')).toBe('orm');
    expect(tag(consumer, 'django_q2.broker.type')).toBe('orm');
  });

  test('broker.type appears on a failing task too', async ({ request }) => {
    // Sanity-check the error path: the attribute is stamped at span-open time
    // (in `_set_messaging_basics`), so it must be present regardless of the
    // task's terminal outcome. Dashboards that filter "broker=orm AND status=error"
    // depend on this.
    const trigger = unique('e2e-broker-type-error');

    const enqueue = await enqueueTask(request, { task: 'boom', trigger_span: trigger });
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);

    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.boom');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.boom');

    expect(tag(producer, 'django_q2.broker.type')).toBe('orm');
    expect(tag(consumer, 'django_q2.broker.type')).toBe('orm');
  });
});

// `django_q2.timeout` is the per-task budget the Sentinel will enforce — a
// positive integer number of seconds. Producer side: only when the caller
// passed `timeout=`. Consumer side: caller value if present, otherwise
// `Conf.TIMEOUT` from the worker's Q_CLUSTER settings (the sample uses
// `timeout: 60`). When neither source has a positive value, the attribute is
// absent — that asymmetry is intentional so dashboards can express
// "duration / timeout" ratios without a NULL-handling clause.
test.describe('django_q2.timeout — per-task budget', () => {
  test('Conf.TIMEOUT fallback (60s) appears on CONSUMER but NOT on PRODUCER when caller did not pass timeout=', async ({ request }) => {
    const trigger = unique('e2e-timeout-conf-fallback');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    // Asymmetry contract: the producer can't see worker config; it has nothing
    // to stamp. The consumer reads Conf.TIMEOUT and stamps the worker's budget.
    expect(tag(producer, 'django_q2.timeout')).toBeUndefined();
    expect(tag(consumer, 'django_q2.timeout')).toBe(60);
  });

  test('caller-passed timeout=120 appears on BOTH spans, overriding the Conf fallback', async ({ request }) => {
    // When the caller explicitly passes `timeout=N`, that's THIS task's budget.
    // The Sentinel will enforce N, not Conf.TIMEOUT — so both spans should
    // surface N. Verifies the producer-side stamp lands, the consumer-side
    // task-dict read wins over the Conf fallback.
    const trigger = unique('e2e-timeout-explicit');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { timeout: 120 },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.timeout')).toBe(120);
    expect(tag(consumer, 'django_q2.timeout')).toBe(120);
  });

  test('every consumer in a cascade inherits the Conf.TIMEOUT fallback', async ({ request }) => {
    // Cascade-spawned tasks (running in the worker) also enjoy the Conf fallback
    // because `_on_pre_execute` calls `_read_configured_timeout()` per task.
    // Without this, dashboards alerting on "duration/timeout headroom" would
    // miss cascaded tasks entirely.
    const trigger = unique('e2e-timeout-cascade');

    const enqueue = await enqueueTask(request, {
      task: 'cascade_three',
      trigger_span: trigger,
      args: ['hello'],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);
    const consumers = spansByKind(trace, 'consumer');
    expect(consumers).toHaveLength(3);

    for (const consumer of consumers) {
      expect(tag(consumer, 'django_q2.timeout'), `missing timeout on ${consumer.operationName}`).toBe(60);
    }
  });

  test('Conf.TIMEOUT fallback applies to every consumer in an async_iter fan-out', async ({ request }) => {
    // Each iter-spawned consumer reads Conf.TIMEOUT independently via
    // `_on_pre_execute`. A regression where the fallback wasn't applied per task
    // would leave fan-out consumers without a budget, breaking dashboards that
    // alert on "duration/timeout headroom" for batched workloads.
    const trigger = unique('e2e-timeout-iter-fallback');

    const enqueue = await enqueueIter(request, {
      task: 'noop',
      trigger_span: trigger,
      args_iter: [['a'], ['b']],
    });

    // HTTP + 2 producers + 2 consumers = 5 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 5);
    const consumers = spansByKind(trace, 'consumer');
    expect(consumers).toHaveLength(2);

    for (const consumer of consumers) {
      expect(tag(consumer, 'django_q2.timeout'), `missing timeout on ${consumer.operationName}`).toBe(60);
    }
  });

  test('timeout=0 falls back to Conf.TIMEOUT on consumer (zero is not a real budget)', async ({ request }) => {
    // Stamping `timeout=0` on the span would mislead "duration ≥ timeout"
    // alerting (any non-instant task would look over-budget). When the task
    // dict carries 0, the consumer side rejects it and falls back to Conf.
    const trigger = unique('e2e-timeout-zero');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
      kwargs: { timeout: 0 },
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.noop');

    expect(tag(producer, 'django_q2.timeout')).toBeUndefined();
    // Conf.TIMEOUT=60 wins on the consumer side because task["timeout"]=0 is
    // rejected by the positive-int gate.
    expect(tag(consumer, 'django_q2.timeout')).toBe(60);
  });
});
