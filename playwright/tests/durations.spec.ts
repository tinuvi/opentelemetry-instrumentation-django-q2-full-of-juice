import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  enqueueTask,
  fetchTraceWhenReady,
  parentOf,
  serviceOf,
  spanByOperation,
} from '../helpers/jaeger';

const SLEEP_SECONDS = 0.4;
// Jaeger duration unit is microseconds. The CONSUMER span brackets the user
// function call, so it must be at least the sleep — minus a small slack to
// absorb sub-ms clock skew between the sleep boundary and span end.
const SLEEP_MICROS_FLOOR = Math.floor(SLEEP_SECONDS * 1_000_000 * 0.9);

test.describe('span durations reflect real work', () => {
  test('single slow task — CONSUMER duration covers the sleep', async ({ request }) => {
    const trigger = unique('e2e-slow-noop');

    const enqueue = await enqueueTask(request, {
      task: 'slow_noop',
      trigger_span: trigger,
      args: ['hello', SLEEP_SECONDS],
    });

    const trace = await fetchTraceWhenReady(enqueue.trace_id, 3);
    const producer = spanByOperation(trace, 'async_task/tasks_app.tasks.slow_noop');
    const consumer = spanByOperation(trace, 'run/tasks_app.tasks.slow_noop');

    expect(consumer.duration).toBeGreaterThanOrEqual(SLEEP_MICROS_FLOOR);
    // The producer span runs in the web process and ends after broker.enqueue
    // returns — so it must be much shorter than the worker-side sleep.
    expect(producer.duration).toBeLessThan(consumer.duration);
    // And producer must be >0 (gap #2: real broker-publish duration).
    expect(producer.duration).toBeGreaterThan(100);
  });

  test('slow cascade — each layer carries its own sleep duration', async ({ request }) => {
    const trigger = unique('e2e-slow-cascade');

    const enqueue = await enqueueTask(request, {
      task: 'slow_cascade_three',
      trigger_span: trigger,
      args: ['deep', SLEEP_SECONDS],
    });

    // 1 HTTP + 3 producers + 3 consumers = 7 spans.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 7);

    const cc3 = spanByOperation(trace, 'run/tasks_app.tasks.slow_cascade_three');
    const cc2 = spanByOperation(trace, 'run/tasks_app.tasks.slow_cascade_two');
    const cleaf = spanByOperation(trace, 'run/tasks_app.tasks.slow_noop');

    // Each consumer sleeps SLEEP_SECONDS, so each must clear the floor. We don't
    // assert duration ordering between layers: async_task is fire-and-forget, so
    // the outer task returns as soon as its enqueue completes — every layer is
    // dominated by its own sleep, and inter-layer overhead is sub-ms noise.
    for (const consumer of [cc3, cc2, cleaf]) {
      expect(consumer.duration).toBeGreaterThanOrEqual(SLEEP_MICROS_FLOOR);
    }

    // The cascade producers are recorded by the worker (they're spawned from
    // inside a running task). Confirm that's still the case with the wrap.
    const pc2 = spanByOperation(trace, 'async_task/tasks_app.tasks.slow_cascade_two');
    const pleaf = spanByOperation(trace, 'async_task/tasks_app.tasks.slow_noop');
    expect(serviceOf(trace, pc2)).toBe('sample-worker');
    expect(serviceOf(trace, pleaf)).toBe('sample-worker');

    // Inner-layer producers must report >0 duration too — proves the wrap stayed
    // installed across the worker fork and the in-task async_task call.
    expect(pc2.duration).toBeGreaterThan(100);
    expect(pleaf.duration).toBeGreaterThan(100);

    // And the chain shape still holds.
    expect(parentOf(pc2)).toBe(cc3.spanID);
    expect(parentOf(pleaf)).toBe(cc2.spanID);
  });
});
