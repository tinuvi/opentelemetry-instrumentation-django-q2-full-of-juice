import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import {
  enqueueTask,
  fetchTraceWhenReady,
  spansByOperation,
  tag,
} from '../helpers/jaeger';

// What this proves end-to-end on `django-q2-full-of-juice`
// =========================================================
// The juice fork's pusher stamps `task["attempt"]` (1 on first delivery,
// N >= 2 on re-deliveries) before `pre_execute` fires. The instrumentor
// reads that key in `_apply_task_attributes` and stamps `django_q2.attempt`
// on the consumer span. Operators can then filter retries on
// `django_q2.attempt > 1` in dashboards without a DB join.
//
// To prove the field tracks re-deliveries we force one: enqueue a task that
// always raises and let django-q2's default failure semantics do the work.
// The monitor only calls `broker.acknowledge` if `task["success"]` OR
// `task.get("ack_failure", False)` — so default-False on failure means the
// broker doesn't ack, the lock expires, and the same task is re-popped after
// `Q_CLUSTER["retry"]` seconds. The juice service is configured with
// retry=5 and max_attempts=2 (see docker-compose.yml) so the test wraps
// quickly and the monitor force-acks once `attempt_count >= MAX_ATTEMPTS`.
//
// Both deliveries land on the SAME trace because `_on_post_execute_in_worker`
// re-injects `task["otel_carrier"]` with the previous CONSUMER's traceparent
// before the broker re-pops — the next delivery's CONSUMER parents under
// the prior CONSUMER. That's the same continuity mechanism that powers
// async_chain across links.
//
// Upstream `django-q2` doesn't stamp `task["attempt"]` — the existing
// upstream specs (no `django_q2.attempt` assertion anywhere) implicitly
// pin "field absent on upstream", which is the cleanest signal we can give.

test.describe('juice fork — task["attempt"] surfaces on consumer spans across re-deliveries', () => {
  // Long timeout: the broker pops at retry-interval boundaries (5s), so
  // attempt 2 lands ~5-10s after attempt 1. Add buffer for Jaeger ingest.
  test.setTimeout(60_000);

  test('re-delivered failing task lands two consumer spans with attempt=1 and attempt=2', async ({ request }) => {
    const trigger = unique('e2e-juice-retry');

    const enqueue = await enqueueTask(request, {
      task: 'retry_boom',
      trigger_span: trigger,
      // Default `ack_failure=False`: the monitor will NOT acknowledge the
      // task on failure, the broker lock expires, and the broker re-pops the
      // same task — that's what surfaces the second `task["attempt"]` value
      // on the consumer span. Don't pass `q_options: { ack_failure: true }`
      // here: that semantic is "ack even on failure" and would suppress the
      // retry the test exists to prove.
    });

    // HTTP root + PRODUCER + CONSUMER_1 + CONSUMER_2 = 4 spans on one trace.
    // `fetchTraceWhenReady` polls until the second consumer arrives, then
    // returns the full trace. Generous timeout to absorb the broker's
    // retry-interval (5s) and Jaeger's ingest delay.
    const trace = await fetchTraceWhenReady(enqueue.trace_id, 4, { timeoutMs: 45_000 });

    const consumers = spansByOperation(trace, 'run/tasks_app.tasks.retry_boom');
    // Two attempts ⇒ two consumer spans. Asserting exactly 2 catches a
    // regression where `max_attempts` stops bounding retries (would land 3+)
    // or where the pusher silently double-stamps.
    expect(consumers).toHaveLength(2);

    // Sort chronologically — the broker may emit deliveries on either worker
    // process, so spanID order isn't deterministic.
    consumers.sort((a, b) => a.startTime - b.startTime);
    const [first, second] = consumers;

    expect(tag(first, 'django_q2.attempt')).toBe(1);
    expect(tag(second, 'django_q2.attempt')).toBe(2);

    // Both failures are real — task["success"] = False on each, so each
    // consumer carries `django_q2.state=error`. Catches a regression where
    // the state stops landing under re-delivery.
    expect(tag(first, 'django_q2.state')).toBe('error');
    expect(tag(second, 'django_q2.state')).toBe('error');

    // And both spans agree on `messaging.message.id` — the load-bearing
    // operator-side correlation between deliveries of the same task.
    expect(tag(first, 'messaging.message.id')).toBe(enqueue.task_id);
    expect(tag(second, 'messaging.message.id')).toBe(enqueue.task_id);
  });
});
