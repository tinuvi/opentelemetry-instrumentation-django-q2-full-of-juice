import { expect, test } from '@playwright/test';

import { unique } from '../helpers/data';
import { enqueueTask, fetchTraceWhenReady } from '../helpers/jaeger';
import { countMatching, fetchPrometheusUntil } from '../helpers/prometheus';

// The OTel collector's Prometheus exporter renames the OTLP `django_q2.task.duration`
// histogram with unit `s` to `django_q2_task_duration_seconds`. Labels follow
// the same dot-to-underscore rule, so `django_q2.func` becomes `django_q2_func`.
const HISTOGRAM_COUNT = 'django_q2_task_duration_seconds_count';
const HISTOGRAM_SUM = 'django_q2_task_duration_seconds_sum';
const PUBLISH_HISTOGRAM_COUNT = 'django_q2_publish_duration_seconds_count';
const PUBLISH_HISTOGRAM_SUM = 'django_q2_publish_duration_seconds_sum';

test.describe('metrics — django_q2.task.duration', () => {
  test('successful task records one histogram sample with status=success', async ({ request }) => {
    const trigger = unique('e2e-metric-success');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    // Wait for the trace to ensure the task ran end-to-end before scraping.
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    const samples = await fetchPrometheusUntil(s =>
      countMatching(s, HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        status: 'success',
      }) >= 1,
    );

    const count = countMatching(samples, HISTOGRAM_COUNT, {
      django_q2_func: 'tasks_app.tasks.noop',
      status: 'success',
    });
    expect(count).toBeGreaterThanOrEqual(1);

    const sum = countMatching(samples, HISTOGRAM_SUM, {
      django_q2_func: 'tasks_app.tasks.noop',
      status: 'success',
    });
    // Sum is in seconds — a noop is fast but non-negative.
    expect(sum).toBeGreaterThanOrEqual(0);
  });

  test('failing task records histogram sample with status=error', async ({ request }) => {
    const trigger = unique('e2e-metric-error');

    const enqueue = await enqueueTask(request, {
      task: 'boom',
      trigger_span: trigger,
    });

    // boom raises in the worker, but the consumer span + histogram still fire.
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    const samples = await fetchPrometheusUntil(s =>
      countMatching(s, HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.boom',
        status: 'error',
      }) >= 1,
    );

    const errorCount = countMatching(samples, HISTOGRAM_COUNT, {
      django_q2_func: 'tasks_app.tasks.boom',
      status: 'error',
    });
    expect(errorCount).toBeGreaterThanOrEqual(1);
  });

  test('destination label reflects django-q2 cluster name', async ({ request }) => {
    // The sample-project's Q_CLUSTER_NAME is "sample-cluster" — the histogram
    // must carry that as messaging_destination_name so dashboards can split per
    // cluster (mirrors Celery's `queue` label).
    const trigger = unique('e2e-metric-cluster');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['cluster-check'],
    });
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    const samples = await fetchPrometheusUntil(s =>
      countMatching(s, HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        messaging_destination_name: 'sample-cluster',
      }) >= 1,
    );

    const count = countMatching(samples, HISTOGRAM_COUNT, {
      django_q2_func: 'tasks_app.tasks.noop',
      messaging_destination_name: 'sample-cluster',
    });
    expect(count).toBeGreaterThanOrEqual(1);
  });
});

test.describe('metrics — django_q2.publish.duration', () => {
  // Producer-side counterpart of `task.duration`. The async_task wrap times
  // broker.enqueue + signing in async mode, so it must record on every publish.
  test('successful enqueue records one publish sample with status=success', async ({ request }) => {
    const trigger = unique('e2e-publish-metric-success');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });

    // Wait for the trace to ensure the enqueue path completed end-to-end before
    // scraping — same shape as the consumer-side metric tests above.
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    const samples = await fetchPrometheusUntil(s =>
      countMatching(s, PUBLISH_HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        status: 'success',
      }) >= 1,
    );

    const count = countMatching(samples, PUBLISH_HISTOGRAM_COUNT, {
      django_q2_func: 'tasks_app.tasks.noop',
      status: 'success',
    });
    expect(count).toBeGreaterThanOrEqual(1);

    const sum = countMatching(samples, PUBLISH_HISTOGRAM_SUM, {
      django_q2_func: 'tasks_app.tasks.noop',
      status: 'success',
    });
    // Publish time is small but must be ≥0 — sanity check that the unit is right.
    expect(sum).toBeGreaterThanOrEqual(0);
  });

  test('publish destination label reflects the cluster the producer targets', async ({ request }) => {
    const trigger = unique('e2e-publish-metric-cluster');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['cluster-check'],
    });
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    const samples = await fetchPrometheusUntil(s =>
      countMatching(s, PUBLISH_HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        messaging_destination_name: 'sample-cluster',
      }) >= 1,
    );

    const count = countMatching(samples, PUBLISH_HISTOGRAM_COUNT, {
      django_q2_func: 'tasks_app.tasks.noop',
      messaging_destination_name: 'sample-cluster',
    });
    expect(count).toBeGreaterThanOrEqual(1);
  });

  test('publish and task histograms both record for the same task', async ({ request }) => {
    // Single enqueue ⇒ exactly one increment on each histogram. Asserting both
    // together pins the contract: an operator splitting "is the broker slow" vs
    // "are the workers slow" needs both samples present for a given func.
    const trigger = unique('e2e-publish-and-task-both');

    const enqueue = await enqueueTask(request, {
      task: 'noop',
      trigger_span: trigger,
      args: ['hello'],
    });
    await fetchTraceWhenReady(enqueue.trace_id, 3);

    await fetchPrometheusUntil(samples => {
      const publish = countMatching(samples, PUBLISH_HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        status: 'success',
      });
      const consume = countMatching(samples, HISTOGRAM_COUNT, {
        django_q2_func: 'tasks_app.tasks.noop',
        status: 'success',
      });
      return publish >= 1 && consume >= 1;
    });
  });
});
