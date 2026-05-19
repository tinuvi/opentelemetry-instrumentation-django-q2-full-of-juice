from __future__ import annotations

from unittest import mock

import django_q.tasks
from django.test import TestCase
from django_q.signals import post_execute_in_worker, pre_execute
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import OTEL_CARRIER_KEY

SCOPE = "opentelemetry_instrumentation_django_q2.instrumentor"
HISTOGRAM_NAME = "django_q2.task.duration"
PUBLISH_HISTOGRAM_NAME = "django_q2.publish.duration"


def _data_points(metric):
    return list(metric.data.data_points)


def _by_attr(points, key, value):
    return [p for p in points if p.attributes.get(key) == value]


class TaskDurationHistogramTests(TestBase, TestCase):
    """End-to-end metric flow exercised through django-q2's sync execution."""

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _read_histogram(self):
        metrics = self.get_sorted_metrics(SCOPE)
        matches = [m for m in metrics if m.name == HISTOGRAM_NAME]
        self.assertEqual(len(matches), 1, f"expected one {HISTOGRAM_NAME}, got names={[m.name for m in metrics]}")
        return matches[0]

    def test_successful_task_records_one_histogram_data_point(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)

        hist = self._read_histogram()
        self.assertEqual(hist.unit, "s")
        # Description is set so dashboards have a usable label out of the box.
        self.assertTrue(hist.description)

        points = _data_points(hist)
        success_points = _by_attr(points, "status", "success")
        self.assertEqual(
            len(success_points), 1, f"expected 1 success point, attrs={[dict(p.attributes) for p in points]}"
        )

        point = success_points[0]
        self.assertEqual(point.count, 1)
        # Even an in-process sync run takes >0 time — assert non-negative as a sanity check.
        self.assertGreaterEqual(point.sum, 0)
        self.assertEqual(point.attributes["django_q2.func"], "tests.fixtures.add")
        # When the caller doesn't pass `cluster=`, the destination resolves to the
        # configured Q_CLUSTER name — same value the consumer side sees once
        # django-q2's pusher stamps `task["cluster"]` server-side.
        self.assertEqual(point.attributes["messaging.destination.name"], "test-cluster")

    def test_failed_task_records_status_error(self):
        # Async-mode failure: worker.py sets task["success"]=False *before* firing
        # post_execute_in_worker, so the handler can label the histogram error.
        # Sync mode fires post_execute first and re-raises, so we use direct
        # signals here to mirror the production (async-worker) shape.
        task = {
            "id": "metrics-failure-id",
            "name": "demo",
            "func": "tests.fixtures.boom",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: {},
        }
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = False
        task["result"] = 'RuntimeError("boom!") : Traceback (most recent call last):\nRuntimeError: boom!'
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        hist = self._read_histogram()
        error_points = _by_attr(_data_points(hist), "status", "error")
        self.assertEqual(len(error_points), 1)
        self.assertEqual(error_points[0].attributes["django_q2.func"], "tests.fixtures.boom")

    def test_histogram_buckets_each_task_separately(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)
        django_q.tasks.async_task("tests.fixtures.add", 3, 4, sync=True)
        django_q.tasks.async_task("tests.fixtures.noop", sync=True)

        hist = self._read_histogram()
        points = _data_points(hist)
        add_points = [p for p in points if p.attributes.get("django_q2.func") == "tests.fixtures.add"]
        noop_points = [p for p in points if p.attributes.get("django_q2.func") == "tests.fixtures.noop"]
        self.assertEqual(sum(p.count for p in add_points), 2)
        self.assertEqual(sum(p.count for p in noop_points), 1)

    def test_destination_attribute_reflects_cluster(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True, cluster="custom-cluster")

        hist = self._read_histogram()
        custom_points = _by_attr(_data_points(hist), "messaging.destination.name", "custom-cluster")
        self.assertEqual(sum(p.count for p in custom_points), 1)

    def test_uninstrument_stops_recording(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)
        before = sum(p.count for p in _data_points(self._read_histogram()))
        self.assertEqual(before, 1)

        self.instrumentor.uninstrument()
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)

        # The histogram instrument still exists but receives no new recordings — count is unchanged.
        after = sum(p.count for p in _data_points(self._read_histogram()))
        self.assertEqual(after, before)


class PublishDurationHistogramTests(TestBase, TestCase):
    """
    Producer-side histogram.

    Mirrors the consumer-side `django_q2.task.duration`: same unit / status
    cardinality, but timed from the async_task wrap so it reflects broker.enqueue
    + signing roundtrip (async mode) or the inline run (sync mode).
    """

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _read_publish_histogram(self):
        metrics = self.get_sorted_metrics(SCOPE)
        matches = [m for m in metrics if m.name == PUBLISH_HISTOGRAM_NAME]
        self.assertEqual(
            len(matches),
            1,
            f"expected one {PUBLISH_HISTOGRAM_NAME}, got names={[m.name for m in metrics]}",
        )
        return matches[0]

    def test_successful_publish_records_one_sample_with_status_success(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)

        hist = self._read_publish_histogram()
        self.assertEqual(hist.unit, "s")
        self.assertTrue(hist.description)

        points = _data_points(hist)
        success_points = _by_attr(points, "status", "success")
        self.assertEqual(
            len(success_points), 1, f"expected 1 success point, attrs={[dict(p.attributes) for p in points]}"
        )

        point = success_points[0]
        self.assertEqual(point.count, 1)
        self.assertGreaterEqual(point.sum, 0)
        self.assertEqual(point.attributes["django_q2.func"], "tests.fixtures.add")
        # Producer publishes resolve to the configured cluster the same way the
        # consumer-side histogram does — operators get a consistent label across
        # both metrics without needing the caller to pass `cluster=`.
        self.assertEqual(point.attributes["messaging.destination.name"], "test-cluster")

    def test_failed_publish_records_status_error_and_reraises(self):
        # Patch SignedPackage.dumps to blow up — exercises the "broker.enqueue
        # path raised before completing" branch so the histogram must tag the
        # failure as status=error without swallowing the exception.
        from django_q.signing import SignedPackage

        def boom(_obj):
            raise RuntimeError("signing broke")

        with mock.patch.object(SignedPackage, "dumps", staticmethod(boom)):
            with self.assertRaises(RuntimeError):
                django_q.tasks.async_task("tests.fixtures.add", 1, 2)

        hist = self._read_publish_histogram()
        error_points = _by_attr(_data_points(hist), "status", "error")
        self.assertEqual(len(error_points), 1)
        self.assertEqual(error_points[0].attributes["django_q2.func"], "tests.fixtures.add")

    def test_destination_label_reflects_cluster_kwarg(self):
        django_q.tasks.async_task("tests.fixtures.noop", sync=True, cluster="custom-cluster")

        hist = self._read_publish_histogram()
        custom_points = _by_attr(_data_points(hist), "messaging.destination.name", "custom-cluster")
        self.assertEqual(sum(p.count for p in custom_points), 1)

    def test_destination_label_falls_back_to_configured_cluster_name(self):
        # When the caller doesn't pass `cluster=`, both histograms (publish and
        # task.duration) should land under the configured Q_CLUSTER name — that's
        # what django-q2's pusher.py stamps server-side, so the producer and the
        # consumer agree on the destination. The testapp settings use
        # Q_CLUSTER["name"] = "django-q2-otel-tests".
        from django_q.conf import Conf

        configured_name = Conf.CLUSTER_NAME

        django_q.tasks.async_task("tests.fixtures.noop", sync=True)

        hist = self._read_publish_histogram()
        resolved_points = _by_attr(_data_points(hist), "messaging.destination.name", configured_name)
        self.assertEqual(
            sum(p.count for p in resolved_points),
            1,
            f"expected 1 sample under {configured_name!r}, got attrs={[dict(p.attributes) for p in _data_points(hist)]}",
        )

    def test_histogram_buckets_each_func_separately(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)
        django_q.tasks.async_task("tests.fixtures.noop", sync=True)
        django_q.tasks.async_task("tests.fixtures.noop", sync=True)

        hist = self._read_publish_histogram()
        points = _data_points(hist)
        add_count = sum(p.count for p in points if p.attributes.get("django_q2.func") == "tests.fixtures.add")
        noop_count = sum(p.count for p in points if p.attributes.get("django_q2.func") == "tests.fixtures.noop")
        self.assertEqual(add_count, 1)
        self.assertEqual(noop_count, 2)

    def test_uninstrument_stops_recording_publishes(self):
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)
        before = sum(p.count for p in _data_points(self._read_publish_histogram()))
        self.assertEqual(before, 1)

        self.instrumentor.uninstrument()
        django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)

        after = sum(p.count for p in _data_points(self._read_publish_histogram()))
        self.assertEqual(after, before)


class TaskDurationStartTimeHousekeepingTests(TestBase):
    """Guards the start-time bookkeeping so dropped tasks don't leak memory."""

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
        )

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-housekeeping",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: {},
        }
        task.update(overrides)
        return task

    def test_post_execute_pops_start_time(self):
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        self.assertIn(task["id"], self.instrumentor._task_start_times)

        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})
        self.assertNotIn(task["id"], self.instrumentor._task_start_times)

    def test_post_execute_without_prior_pre_execute_does_not_record(self):
        task = self._build_task(id="orphan-id", success=True)
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        metrics = self.get_sorted_metrics(SCOPE)
        # No histogram should be recorded because there was no matching pre_execute.
        for metric in metrics:
            if metric.name == HISTOGRAM_NAME:
                # Either no data point, or zero count for orphan.
                self.assertEqual(sum(p.count for p in _data_points(metric)), 0)

    def test_uninstrument_clears_start_times(self):
        task = self._build_task(id="leftover-id")
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        self.assertIn("leftover-id", self.instrumentor._task_start_times)

        self.instrumentor.uninstrument()

        self.assertNotIn("leftover-id", self.instrumentor._task_start_times)
