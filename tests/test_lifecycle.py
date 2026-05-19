from __future__ import annotations

import logging

import django_q.tasks
from django.test import TestCase
from django_q.signals import (
    post_execute_in_worker,
    post_spawn,
    pre_enqueue,
    pre_execute,
)
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import (
    OTEL_CARRIER_KEY,
    attach_task_context,
    retrieve_task_context,
)


class PostSpawnTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def test_post_spawn_logs_proc_name_without_raising(self):
        with self.assertLogs("opentelemetry_instrumentation_django_q2", level=logging.DEBUG) as captured:
            post_spawn.send(sender="django_q", proc_name="qworker-1")
        self.assertTrue(any("qworker-1" in m for m in captured.output))

    def test_post_spawn_does_not_emit_any_span(self):
        post_spawn.send(sender="django_q", proc_name="qworker-1")
        self.assertEqual(self.memory_exporter.get_finished_spans(), ())

    def test_post_spawn_captures_proc_name_for_later_consumer_spans(self):
        from django_q.signals import post_execute_in_worker, pre_execute

        post_spawn.send(sender="django_q", proc_name="qworker-7")

        task = {
            "id": "task-worker-stamp",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: {},
        }
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.attributes["django_q2.worker"], "qworker-7")
        # semconv mirror — lets messaging dashboards filter by worker without
        # knowing django-q2's bespoke key.
        self.assertEqual(consumer.attributes["messaging.client.id"], "qworker-7")

    def test_consumer_span_has_no_worker_attribute_before_post_spawn(self):
        from django_q.signals import post_execute_in_worker, pre_execute

        task = {
            "id": "task-without-worker",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: {},
        }
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("django_q2.worker", consumer.attributes)
        self.assertNotIn("messaging.client.id", consumer.attributes)

    def test_post_spawn_does_not_stamp_producer_span(self):
        # The producer doesn't know which worker will pick the task up, so we
        # intentionally don't stamp django_q2.worker on producer spans.
        from django_q.tasks import async_task

        post_spawn.send(sender="django_q", proc_name="qworker-9")
        async_task("tests.fixtures.noop", sync=True)

        producer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.PRODUCER)
        self.assertNotIn("django_q2.worker", producer.attributes)
        self.assertNotIn("messaging.client.id", producer.attributes)

    def test_uninstrument_clears_captured_worker_name(self):
        post_spawn.send(sender="django_q", proc_name="qworker-leak")
        self.assertEqual(self.instrumentor._worker_name, "qworker-leak")

        self.instrumentor.uninstrument()
        # Re-instrument to keep tearDown's uninstrument idempotent.
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

        self.assertIsNone(self.instrumentor._worker_name)


class UninstrumentTests(TestBase, TestCase):
    def test_uninstrument_disconnects_pre_enqueue(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()

        task = {"id": "x", "name": "n", "func": "f", "args": (), "kwargs": {}}
        pre_enqueue.send(sender="django_q", task=task)

        self.assertEqual(self.memory_exporter.get_finished_spans(), ())
        self.assertNotIn(OTEL_CARRIER_KEY, task)

    def test_uninstrument_disconnects_pre_execute_and_post_execute(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()

        task = {"id": "x", "name": "n", "func": "f", "args": (), "kwargs": {}, "success": True}
        pre_execute.send(sender="django_q", func=None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        self.assertEqual(self.memory_exporter.get_finished_spans(), ())

    def test_uninstrument_clears_task_context_store(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        attach_task_context("leaked-id", span=None, activation=None, token=None)  # type: ignore[arg-type]

        instrumentor.uninstrument()

        self.assertIsNone(retrieve_task_context("leaked-id"))

    def test_uninstrument_unwraps_async_task(self):
        # async_task must be restored to its original implementation so that
        # subsequent calls don't keep producing spans through a stale wrapper.
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()

        django_q.tasks.async_task("tests.fixtures.noop", sync=True)

        producer_spans = [s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.PRODUCER]
        self.assertEqual(producer_spans, [])

    def test_reinstrumenting_after_uninstrument_works(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        try:
            django_q.tasks.async_task("tests.fixtures.noop", sync=True)
            producer_spans = [s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.PRODUCER]
            self.assertEqual(len(producer_spans), 1)
        finally:
            instrumentor.uninstrument()


class DoubleInstrumentTests(TestBase, TestCase):
    """
    `BaseInstrumentor.instrument()` is a no-op on a second call.

    If our `_instrument` ran twice, every signal handler would be wired twice
    (we connect with `weak=False`), and a single task would emit two PRODUCER
    spans and two CONSUMER spans. Pin the right behavior — mirror of Celery's
    `tests/test_duplicate.py`.
    """

    def test_calling_instrument_twice_does_not_double_fire_signals(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        try:
            django_q.tasks.async_task("tests.fixtures.add", 1, 2, sync=True)
        finally:
            instrumentor.uninstrument()

        spans = self.memory_exporter.get_finished_spans()
        producers = [s for s in spans if s.kind == trace.SpanKind.PRODUCER]
        consumers = [s for s in spans if s.kind == trace.SpanKind.CONSUMER]
        self.assertEqual(len(producers), 1, f"expected 1 producer, got {[s.name for s in producers]}")
        self.assertEqual(len(consumers), 1, f"expected 1 consumer, got {[s.name for s in consumers]}")

    def test_calling_uninstrument_twice_is_safe(self):
        instrumentor = DjangoQ2Instrumentor()
        instrumentor.instrument(tracer_provider=self.tracer_provider)
        instrumentor.uninstrument()
        instrumentor.uninstrument()
        django_q.tasks.async_task("tests.fixtures.noop", sync=True)

        producer_spans = [s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.PRODUCER]
        self.assertEqual(producer_spans, [])
