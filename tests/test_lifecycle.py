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
