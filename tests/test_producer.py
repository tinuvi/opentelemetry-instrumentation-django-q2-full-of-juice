from __future__ import annotations

from django_q.signals import pre_enqueue
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import OTEL_CARRIER_KEY


class PreEnqueueProducerSpanTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-123",
            "name": "demo-task",
            "func": "tests.fixtures.noop",
            "args": (1, 2),
            "kwargs": {"k": "v"},
            "cluster": "test-cluster",
        }
        task.update(overrides)
        return task

    def test_pre_enqueue_creates_producer_span(self):
        task = self._build_task()

        pre_enqueue.send(sender="django_q", task=task)

        spans = self.memory_exporter.get_finished_spans()
        self.assertEqual(len(spans), 1)
        span = spans[0]
        self.assertEqual(span.name, "async_task/tests.fixtures.noop")
        self.assertEqual(span.kind, trace.SpanKind.PRODUCER)

    def test_pre_enqueue_sets_messaging_attributes(self):
        task = self._build_task(group="reports")

        pre_enqueue.send(sender="django_q", task=task)

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["messaging.message.id"], "task-id-123")
        self.assertEqual(span.attributes["messaging.system"], "django_q2")
        self.assertEqual(span.attributes["messaging.destination.name"], "test-cluster")
        self.assertEqual(span.attributes["messaging.operation"], "publish")
        self.assertEqual(span.attributes["django_q2.task.name"], "demo-task")
        self.assertEqual(span.attributes["django_q2.func"], "tests.fixtures.noop")
        self.assertEqual(span.attributes["django_q2.group"], "reports")

    def test_pre_enqueue_uses_default_destination_when_no_cluster(self):
        task = self._build_task()
        task.pop("cluster")

        pre_enqueue.send(sender="django_q", task=task)

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["messaging.destination.name"], "default")

    def test_pre_enqueue_injects_carrier_into_task_dict(self):
        task = self._build_task()

        pre_enqueue.send(sender="django_q", task=task)

        self.assertIn(OTEL_CARRIER_KEY, task)
        carrier = task[OTEL_CARRIER_KEY]
        self.assertIsInstance(carrier, dict)
        self.assertIn("traceparent", carrier)

    def test_carrier_traceparent_matches_emitted_producer_span(self):
        task = self._build_task()

        pre_enqueue.send(sender="django_q", task=task)

        span = self.memory_exporter.get_finished_spans()[0]
        carrier = task[OTEL_CARRIER_KEY]
        trace_id_hex = format(span.context.trace_id, "032x")
        span_id_hex = format(span.context.span_id, "016x")
        self.assertIn(trace_id_hex, carrier["traceparent"])
        self.assertIn(span_id_hex, carrier["traceparent"])

    def test_pre_enqueue_handles_callable_func(self):
        def my_callable():
            pass

        task = self._build_task(func=my_callable)

        pre_enqueue.send(sender="django_q", task=task)

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertIn("my_callable", span.name)
        self.assertIn("my_callable", span.attributes["django_q2.func"])

    def test_pre_enqueue_preserves_existing_carrier_dict(self):
        task = self._build_task()
        task[OTEL_CARRIER_KEY] = {"x-custom": "preexisting"}

        pre_enqueue.send(sender="django_q", task=task)

        carrier = task[OTEL_CARRIER_KEY]
        self.assertEqual(carrier["x-custom"], "preexisting")
        self.assertIn("traceparent", carrier)
