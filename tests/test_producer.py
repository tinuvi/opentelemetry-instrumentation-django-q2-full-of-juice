from __future__ import annotations

import time
from unittest import mock

import django_q.tasks
from django.test import TestCase
from django_q.signals import pre_enqueue
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import OTEL_CARRIER_KEY


def async_task(*args, **kwargs):
    # Resolve via the module attribute so each call goes through whatever
    # wrapper the instrumentor currently has installed on django_q.tasks.
    return django_q.tasks.async_task(*args, **kwargs)


class ProducerSpanLifecycleTests(TestBase, TestCase):
    """
    PRODUCER span lifecycle is owned by the async_task wrap.

    The wrap opens the span and the wrapped async_task returning closes it, so
    the span brackets broker.enqueue (or _sync in sync mode).
    """

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _producer_span(self):
        spans = [s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.PRODUCER]
        self.assertEqual(len(spans), 1, f"expected 1 PRODUCER span, got {len(spans)}: {[s.name for s in spans]}")
        return spans[0]

    def test_async_task_call_emits_a_single_producer_span(self):
        async_task("tests.fixtures.add", 1, 2, sync=True)

        producer = self._producer_span()
        self.assertEqual(producer.name, "async_task/tests.fixtures.add")
        self.assertEqual(producer.kind, trace.SpanKind.PRODUCER)

    def test_producer_span_has_messaging_attributes(self):
        async_task("tests.fixtures.add", 1, 2, sync=True, group="reports", cluster="custom-q")

        producer = self._producer_span()
        self.assertEqual(producer.attributes["messaging.system"], "django_q2")
        self.assertEqual(producer.attributes["messaging.operation.type"], "publish")
        self.assertEqual(producer.attributes["messaging.operation"], "publish")
        self.assertEqual(producer.attributes["messaging.destination.name"], "custom-q")
        self.assertEqual(producer.attributes["django_q2.func"], "tests.fixtures.add")
        self.assertEqual(producer.attributes["django_q2.group"], "reports")
        self.assertIn("messaging.message.id", producer.attributes)
        self.assertIn("django_q2.task.name", producer.attributes)

    def test_producer_span_uses_default_destination_when_no_cluster(self):
        async_task("tests.fixtures.noop", sync=True)

        producer = self._producer_span()
        self.assertEqual(producer.attributes["messaging.destination.name"], "default")

    def test_producer_span_duration_includes_wrapped_call(self):
        # Patch SignedPackage.dumps to add measurable latency — proves the PRODUCER
        # span actually brackets the inside of async_task, not just pre_enqueue.
        from django_q.signing import SignedPackage

        real_dumps = SignedPackage.dumps

        def slow_dumps(obj):
            time.sleep(0.05)
            return real_dumps(obj)

        with mock.patch.object(SignedPackage, "dumps", staticmethod(slow_dumps)):
            async_task("tests.fixtures.noop", sync=True)

        producer = self._producer_span()
        duration_s = (producer.end_time - producer.start_time) / 1e9
        self.assertGreaterEqual(duration_s, 0.05)

    def test_pre_enqueue_injects_carrier_into_task_dict(self):
        # Capture the live task dict — pre_enqueue mutates it before pickling.
        captured: dict = {}

        def grabber(sender, task, **_):
            captured.update(task)

        pre_enqueue.connect(grabber, weak=False)
        try:
            async_task("tests.fixtures.noop", sync=True)
        finally:
            pre_enqueue.disconnect(grabber)

        self.assertIn(OTEL_CARRIER_KEY, captured)
        self.assertIn("traceparent", captured[OTEL_CARRIER_KEY])

    def test_carrier_traceparent_matches_emitted_producer_span(self):
        captured: dict = {}

        def grabber(sender, task, **_):
            captured.update(task)

        pre_enqueue.connect(grabber, weak=False)
        try:
            async_task("tests.fixtures.noop", sync=True)
        finally:
            pre_enqueue.disconnect(grabber)

        producer = self._producer_span()
        trace_id_hex = format(producer.context.trace_id, "032x")
        span_id_hex = format(producer.context.span_id, "016x")
        traceparent = captured[OTEL_CARRIER_KEY]["traceparent"]
        self.assertIn(trace_id_hex, traceparent)
        self.assertIn(span_id_hex, traceparent)

    def test_async_task_with_callable_uses_dotted_name(self):
        from tests import fixtures

        async_task(fixtures.add, 1, 2, sync=True)

        producer = self._producer_span()
        self.assertIn("add", producer.name)
        self.assertIn("add", producer.attributes["django_q2.func"])


class PreEnqueueHandlerTests(TestBase):
    """
    Narrow tests for the pre_enqueue handler in isolation.

    It sets attributes on the currently-active PRODUCER span (opened by the wrap)
    and injects the carrier; we simulate the wrap by opening a span first.
    """

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self._tracer = self.tracer_provider.get_tracer("test-pre-enqueue")

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def test_handler_writes_message_id_to_current_span(self):
        task = {"id": "abc-123", "name": "n", "func": "f", "args": (), "kwargs": {}}
        with self._tracer.start_as_current_span("fake-producer", kind=trace.SpanKind.PRODUCER):
            pre_enqueue.send(sender="django_q", task=task)

        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["messaging.message.id"], "abc-123")

    def test_handler_injects_carrier_even_without_recording_span(self):
        # No surrounding span — handler should still inject without raising.
        task = {"id": "x", "name": "n", "func": "f", "args": (), "kwargs": {}}
        pre_enqueue.send(sender="django_q", task=task)
        self.assertIn(OTEL_CARRIER_KEY, task)

    def test_handler_preserves_existing_carrier_keys(self):
        task = {
            "id": "x",
            "name": "n",
            "func": "f",
            "args": (),
            "kwargs": {},
            OTEL_CARRIER_KEY: {"x-custom": "preexisting"},
        }
        with self._tracer.start_as_current_span("fake-producer", kind=trace.SpanKind.PRODUCER):
            pre_enqueue.send(sender="django_q", task=task)

        self.assertEqual(task[OTEL_CARRIER_KEY]["x-custom"], "preexisting")
        self.assertIn("traceparent", task[OTEL_CARRIER_KEY])
