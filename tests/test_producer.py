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
        # `messaging.message.conversation_id` (semconv) mirrors `django_q2.group` so
        # generic messaging dashboards (built for Celery's `correlation_id`) light up
        # for django-q2 traces too. We keep `django_q2.group` for backward compat.
        self.assertEqual(producer.attributes["messaging.message.conversation_id"], "reports")
        self.assertIn("messaging.message.id", producer.attributes)
        self.assertIn("django_q2.task.name", producer.attributes)

    def test_producer_span_uses_default_destination_when_no_cluster(self):
        async_task("tests.fixtures.noop", sync=True)

        producer = self._producer_span()
        self.assertEqual(producer.attributes["messaging.destination.name"], "default")

    def test_producer_span_omits_conversation_id_when_no_group(self):
        async_task("tests.fixtures.noop", sync=True)

        producer = self._producer_span()
        self.assertNotIn("messaging.message.conversation_id", producer.attributes)
        self.assertNotIn("django_q2.group", producer.attributes)

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


class ProducerExtraAttributeTests(TestBase):
    """
    The `django_q2.*` attribute pack.

    Set on the PRODUCER span when the corresponding task-dict field is present,
    skipped when it isn't.
    """

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self._tracer = self.tracer_provider.get_tracer("test-extra-attrs")

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _send_with_active_producer(self, task: dict) -> None:
        with self._tracer.start_as_current_span("fake-producer", kind=trace.SpanKind.PRODUCER):
            pre_enqueue.send(sender="django_q", task=task)

    def test_cached_attribute_set_when_truthy(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "cached": True})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertIs(span.attributes["django_q2.cached"], True)

    def test_sync_attribute_set_when_truthy(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "sync": True})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertIs(span.attributes["django_q2.sync"], True)

    def test_ack_failure_attribute_set_when_truthy(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "ack_failure": True})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertIs(span.attributes["django_q2.ack_failure"], True)

    def test_hook_attribute_set_when_string(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "hook": "tasks.callback"})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["django_q2.hook"], "tasks.callback")

    def test_hook_attribute_skipped_for_callable(self):
        # A bare function pointer has no stable string form — repr-ing it leaks
        # a memory address that's useless for grouping. Skip it.
        def some_hook():
            pass

        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "hook": some_hook})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertNotIn("django_q2.hook", span.attributes)

    def test_iter_count_attribute_set_when_positive_int(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "iter_count": 5})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["django_q2.iter_count"], 5)

    def test_iter_count_skipped_for_zero(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "iter_count": 0})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertNotIn("django_q2.iter_count", span.attributes)

    def test_chain_length_attribute_set_when_chain_list_present(self):
        self._send_with_active_producer(
            {"id": "x", "func": "f", "args": (), "kwargs": {}, "chain": ["task_b", "task_c"]},
        )
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["django_q2.chain_length"], 2)

    def test_chain_length_zero_when_chain_empty_list(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "chain": []})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertEqual(span.attributes["django_q2.chain_length"], 0)

    def test_chain_attribute_skipped_when_not_a_list(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}, "chain": None})
        span = self.memory_exporter.get_finished_spans()[0]
        self.assertNotIn("django_q2.chain_length", span.attributes)

    def test_all_optional_attributes_absent_when_task_minimal(self):
        self._send_with_active_producer({"id": "x", "func": "f", "args": (), "kwargs": {}})
        span = self.memory_exporter.get_finished_spans()[0]
        for key in (
            "django_q2.cached",
            "django_q2.sync",
            "django_q2.ack_failure",
            "django_q2.hook",
            "django_q2.iter_count",
            "django_q2.chain_length",
        ):
            self.assertNotIn(key, span.attributes)
