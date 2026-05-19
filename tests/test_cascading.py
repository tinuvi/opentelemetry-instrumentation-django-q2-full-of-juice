from __future__ import annotations

from django.test import TestCase
from django_q.tasks import async_task
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor


class CascadingPropagationTests(TestBase, TestCase):
    """End-to-end: drives real async_task(sync=True) and asserts span tree shape."""

    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)
        self._tracer = self.tracer_provider.get_tracer(__name__)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _spans_by_name_kind(self):
        return {(s.name, s.kind): s for s in self.memory_exporter.get_finished_spans()}

    def test_scenario_1_http_to_producer_to_consumer(self):
        with self._tracer.start_as_current_span("HTTP GET /enqueue") as http_span:
            async_task("tests.fixtures.add", 1, 2, sync=True)

        spans = self._spans_by_name_kind()
        producer = spans[("async_task/tests.fixtures.add", trace.SpanKind.PRODUCER)]
        consumer = spans[("run/tests.fixtures.add", trace.SpanKind.CONSUMER)]

        self.assertEqual(producer.parent.span_id, http_span.get_span_context().span_id)
        self.assertEqual(consumer.parent.span_id, producer.context.span_id)
        self.assertEqual(consumer.parent.trace_id, http_span.get_span_context().trace_id)

    def test_scenario_2_http_to_task_a_to_task_b(self):
        with self._tracer.start_as_current_span("HTTP POST /trigger") as http_span:
            async_task("tests.fixtures.cascade_two", "hello", sync=True)

        spans = self._spans_by_name_kind()
        producer_a = spans[("async_task/tests.fixtures.cascade_two", trace.SpanKind.PRODUCER)]
        consumer_a = spans[("run/tests.fixtures.cascade_two", trace.SpanKind.CONSUMER)]
        producer_b = spans[("async_task/tests.fixtures.noop", trace.SpanKind.PRODUCER)]
        consumer_b = spans[("run/tests.fixtures.noop", trace.SpanKind.CONSUMER)]

        # HTTP -> PRODUCER_A -> CONSUMER_A -> PRODUCER_B -> CONSUMER_B
        self.assertEqual(producer_a.parent.span_id, http_span.get_span_context().span_id)
        self.assertEqual(consumer_a.parent.span_id, producer_a.context.span_id)
        self.assertEqual(producer_b.parent.span_id, consumer_a.context.span_id)
        self.assertEqual(consumer_b.parent.span_id, producer_b.context.span_id)
        # All on the same trace.
        trace_id = http_span.get_span_context().trace_id
        for span in (producer_a, consumer_a, producer_b, consumer_b):
            self.assertEqual(span.context.trace_id, trace_id)

    def test_scenario_3_three_level_cascade_stays_on_one_trace(self):
        with self._tracer.start_as_current_span("HTTP POST /deep") as http_span:
            async_task("tests.fixtures.cascade_three", "deep", sync=True)

        spans = self.memory_exporter.get_finished_spans()
        # 1 HTTP + 3 producers + 3 consumers = 7 spans
        self.assertEqual(len(spans), 7)
        trace_id = http_span.get_span_context().trace_id
        for span in spans:
            self.assertEqual(span.context.trace_id, trace_id)

        spans_by_key = self._spans_by_name_kind()
        producer_c3 = spans_by_key[("async_task/tests.fixtures.cascade_three", trace.SpanKind.PRODUCER)]
        consumer_c3 = spans_by_key[("run/tests.fixtures.cascade_three", trace.SpanKind.CONSUMER)]
        producer_c2 = spans_by_key[("async_task/tests.fixtures.cascade_two", trace.SpanKind.PRODUCER)]
        consumer_c2 = spans_by_key[("run/tests.fixtures.cascade_two", trace.SpanKind.CONSUMER)]
        producer_leaf = spans_by_key[("async_task/tests.fixtures.noop", trace.SpanKind.PRODUCER)]
        consumer_leaf = spans_by_key[("run/tests.fixtures.noop", trace.SpanKind.CONSUMER)]

        self.assertEqual(producer_c3.parent.span_id, http_span.get_span_context().span_id)
        self.assertEqual(consumer_c3.parent.span_id, producer_c3.context.span_id)
        self.assertEqual(producer_c2.parent.span_id, consumer_c3.context.span_id)
        self.assertEqual(consumer_c2.parent.span_id, producer_c2.context.span_id)
        self.assertEqual(producer_leaf.parent.span_id, consumer_c2.context.span_id)
        self.assertEqual(consumer_leaf.parent.span_id, producer_leaf.context.span_id)
