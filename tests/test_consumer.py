from __future__ import annotations

from django_q.signals import post_execute_in_worker, pre_execute
from opentelemetry import trace
from opentelemetry.test.test_base import TestBase
from opentelemetry.trace.status import StatusCode

from opentelemetry_instrumentation_django_q2 import DjangoQ2Instrumentor
from opentelemetry_instrumentation_django_q2.utils import (
    OTEL_CARRIER_KEY,
    retrieve_task_context,
)


def _carrier_from(trace_id: int, span_id: int, sampled: bool = True) -> dict[str, str]:
    flags = "01" if sampled else "00"
    return {"traceparent": f"00-{format(trace_id, '032x')}-{format(span_id, '016x')}-{flags}"}


class PreExecuteConsumerStartTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-xyz",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
            OTEL_CARRIER_KEY: _carrier_from(0x11112222333344445555666677778888, 0xAAAABBBBCCCCDDDD),
        }
        task.update(overrides)
        return task

    def test_pre_execute_starts_consumer_span_as_child_of_carrier(self):
        from opentelemetry import context as context_api

        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)

        # Span hasn't ended yet — inspect via the stored context.
        ctx = retrieve_task_context(task["id"])
        self.assertIsNotNone(ctx)
        span, activation, token = ctx
        self.assertEqual(span.kind, trace.SpanKind.CONSUMER)
        parent = span.parent
        self.assertIsNotNone(parent)
        self.assertEqual(parent.trace_id, 0x11112222333344445555666677778888)
        self.assertEqual(parent.span_id, 0xAAAABBBBCCCCDDDD)
        activation.__exit__(None, None, None)
        if token is not None:
            context_api.detach(token)

    def test_pre_execute_sets_messaging_attributes(self):
        task = self._build_task(group="reports")

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        spans = self.memory_exporter.get_finished_spans()
        span = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(span.attributes["messaging.message.id"], "task-id-xyz")
        self.assertEqual(span.attributes["messaging.system"], "django_q2")
        self.assertEqual(span.attributes["messaging.destination.name"], "test-cluster")
        self.assertEqual(span.attributes["messaging.operation.type"], "process")
        self.assertEqual(span.attributes["messaging.operation"], "process")
        self.assertEqual(span.attributes["django_q2.task.name"], "demo")
        self.assertEqual(span.attributes["django_q2.group"], "reports")
        self.assertEqual(span.attributes["messaging.message.conversation_id"], "reports")
        self.assertEqual(span.attributes["django_q2.func"], "tests.fixtures.noop")

    def test_pre_execute_omits_conversation_id_when_no_group(self):
        task = self._build_task()  # no group override

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertNotIn("messaging.message.conversation_id", consumer.attributes)
        self.assertNotIn("django_q2.group", consumer.attributes)

    def test_pre_execute_sets_extra_attribute_pack(self):
        def some_callable():
            pass

        task = self._build_task(
            cached=True,
            sync=True,
            ack_failure=True,
            hook="tasks.callback",
            iter_count=3,
            chain=["task_b", "task_c", "task_d"],
        )
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertIs(consumer.attributes["django_q2.cached"], True)
        self.assertIs(consumer.attributes["django_q2.sync"], True)
        self.assertIs(consumer.attributes["django_q2.ack_failure"], True)
        self.assertEqual(consumer.attributes["django_q2.hook"], "tasks.callback")
        self.assertEqual(consumer.attributes["django_q2.iter_count"], 3)
        self.assertEqual(consumer.attributes["django_q2.chain_length"], 3)

        # And a callable `hook` is intentionally NOT recorded — the repr leaks a
        # memory address that's useless for filtering.
        task_callable = self._build_task(id="task-id-callable-hook", hook=some_callable)
        pre_execute.send(sender="django_q", func=lambda: None, task=task_callable)
        post_execute_in_worker.send(
            sender="django_q",
            func=None,
            task={**task_callable, "success": True, "result": None},
        )
        consumer_b = next(
            s
            for s in self.memory_exporter.get_finished_spans()
            if s.kind == trace.SpanKind.CONSUMER and s.attributes.get("messaging.message.id") == "task-id-callable-hook"
        )
        self.assertNotIn("django_q2.hook", consumer_b.attributes)

    def test_pre_execute_omits_extra_attribute_pack_when_absent(self):
        task = self._build_task()
        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        for key in (
            "django_q2.cached",
            "django_q2.sync",
            "django_q2.ack_failure",
            "django_q2.hook",
            "django_q2.iter_count",
            "django_q2.chain_length",
        ):
            self.assertNotIn(key, consumer.attributes)

    def test_pre_execute_consumer_span_is_current_during_execution(self):
        from opentelemetry import context as context_api

        task = self._build_task()
        observed: list[int] = []

        def fake_func():
            observed.append(trace.get_current_span().get_span_context().span_id)

        pre_execute.send(sender="django_q", func=fake_func, task=task)
        fake_func()
        ctx = retrieve_task_context(task["id"])
        span, activation, token = ctx
        self.assertEqual(observed[-1], span.get_span_context().span_id)
        activation.__exit__(None, None, None)
        if token is not None:
            context_api.detach(token)

    def test_pre_execute_without_carrier_still_creates_span(self):
        task = self._build_task()
        task.pop(OTEL_CARRIER_KEY)

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task={**task, "success": True, "result": None})

        spans = self.memory_exporter.get_finished_spans()
        consumer = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        # No carrier means no parent.
        self.assertIsNone(consumer.parent)


class PostExecuteInWorkerEndTests(TestBase):
    def setUp(self):
        super().setUp()
        self.instrumentor = DjangoQ2Instrumentor()
        self.instrumentor.instrument(tracer_provider=self.tracer_provider)

    def tearDown(self):
        self.instrumentor.uninstrument()
        super().tearDown()

    def _build_task(self, **overrides):
        task = {
            "id": "task-id-end",
            "name": "demo",
            "func": "tests.fixtures.noop",
            "args": (),
            "kwargs": {},
            "cluster": "test-cluster",
        }
        task.update(overrides)
        return task

    def test_success_path_ends_span_unset_status(self):
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = True
        task["result"] = 42
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        spans = self.memory_exporter.get_finished_spans()
        consumer = next(s for s in spans if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)
        self.assertEqual(consumer.events, ())
        # Mirror of Celery's `celery.state` — lets dashboards filter terminal state
        # without parsing the OTel status code.
        self.assertEqual(consumer.attributes["django_q2.state"], "success")
        self.assertIsNone(retrieve_task_context(task["id"]))

    def test_failure_records_exception_event_with_type_message_stacktrace(self):
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = False
        task["result"] = (
            "boom! : Traceback (most recent call last):\n"
            '  File "/app/tests/fixtures.py", line 17, in boom\n'
            '    raise RuntimeError("boom!")\n'
            "RuntimeError: boom!"
        )
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "boom!")
        self.assertEqual(consumer.attributes["django_q2.state"], "error")
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.name, "exception")
        self.assertEqual(event.attributes["exception.type"], "RuntimeError")
        self.assertEqual(event.attributes["exception.message"], "boom!")
        self.assertIn("RuntimeError: boom!", event.attributes["exception.stacktrace"])

    def test_failure_without_traceback_separator_records_status_only(self):
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        task["success"] = False
        task["result"] = "just-an-error-string"
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.ERROR)
        self.assertEqual(consumer.status.description, "just-an-error-string")
        # No traceback in the input ⇒ we still emit an event with just the message.
        self.assertEqual(len(consumer.events), 1)
        event = consumer.events[0]
        self.assertEqual(event.attributes["exception.message"], "just-an-error-string")
        self.assertNotIn("exception.type", event.attributes)
        self.assertNotIn("exception.stacktrace", event.attributes)

    def test_sync_error_branch_missing_success_key_is_tolerated(self):
        # Mirrors worker.py:113 — sync error path may emit post_execute_in_worker
        # before task["success"] / task["result"] / task["stopped"] are set.
        task = self._build_task()

        pre_execute.send(sender="django_q", func=lambda: None, task=task)
        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        consumer = next(s for s in self.memory_exporter.get_finished_spans() if s.kind == trace.SpanKind.CONSUMER)
        self.assertEqual(consumer.status.status_code, StatusCode.UNSET)
        self.assertEqual(consumer.events, ())
        # No `django_q2.state` either — we don't have a terminal state to record,
        # and inventing one would mislead dashboards.
        self.assertNotIn("django_q2.state", consumer.attributes)

    def test_post_execute_without_prior_pre_execute_is_noop(self):
        task = self._build_task()
        task["success"] = True

        post_execute_in_worker.send(sender="django_q", func=None, task=task)

        self.assertEqual(self.memory_exporter.get_finished_spans(), ())
